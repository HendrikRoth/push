//! Mattermost WebSocket input and REST API output.

use std::collections::HashSet;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, StatusCode};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::channel::RawMessage;

/// Mattermost's default maximum post length in characters.
const MAX_TEXT_CHARS: usize = 16_383;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Writer = SplitSink<Socket, Message>;
type Reader = SplitStream<Socket>;

#[derive(Clone)]
pub struct Mattermost {
    state: Arc<State>,
    receiver: Arc<ReceiverTask>,
}

struct State {
    token: String,
    base_url: String,
    websocket_url: String,
    allow_user_ids: HashSet<String>,
    inbox: Mutex<Inbox>,
    client: Client,
    // The WebSocket is split so outbound frames (typing, pong) never wait on
    // the receiver's long-lived read borrow during `next().await`.
    writer: AsyncMutex<Option<Writer>>,
    reader: AsyncMutex<Option<Reader>>,
    seq: AtomicU64,
    identity: AsyncMutex<Option<Identity>>,
    notify: Notify,
    last_error: Mutex<Option<String>>,
}

struct ReceiverTask {
    handle: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
struct Identity {
    user_id: String,
}

struct Inbox {
    connection: Connection,
    path: String,
}

#[derive(Debug)]
struct Event {
    event_id: String,
    channel: String,
    user: String,
    text: String,
    root: String,
    is_group: bool,
    is_from_me: bool,
    is_supported: bool,
}

#[derive(Deserialize)]
struct WsEnvelope {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    data: Option<Value>,
}

/// The `post` field of a `posted` event is a JSON-encoded string.
#[derive(Deserialize)]
struct Post {
    id: String,
    #[serde(default)]
    channel_id: String,
    #[serde(default)]
    user_id: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    root_id: String,
    #[serde(default, rename = "type")]
    kind: String,
}

impl Drop for ReceiverTask {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.lock().unwrap().take() {
            handle.abort();
        }
    }
}

impl Mattermost {
    pub fn new(
        base_url: String,
        token: String,
        allow_user_ids: Vec<String>,
        state_path: &str,
    ) -> Result<Self> {
        let inbox_path = format!("{state_path}.mattermost-inbox.db");
        Self::with_inbox(base_url, token, allow_user_ids, &inbox_path)
    }

    fn with_inbox(
        base_url: String,
        token: String,
        allow_user_ids: Vec<String>,
        inbox_path: &str,
    ) -> Result<Self> {
        let base_url = base_url.trim().trim_end_matches('/').to_string();
        let websocket_url = websocket_url(&base_url);
        Ok(Self {
            state: Arc::new(State {
                token,
                base_url,
                websocket_url,
                allow_user_ids: allow_user_ids
                    .into_iter()
                    .map(|value| value.trim().to_string())
                    .collect(),
                inbox: Mutex::new(Inbox::open(inbox_path)?),
                client: Client::builder()
                    .timeout(Duration::from_secs(25))
                    .build()
                    .context("build Mattermost HTTP client")?,
                writer: AsyncMutex::new(None),
                reader: AsyncMutex::new(None),
                // seq 1 is reserved for the authentication challenge.
                seq: AtomicU64::new(2),
                identity: AsyncMutex::new(None),
                notify: Notify::new(),
                last_error: Mutex::new(None),
            }),
            receiver: Arc::new(ReceiverTask {
                handle: Mutex::new(None),
            }),
        })
    }

    pub fn allows_user(&self, user: &str) -> bool {
        self.state.allow_user_ids.contains(user.trim())
    }

    pub async fn poll(&self, since: i64) -> Result<Vec<RawMessage>> {
        self.start_receiver();
        loop {
            let notified = self.state.notify.notified();
            if let Some(messages) = self.pending(since)? {
                return Ok(messages);
            }
            if let Some(error) = self.state.last_error.lock().unwrap().take() {
                bail!(error);
            }
            notified.await;
        }
    }

    pub fn latest_cursor(&self) -> Result<i64> {
        self.state.inbox.lock().unwrap().latest_cursor()
    }

    pub async fn send_message(&self, target: &str, text: &str) -> Result<()> {
        let (channel_id, root) = self.resolve_target(target).await?;
        let mut body = json!({"channel_id": channel_id, "message": text});
        if let Some(root) = root {
            body.as_object_mut()
                .expect("Mattermost post payload is an object")
                .insert("root_id".to_string(), Value::String(root));
        }
        self.state.api_post("/api/v4/posts", body).await?;
        Ok(())
    }

    /// Sends a best-effort `user_typing` frame on the shared WebSocket writer.
    /// A closed or not-yet-open connection is silently skipped.
    pub async fn send_typing(&self, target: &str) -> Result<()> {
        let Some((channel, root)) = parse_reply_target(target) else {
            return Ok(());
        };
        let seq = self.state.seq.fetch_add(1, Ordering::Relaxed);
        let frame = json!({
            "seq": seq,
            "action": "user_typing",
            "data": {"channel_id": channel, "parent_id": root}
        });
        if let Some(writer) = self.state.writer.lock().await.as_mut() {
            writer
                .send(Message::Text(frame.to_string().into()))
                .await
                .context("send Mattermost typing")?;
        }
        Ok(())
    }

    async fn resolve_target(&self, target: &str) -> Result<(String, Option<String>)> {
        if let Some((channel, root)) = parse_reply_target(target) {
            return Ok((channel.to_string(), Some(root.to_string())));
        }
        let Some(user) = target.strip_prefix("user:") else {
            bail!("invalid Mattermost delivery target");
        };
        if !self.allows_user(user) {
            bail!("Mattermost delivery user is not allowlisted");
        }
        Ok((self.state.direct_channel(user).await?, None))
    }

    fn pending(&self, since: i64) -> Result<Option<Vec<RawMessage>>> {
        let messages = self.state.inbox.lock().unwrap().after(since)?;
        Ok((!messages.is_empty()).then_some(messages))
    }

    fn start_receiver(&self) {
        let mut handle = self.receiver.handle.lock().unwrap();
        if handle.as_ref().is_some_and(|handle| !handle.is_finished()) {
            return;
        }
        if let Some(finished) = handle.take() {
            drop(finished);
        }
        let state = self.state.clone();
        *handle = Some(tokio::spawn(async move { receive_loop(state).await }));
    }
}

impl State {
    async fn api_get(&self, path: &str) -> Result<Value> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .client
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .with_context(|| format!("call Mattermost GET {path}"))?;
        self.decode(path, response).await
    }

    async fn api_post(&self, path: &str, body: Value) -> Result<Value> {
        let url = format!("{}{path}", self.base_url);
        let response = self
            .client
            .post(&url)
            .bearer_auth(&self.token)
            .json(&body)
            .send()
            .await
            .with_context(|| format!("call Mattermost POST {path}"))?;
        self.decode(path, response).await
    }

    async fn decode(&self, path: &str, response: reqwest::Response) -> Result<Value> {
        let status = response.status();
        if status == StatusCode::TOO_MANY_REQUESTS {
            bail!("Mattermost {path} rate limited");
        }
        let value: Value = response
            .json()
            .await
            .with_context(|| format!("decode Mattermost {path} response ({status})"))?;
        if !status.is_success() {
            let message = value
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or(status.as_str());
            bail!("Mattermost {path} failed: {message}");
        }
        Ok(value)
    }

    async fn ensure_identity(&self) -> Result<Identity> {
        if let Some(identity) = self.identity.lock().await.clone() {
            return Ok(identity);
        }
        let response = self.api_get("/api/v4/users/me").await?;
        let user_id = response
            .get("id")
            .and_then(Value::as_str)
            .context("Mattermost users/me omitted id")?
            .to_string();
        let identity = Identity { user_id };
        *self.identity.lock().await = Some(identity.clone());
        Ok(identity)
    }

    async fn direct_channel(&self, user: &str) -> Result<String> {
        let identity = self.ensure_identity().await?;
        let response = self
            .api_post(
                "/api/v4/channels/direct",
                json!([identity.user_id, user.trim()]),
            )
            .await?;
        response
            .get("id")
            .and_then(Value::as_str)
            .map(str::to_string)
            .context("Mattermost channels/direct omitted id")
    }

    async fn ensure_socket(&self) -> Result<()> {
        if self.reader.lock().await.is_some() {
            return Ok(());
        }
        self.ensure_identity().await?;
        let (socket, _) = tokio_tungstenite::connect_async(&self.websocket_url)
            .await
            .context("connect Mattermost WebSocket")?;
        let (mut writer, reader) = socket.split();
        let challenge = json!({
            "seq": 1,
            "action": "authentication_challenge",
            "data": {"token": self.token}
        });
        writer
            .send(Message::Text(challenge.to_string().into()))
            .await
            .context("authenticate Mattermost WebSocket")?;
        *self.writer.lock().await = Some(writer);
        *self.reader.lock().await = Some(reader);
        Ok(())
    }

    async fn reset_socket(&self) {
        *self.writer.lock().await = None;
        *self.reader.lock().await = None;
    }

    async fn receive_one(&self) -> Result<bool> {
        self.ensure_socket().await?;
        let next = {
            let mut reader = self.reader.lock().await;
            reader
                .as_mut()
                .context("Mattermost WebSocket connection is unavailable")?
                .next()
                .await
        };
        match next {
            Some(Ok(Message::Text(text))) => self.handle_socket_text(&text).await,
            Some(Ok(Message::Ping(payload))) => {
                if let Some(writer) = self.writer.lock().await.as_mut() {
                    writer.send(Message::Pong(payload)).await?;
                }
                Ok(false)
            }
            Some(Ok(Message::Close(_))) | None => {
                self.reset_socket().await;
                bail!("Mattermost WebSocket connection closed")
            }
            Some(Ok(_)) => Ok(false),
            Some(Err(error)) => {
                self.reset_socket().await;
                Err(error).context("receive Mattermost WebSocket message")
            }
        }
    }

    async fn handle_socket_text(&self, text: &str) -> Result<bool> {
        let envelope: WsEnvelope =
            serde_json::from_str(text).context("parse Mattermost envelope")?;
        if envelope.event.as_deref() != Some("posted") {
            return Ok(false);
        }
        let identity = self.ensure_identity().await?;
        let Some(mut event) = envelope
            .data
            .as_ref()
            .and_then(|data| parse_event(data, &identity))
        else {
            return Ok(false);
        };
        let accepted = event.is_supported
            && !event.is_group
            && !event.is_from_me
            && self.allow_user_ids.contains(&event.user);
        if !accepted {
            event.text.clear();
        }
        self.inbox.lock().unwrap().insert(&event)?;
        Ok(true)
    }
}

/// Derives the realtime WebSocket URL from a Mattermost base URL, swapping the
/// HTTP scheme for its WebSocket equivalent and appending the realtime path.
fn websocket_url(base: &str) -> String {
    let base = base.trim_end_matches('/');
    let scheme_swapped = if let Some(rest) = base.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if let Some(rest) = base.strip_prefix("http://") {
        format!("ws://{rest}")
    } else {
        base.to_string()
    };
    format!("{scheme_swapped}/api/v4/websocket")
}

async fn receive_loop(state: Arc<State>) {
    loop {
        match state.receive_one().await {
            Ok(inserted) => {
                if inserted {
                    state.notify.notify_one();
                }
            }
            Err(error) => {
                state.reset_socket().await;
                *state.last_error.lock().unwrap() = Some(format!("{error:#}"));
                state.notify.notify_one();
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

impl Inbox {
    fn open(path: &str) -> Result<Self> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).with_context(|| {
                format!("create Mattermost inbox directory {}", parent.display())
            })?;
        }
        let connection =
            Connection::open(path).with_context(|| format!("open Mattermost inbox {path}"))?;
        crate::util::restrict_permissions(Path::new(path), false)
            .with_context(|| format!("restrict Mattermost inbox permissions {path}"))?;
        connection
            .busy_timeout(Duration::from_secs(5))
            .context("configure Mattermost inbox busy timeout")?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS mattermost_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                channel_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                text TEXT NOT NULL,
                root_id TEXT NOT NULL,
                is_group INTEGER NOT NULL,
                is_from_me INTEGER NOT NULL,
                is_supported INTEGER NOT NULL
            );",
        )?;
        Ok(Self {
            connection,
            path: path.to_string(),
        })
    }

    fn insert(&mut self, event: &Event) -> Result<i64> {
        self.connection.execute(
            "INSERT INTO mattermost_events (
                event_id, channel_id, user_id, text, root_id,
                is_group, is_from_me, is_supported
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(event_id) DO NOTHING",
            params![
                event.event_id,
                event.channel,
                event.user,
                event.text,
                event.root,
                event.is_group,
                event.is_from_me,
                event.is_supported,
            ],
        )?;
        self.connection
            .query_row(
                "SELECT id FROM mattermost_events WHERE event_id = ?1",
                [&event.event_id],
                |row| row.get(0),
            )
            .with_context(|| format!("read Mattermost event from {}", self.path))
    }

    fn latest_cursor(&self) -> Result<i64> {
        self.connection
            .query_row("SELECT MAX(id) FROM mattermost_events", [], |row| row.get(0))
            .optional()?
            .flatten()
            .map_or(Ok(0), Ok)
    }

    fn after(&self, since: i64) -> Result<Vec<RawMessage>> {
        let mut statement = self.connection.prepare(
            "SELECT id, event_id, channel_id, user_id, text, root_id,
                    is_group, is_from_me, is_supported
             FROM mattermost_events WHERE id > ?1 ORDER BY id",
        )?;
        let rows = statement
            .query_map([since], |row| {
                let channel: String = row.get(2)?;
                let root: String = row.get(5)?;
                Ok(RawMessage {
                    row_id: row.get(0)?,
                    provider_event_id: Some(row.get(1)?),
                    channel: "mattermost",
                    handle: row.get(3)?,
                    chat_identifier: format!("{channel}|{root}"),
                    is_group: row.get(6)?,
                    text: row.get(4)?,
                    voice: None,
                    is_from_me: row.get(7)?,
                    is_supported: row.get(8)?,
                    thread_id: None,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("read pending Mattermost inbox events")?;
        Ok(rows)
    }
}

fn parse_event(data: &Value, identity: &Identity) -> Option<Event> {
    let channel_type = data
        .get("channel_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let post_raw = data.get("post")?.as_str()?;
    let post: Post = serde_json::from_str(post_raw).ok()?;
    let root = if post.root_id.is_empty() {
        post.id.clone()
    } else {
        post.root_id.clone()
    };
    let is_from_me = post.user_id == identity.user_id;
    let is_group = channel_type != "D";
    let is_supported = post.kind.is_empty()
        && !post.channel_id.is_empty()
        && !post.user_id.is_empty()
        && !post.message.trim().is_empty()
        && !root.is_empty();
    Some(Event {
        event_id: post.id,
        channel: post.channel_id,
        user: post.user_id,
        text: post.message,
        root,
        is_group,
        is_from_me,
        is_supported,
    })
}

pub fn split_text(text: &str) -> Vec<String> {
    let mut chunks = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if current.chars().count() == MAX_TEXT_CHARS {
            chunks.push(std::mem::take(&mut current));
        }
        current.push(character);
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    chunks
}

pub fn parse_message_target(value: &str) -> Option<(&str, &str)> {
    let (channel, root) = value.split_once('|')?;
    (!channel.is_empty() && !root.is_empty()).then_some((channel, root))
}

fn parse_reply_target(value: &str) -> Option<(&str, &str)> {
    parse_message_target(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_path;
    use tokio::net::TcpListener;

    fn identity() -> Identity {
        Identity {
            user_id: "UBOT".to_string(),
        }
    }

    fn data(post: Value, channel_type: &str) -> Value {
        json!({
            "channel_type": channel_type,
            "post": post.to_string(),
        })
    }

    fn post(id: &str, user: &str, message: &str, root: &str, kind: &str) -> Value {
        json!({
            "id": id,
            "channel_id": "D1",
            "user_id": user,
            "message": message,
            "root_id": root,
            "type": kind,
        })
    }

    #[test]
    fn websocket_url_swaps_scheme_and_appends_realtime_path() {
        assert_eq!(
            websocket_url("https://mm.example.com"),
            "wss://mm.example.com/api/v4/websocket"
        );
        assert_eq!(
            websocket_url("http://127.0.0.1:8065/"),
            "ws://127.0.0.1:8065/api/v4/websocket"
        );
    }

    #[test]
    fn parses_only_plain_direct_messages() {
        let accepted = parse_event(&data(post("p1", "U1", "hello", "", ""), "D"), &identity()).unwrap();
        assert!(accepted.is_supported);
        assert!(!accepted.is_group);
        assert!(!accepted.is_from_me);
        assert_eq!(accepted.root, "p1");

        for (event, channel_type) in [
            (post("p2", "U1", "hi", "", ""), "O"),
            (post("p3", "U1", "hi", "", ""), "G"),
            (post("p4", "UBOT", "hi", "", ""), "D"),
            (post("p5", "U1", "joined", "", "system_join_channel"), "D"),
            (post("p6", "U1", "  ", "", ""), "D"),
        ] {
            let parsed = parse_event(&data(event, channel_type), &identity()).unwrap();
            assert!(parsed.is_group || parsed.is_from_me || !parsed.is_supported);
        }
    }

    #[test]
    fn threaded_reply_keeps_root_post_id() {
        let event = parse_event(
            &data(post("p7", "U1", "reply", "root1", ""), "D"),
            &identity(),
        )
        .unwrap();
        assert_eq!(event.root, "root1");
    }

    #[test]
    fn inbox_deduplicates_post_ids_and_recovers_rows() {
        let path = temp_path("mattermost-inbox");
        let mut inbox = Inbox::open(path.to_str().unwrap()).unwrap();
        let event =
            parse_event(&data(post("p1", "U1", "hello", "", ""), "D"), &identity()).unwrap();
        assert_eq!(inbox.insert(&event).unwrap(), 1);
        assert_eq!(inbox.insert(&event).unwrap(), 1);
        drop(inbox);

        let inbox = Inbox::open(path.to_str().unwrap()).unwrap();
        let rows = inbox.after(0).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].event_id(), "mattermost:p1");
        assert_eq!(rows[0].chat_identifier, "D1|p1");
        assert_eq!(inbox.latest_cursor().unwrap(), 1);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn chunks_unicode_without_splitting_characters() {
        let text = "🦀".repeat(MAX_TEXT_CHARS + 1);
        let chunks = split_text(&text);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chars().count(), MAX_TEXT_CHARS);
        assert_eq!(chunks[1], "🦀");
    }

    #[tokio::test]
    async fn typing_sends_user_typing_frame_on_the_split_writer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap().into_text().unwrap()
        });

        let path = temp_path("mattermost-typing-inbox");
        let mm = Mattermost::with_inbox(
            "http://unused".to_string(),
            "token".to_string(),
            vec!["U1".to_string()],
            path.to_str().unwrap(),
        )
        .unwrap();
        let (client, _) = tokio_tungstenite::connect_async(format!("ws://{address}"))
            .await
            .unwrap();
        let (writer, reader) = client.split();
        *mm.state.writer.lock().await = Some(writer);
        *mm.state.reader.lock().await = Some(reader);

        // A held reader borrow must not block the writer-only typing send.
        let _held = mm.state.reader.lock().await;
        mm.send_typing("D1|root1").await.unwrap();

        let frame: Value = serde_json::from_str(&server.await.unwrap()).unwrap();
        assert_eq!(frame["action"], "user_typing");
        assert_eq!(frame["data"]["channel_id"], "D1");
        assert_eq!(frame["data"]["parent_id"], "root1");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn typing_without_open_socket_is_a_noop() {
        let path = temp_path("mattermost-typing-noop");
        let mm = Mattermost::with_inbox(
            "http://unused".to_string(),
            "token".to_string(),
            vec!["U1".to_string()],
            path.to_str().unwrap(),
        )
        .unwrap();
        mm.send_typing("D1|root1").await.unwrap();
        mm.send_typing("user:U1").await.unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn parses_message_and_reply_targets() {
        assert_eq!(parse_message_target("D1|root1"), Some(("D1", "root1")));
        assert_eq!(parse_message_target("D1|"), None);
        assert_eq!(parse_message_target("|root1"), None);
        assert_eq!(parse_reply_target("D1|root1"), Some(("D1", "root1")));
    }
}
