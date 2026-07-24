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
/// Reaction added to a channel @mention while the agent works on it.
const WORKING_EMOJI: &str = "hourglass_flowing_sand";
/// Reaction that replaces it once the run has been delivered.
const DONE_EMOJI: &str = "white_check_mark";
/// Reaction that replaces it when the run failed, timed out, or was stopped.
const FAILED_EMOJI: &str = "x";
/// Upper bound on remembered active threads; the oldest are pruned past this.
const MAX_ACTIVE_THREADS: i64 = 500;

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
    // Post IDs already given a "working" reaction, to react at most once.
    reacted: Mutex<HashSet<String>>,
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
    username: String,
}

struct Inbox {
    connection: Connection,
    path: String,
}

#[derive(Debug)]
struct Event {
    event_id: String,
    /// Mattermost channel type: "D" direct, "G" group, "O" public, "P" private.
    channel_type: String,
    channel: String,
    user: String,
    text: String,
    root: String,
    /// True when the bot user is in the event's `mentions` list.
    is_mention: bool,
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
                reacted: Mutex::new(HashSet::new()),
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
        if let Some(root) = &root {
            body.as_object_mut()
                .expect("Mattermost post payload is an object")
                .insert("root_id".to_string(), Value::String(root.clone()));
        }
        self.state.api_post("/api/v4/posts", body).await?;
        // Remember the thread so later replies from allowlisted users are
        // accepted without a fresh @mention.
        if let Some(root) = root {
            self.state
                .inbox
                .lock()
                .unwrap()
                .mark_active_thread(&channel_id, &root)?;
        }
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
        // Channel targets carry the triggering post ID; give it a one-time
        // reaction so a @mention in a busy channel has a visible acknowledgement
        // that outlives the ephemeral typing signal.
        if let Some(post_id) = reply_target_post_id(target) {
            self.react_working(post_id).await;
        }
        Ok(())
    }

    async fn react_working(&self, post_id: &str) {
        if self.state.reacted.lock().unwrap().contains(post_id) {
            return;
        }
        let Ok(identity) = self.state.ensure_identity().await else {
            return;
        };
        let posted = self
            .state
            .api_post(
                "/api/v4/reactions",
                json!({
                    "user_id": identity.user_id,
                    "post_id": post_id,
                    "emoji_name": WORKING_EMOJI
                }),
            )
            .await
            .is_ok();
        if posted {
            self.state.reacted.lock().unwrap().insert(post_id.to_string());
        }
    }

    /// Swaps the working reaction for an outcome reaction once the run has
    /// finished: `DONE_EMOJI` when the reply was delivered, `FAILED_EMOJI`
    /// otherwise. Best-effort and channel-only: DM and scheduled targets carry
    /// no post id and are left untouched.
    pub async fn finish_working(&self, target: &str, delivered: bool) {
        let Some(post_id) = reply_target_post_id(target) else {
            return;
        };
        let Ok(identity) = self.state.ensure_identity().await else {
            return;
        };
        let _ = self
            .state
            .api_delete(&format!(
                "/api/v4/users/{}/posts/{post_id}/reactions/{WORKING_EMOJI}",
                identity.user_id
            ))
            .await;
        let emoji = if delivered { DONE_EMOJI } else { FAILED_EMOJI };
        let _ = self
            .state
            .api_post(
                "/api/v4/reactions",
                json!({
                    "user_id": identity.user_id,
                    "post_id": post_id,
                    "emoji_name": emoji
                }),
            )
            .await;
        self.state.reacted.lock().unwrap().remove(post_id);
    }

    async fn resolve_target(&self, target: &str) -> Result<(String, Option<String>)> {
        if let Some((channel, root)) = parse_reply_target(target) {
            return Ok((channel.to_string(), Some(root.to_string())));
        }
        if let Some(channel) = target.strip_prefix("channel:") {
            let channel = channel.trim();
            if channel.is_empty() {
                bail!("invalid Mattermost channel target");
            }
            return Ok((channel.to_string(), None));
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
        self.send_retrying(path, || self.client.get(&url).bearer_auth(&self.token))
            .await
    }

    async fn api_post(&self, path: &str, body: Value) -> Result<Value> {
        let url = format!("{}{path}", self.base_url);
        self.send_retrying(path, || {
            self.client.post(&url).bearer_auth(&self.token).json(&body)
        })
        .await
    }

    async fn api_delete(&self, path: &str) -> Result<Value> {
        let url = format!("{}{path}", self.base_url);
        self.send_retrying(path, || self.client.delete(&url).bearer_auth(&self.token))
            .await
    }

    /// Sends a request, retrying once after Mattermost's `Retry-After` on a 429.
    async fn send_retrying(
        &self,
        path: &str,
        build: impl Fn() -> reqwest::RequestBuilder,
    ) -> Result<Value> {
        let mut attempt = 0;
        loop {
            let response = build()
                .send()
                .await
                .with_context(|| format!("call Mattermost {path}"))?;
            if response.status() == StatusCode::TOO_MANY_REQUESTS && attempt == 0 {
                tokio::time::sleep(retry_after(response.headers())).await;
                attempt += 1;
                continue;
            }
            return self.decode(path, response).await;
        }
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
        let username = response
            .get("username")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let identity = Identity { user_id, username };
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
        // In direct messages every allowlisted post is in scope. In channels
        // and group DMs the bot only answers when it is @mentioned or the post
        // continues a thread the bot already replied in.
        let triggered = event.channel_type == "D"
            || event.is_mention
            || self
                .inbox
                .lock()
                .unwrap()
                .is_active_thread(&event.channel, &event.root)?;
        let accepted = event.is_supported
            && triggered
            && !event.is_from_me
            && self.allow_user_ids.contains(&event.user);
        if !accepted {
            event.text.clear();
        }
        self.inbox.lock().unwrap().insert(&event)?;
        Ok(true)
    }
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .map_or(Duration::from_secs(1), Duration::from_secs)
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
    const MIN_BACKOFF: Duration = Duration::from_secs(1);
    const MAX_BACKOFF: Duration = Duration::from_secs(30);
    let mut backoff = MIN_BACKOFF;
    loop {
        match state.receive_one().await {
            Ok(inserted) => {
                backoff = MIN_BACKOFF;
                if inserted {
                    state.notify.notify_one();
                }
            }
            Err(error) => {
                state.reset_socket().await;
                *state.last_error.lock().unwrap() = Some(format!("{error:#}"));
                state.notify.notify_one();
                // Exponential backoff so a persistent failure (for example an
                // invalid token returning 401 on every connect) does not hammer
                // the server or flood the log every second.
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
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
                channel_type TEXT NOT NULL DEFAULT 'D',
                is_group INTEGER NOT NULL,
                is_from_me INTEGER NOT NULL,
                is_supported INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS mattermost_threads (
                channel_id TEXT NOT NULL,
                root_id TEXT NOT NULL,
                PRIMARY KEY (channel_id, root_id)
            );",
        )?;
        // Add channel_type to inboxes created before channel support. A
        // duplicate-column error means the migration already ran.
        let _ = connection.execute(
            "ALTER TABLE mattermost_events ADD COLUMN channel_type TEXT NOT NULL DEFAULT 'D'",
            [],
        );
        Ok(Self {
            connection,
            path: path.to_string(),
        })
    }

    fn insert(&mut self, event: &Event) -> Result<i64> {
        self.connection.execute(
            "INSERT INTO mattermost_events (
                event_id, channel_id, user_id, text, root_id,
                channel_type, is_group, is_from_me, is_supported
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(event_id) DO NOTHING",
            params![
                event.event_id,
                event.channel,
                event.user,
                event.text,
                event.root,
                event.channel_type,
                false,
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
                    channel_type, is_from_me, is_supported
             FROM mattermost_events WHERE id > ?1 ORDER BY id",
        )?;
        let rows = statement
            .query_map([since], |row| {
                let channel: String = row.get(2)?;
                let root: String = row.get(5)?;
                let channel_type: String = row.get(6)?;
                Ok(RawMessage {
                    row_id: row.get(0)?,
                    provider_event_id: Some(row.get(1)?),
                    channel: "mattermost",
                    handle: row.get(3)?,
                    chat_identifier: format!("{channel_type}|{channel}|{root}"),
                    is_group: false,
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

    fn mark_active_thread(&mut self, channel: &str, root: &str) -> Result<()> {
        if root.is_empty() {
            return Ok(());
        }
        self.connection
            .execute(
                "INSERT INTO mattermost_threads (channel_id, root_id)
                 VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                params![channel, root],
            )
            .with_context(|| format!("record Mattermost thread in {}", self.path))?;
        // Keep only the most recently inserted threads so the table cannot grow
        // without bound over the lifetime of the inbox.
        self.connection
            .execute(
                "DELETE FROM mattermost_threads WHERE rowid NOT IN (
                    SELECT rowid FROM mattermost_threads ORDER BY rowid DESC LIMIT ?1
                 )",
                params![MAX_ACTIVE_THREADS],
            )
            .with_context(|| format!("prune Mattermost threads in {}", self.path))?;
        Ok(())
    }

    fn is_active_thread(&self, channel: &str, root: &str) -> Result<bool> {
        if root.is_empty() {
            return Ok(false);
        }
        let exists = self
            .connection
            .query_row(
                "SELECT 1 FROM mattermost_threads WHERE channel_id = ?1 AND root_id = ?2",
                params![channel, root],
                |_| Ok(()),
            )
            .optional()
            .with_context(|| format!("read Mattermost thread from {}", self.path))?
            .is_some();
        Ok(exists)
    }
}

fn parse_event(data: &Value, identity: &Identity) -> Option<Event> {
    let channel_type = data
        .get("channel_type")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let post_raw = data.get("post")?.as_str()?;
    let post: Post = serde_json::from_str(post_raw).ok()?;
    let root = if post.root_id.is_empty() {
        post.id.clone()
    } else {
        post.root_id.clone()
    };
    // `mentions` is a JSON-encoded array string of mentioned user IDs.
    let is_mention = data
        .get("mentions")
        .and_then(Value::as_str)
        .and_then(|raw| serde_json::from_str::<Vec<String>>(raw).ok())
        .is_some_and(|mentions| mentions.iter().any(|id| id == &identity.user_id));
    let is_from_me = post.user_id == identity.user_id;
    // Strip the bot's own @mention so the agent receives the plain request.
    let text = strip_mention(&post.message, &identity.username);
    let is_supported = post.kind.is_empty()
        && !post.channel_id.is_empty()
        && !post.user_id.is_empty()
        && !text.trim().is_empty()
        && !root.is_empty();
    Some(Event {
        event_id: post.id,
        channel_type,
        channel: post.channel_id,
        user: post.user_id,
        text,
        root,
        is_mention,
        is_from_me,
        is_supported,
    })
}

/// Removes the bot's own `@username` tokens from a post so the agent is not fed
/// its own handle. A no-op when the username is unknown.
fn strip_mention(text: &str, username: &str) -> String {
    if username.is_empty() {
        return text.to_string();
    }
    text.replace(&format!("@{username}"), "")
        .trim()
        .to_string()
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

/// Splits a stored `chat_identifier` (`<channel_type>|<channel>|<root>`).
pub fn parse_message_target(value: &str) -> Option<(&str, &str, &str)> {
    let (channel_type, rest) = value.split_once('|')?;
    let (channel, root) = rest.split_once('|')?;
    (!channel_type.is_empty() && !channel.is_empty() && !root.is_empty())
        .then_some((channel_type, channel, root))
}

/// Splits a reply target (`<channel>|<root>` or `<channel>|<root>|<post-id>`),
/// returning the channel and thread root. Any trailing post id is ignored.
fn parse_reply_target(value: &str) -> Option<(&str, &str)> {
    let mut parts = value.splitn(3, '|');
    let channel = parts.next()?;
    let root = parts.next()?;
    (!channel.is_empty() && !root.is_empty()).then_some((channel, root))
}

/// Returns the optional third segment of a channel reply target: the id of the
/// post that triggered the run, used to react to it.
fn reply_target_post_id(value: &str) -> Option<&str> {
    value.splitn(3, '|').nth(2).filter(|id| !id.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::temp_path;
    use tokio::net::TcpListener;

    fn identity() -> Identity {
        Identity {
            user_id: "UBOT".to_string(),
            username: "push".to_string(),
        }
    }

    fn data(post: Value, channel_type: &str) -> Value {
        json!({
            "channel_type": channel_type,
            "post": post.to_string(),
        })
    }

    fn data_mention(post: Value, channel_type: &str, mentions: &[&str]) -> Value {
        json!({
            "channel_type": channel_type,
            "post": post.to_string(),
            "mentions": serde_json::to_string(mentions).unwrap(),
        })
    }

    fn post(id: &str, user: &str, message: &str, root: &str, kind: &str) -> Value {
        post_in("D1", id, user, message, root, kind)
    }

    fn post_in(
        channel: &str,
        id: &str,
        user: &str,
        message: &str,
        root: &str,
        kind: &str,
    ) -> Value {
        json!({
            "id": id,
            "channel_id": channel,
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
    fn parses_post_fields_channel_type_and_origin() {
        let event =
            parse_event(&data(post("p1", "U1", "hello", "", ""), "D"), &identity()).unwrap();
        assert!(event.is_supported);
        assert!(!event.is_from_me);
        assert!(!event.is_mention);
        assert_eq!(event.channel_type, "D");
        assert_eq!(event.root, "p1");

        // Bot posts, system posts, and empty text are unsupported or self.
        assert!(parse_event(&data(post("p4", "UBOT", "hi", "", ""), "D"), &identity())
            .unwrap()
            .is_from_me);
        assert!(!parse_event(
            &data(post("p5", "U1", "joined", "", "system_join_channel"), "O"),
            &identity()
        )
        .unwrap()
        .is_supported);
        assert!(!parse_event(&data(post("p6", "U1", "  ", "", ""), "P"), &identity())
            .unwrap()
            .is_supported);
    }

    #[test]
    fn detects_bot_mention_in_channel_post() {
        let mentioned = parse_event(
            &data_mention(post_in("C1", "p8", "U1", "@push hi", "", ""), "O", &["UBOT"]),
            &identity(),
        )
        .unwrap();
        assert!(mentioned.is_mention);
        assert_eq!(mentioned.channel_type, "O");
        // The bot's own @mention is stripped from the prompt text.
        assert_eq!(mentioned.text, "hi");

        let others = parse_event(
            &data_mention(post_in("C1", "p9", "U1", "@someone hi", "", ""), "O", &["U2"]),
            &identity(),
        )
        .unwrap();
        assert!(!others.is_mention);
    }

    #[test]
    fn strips_bot_mention_and_rejects_mention_only_posts() {
        assert_eq!(strip_mention("@push do the thing", "push"), "do the thing");
        assert_eq!(strip_mention("hey @push now", "push"), "hey  now");
        assert_eq!(strip_mention("no mention", "push"), "no mention");
        assert_eq!(strip_mention("@push", "push"), "");
        assert_eq!(strip_mention("@push", ""), "@push");

        // A bare @mention with no request is empty after stripping, so it is
        // treated as unsupported instead of an empty agent prompt.
        let bare = parse_event(
            &data_mention(post_in("C1", "p10", "U1", "@push", "", ""), "O", &["UBOT"]),
            &identity(),
        )
        .unwrap();
        assert!(!bare.is_supported);
    }

    #[test]
    fn active_thread_is_marked_on_send_and_read_back() {
        let path = temp_path("mattermost-threads");
        let mut inbox = Inbox::open(path.to_str().unwrap()).unwrap();
        assert!(!inbox.is_active_thread("C1", "root1").unwrap());
        assert!(!inbox.is_active_thread("C1", "").unwrap());
        inbox.mark_active_thread("C1", "root1").unwrap();
        assert!(inbox.is_active_thread("C1", "root1").unwrap());
        assert!(!inbox.is_active_thread("C1", "root2").unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn active_threads_are_pruned_to_the_cap() {
        let path = temp_path("mattermost-thread-prune");
        let mut inbox = Inbox::open(path.to_str().unwrap()).unwrap();
        let total = MAX_ACTIVE_THREADS + 5;
        for i in 0..total {
            inbox.mark_active_thread("C1", &format!("r{i}")).unwrap();
        }
        let count: i64 = inbox
            .connection
            .query_row("SELECT COUNT(*) FROM mattermost_threads", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, MAX_ACTIVE_THREADS);
        // The oldest thread was evicted; the newest is retained.
        assert!(!inbox.is_active_thread("C1", "r0").unwrap());
        assert!(inbox
            .is_active_thread("C1", &format!("r{}", total - 1))
            .unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn channel_and_dm_posts_key_and_target_distinctly() {
        let mm = Mattermost::with_inbox(
            "http://unused".to_string(),
            "token".to_string(),
            vec!["U1".to_string()],
            temp_path("mattermost-accept").to_str().unwrap(),
        )
        .unwrap();
        let channel = crate::channel::Channel::Mattermost(mm);

        let mut dm = parse_event(&data(post("p1", "U1", "hi", "", ""), "D"), &identity()).unwrap();
        dm.text = "hi".to_string();
        let dm_row = row(&dm);
        assert_eq!(
            channel.accept(&dm_row),
            Some(("mattermost:dm:D1".to_string(), "D1|p1".to_string()))
        );

        let ch = parse_event(
            &data_mention(post_in("C1", "p2", "U1", "@push hi", "", ""), "O", &["UBOT"]),
            &identity(),
        )
        .unwrap();
        let ch_row = row(&ch);
        assert_eq!(
            channel.accept(&ch_row),
            Some(("mattermost:ch:C1:p2".to_string(), "C1|p2|p2".to_string()))
        );
    }

    fn row(event: &Event) -> RawMessage {
        RawMessage {
            row_id: 1,
            provider_event_id: Some(event.event_id.clone()),
            channel: "mattermost",
            handle: event.user.clone(),
            chat_identifier: format!("{}|{}|{}", event.channel_type, event.channel, event.root),
            is_group: false,
            text: event.text.clone(),
            voice: None,
            is_from_me: event.is_from_me,
            is_supported: event.is_supported,
            thread_id: None,
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
        assert_eq!(rows[0].chat_identifier, "D|D1|p1");
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
    async fn channel_target_reacts_to_the_triggering_post_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // The server answers exactly two requests: identity, then the reaction.
        // A duplicate reaction would open a third connection and fail this join.
        let server = tokio::spawn(async move {
            let mut paths = Vec::new();
            for body in [r#"{"id":"UBOT"}"#, r#"{"emoji_name":"eyes"}"#] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = [0_u8; 1024];
                let read = stream.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                paths.push(request.lines().next().unwrap_or_default().to_string());
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            paths
        });

        let mm = Mattermost::with_inbox(
            format!("http://{address}"),
            "token".to_string(),
            vec!["U1".to_string()],
            temp_path("mattermost-react").to_str().unwrap(),
        )
        .unwrap();

        mm.send_typing("C1|root1|p9").await.unwrap();
        mm.send_typing("C1|root1|p9").await.unwrap();

        let paths = server.await.unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths[0].starts_with("GET /api/v4/users/me"));
        assert!(paths[1].starts_with("POST /api/v4/reactions"));
    }

    #[tokio::test]
    async fn finish_swaps_working_reaction_for_a_done_reaction() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        // Three requests: identity, remove :eyes:, add :white_check_mark:.
        let server = tokio::spawn(async move {
            let mut lines = Vec::new();
            for body in [r#"{"id":"UBOT"}"#, r#"{"status":"OK"}"#, r#"{"emoji_name":"ok"}"#] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = [0_u8; 1024];
                let read = stream.read(&mut buffer).await.unwrap();
                let request = String::from_utf8_lossy(&buffer[..read]).to_string();
                lines.push(request.lines().next().unwrap_or_default().to_string());
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
            lines
        });

        let mm = Mattermost::with_inbox(
            format!("http://{address}"),
            "token".to_string(),
            vec!["U1".to_string()],
            temp_path("mattermost-finish").to_str().unwrap(),
        )
        .unwrap();

        mm.finish_working("C1|root1|p9", true).await;
        // A DM/scheduled target with no post id makes no request.
        mm.finish_working("C1|root1", true).await;

        let lines = server.await.unwrap();
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("GET /api/v4/users/me"));
        assert!(lines[1].starts_with(
            "DELETE /api/v4/users/UBOT/posts/p9/reactions/hourglass_flowing_sand"
        ));
        assert!(lines[2].starts_with("POST /api/v4/reactions"));
    }

    #[tokio::test]
    async fn resolve_target_handles_channel_reply_and_user_forms() {
        let mm = Mattermost::with_inbox(
            "http://unused".to_string(),
            "token".to_string(),
            vec!["U1".to_string()],
            temp_path("mattermost-resolve").to_str().unwrap(),
        )
        .unwrap();

        // Channel delivery: top-level post, no thread root.
        assert_eq!(
            mm.resolve_target("channel:C1").await.unwrap(),
            ("C1".to_string(), None)
        );
        // Thread reply target keeps its root.
        assert_eq!(
            mm.resolve_target("C1|root1").await.unwrap(),
            ("C1".to_string(), Some("root1".to_string()))
        );
        assert!(mm.resolve_target("channel:").await.is_err());
        // A non-allowlisted user DM is refused before any network call.
        assert!(mm.resolve_target("user:U2").await.is_err());
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
        assert_eq!(parse_message_target("D|D1|root1"), Some(("D", "D1", "root1")));
        assert_eq!(parse_message_target("O|C1|p2"), Some(("O", "C1", "p2")));
        assert_eq!(parse_message_target("D1|root1"), None);
        assert_eq!(parse_message_target("D|D1|"), None);
        assert_eq!(parse_reply_target("D1|root1"), Some(("D1", "root1")));
        assert_eq!(parse_reply_target("D1|"), None);
        // A channel reply target carries a trailing post id, ignored for replies.
        assert_eq!(parse_reply_target("C1|root1|p9"), Some(("C1", "root1")));
        assert_eq!(reply_target_post_id("C1|root1|p9"), Some("p9"));
        assert_eq!(reply_target_post_id("D1|root1"), None);
        assert_eq!(reply_target_post_id("C1|root1|"), None);
    }
}
