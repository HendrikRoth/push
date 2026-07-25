# Mattermost

Push supports Mattermost direct messages, group messages, and public and
private channels. It receives events through the
[WebSocket API](https://developers.mattermost.com/integrate/websocket/) and
sends replies with the
[create-post REST endpoint](https://api.mattermost.com/#tag/posts/operation/CreatePost).
In direct messages every allowlisted post is in scope. In group messages and
channels the bot answers only when it is **@mentioned** or when a post
continues a thread the bot already replied in. Posts from the bot itself and
system posts are ignored.

## Create the Mattermost bot

1. In the System Console, enable **Bot Account Creation**.
2. Under **Integrations → Bot Accounts**, create a bot and copy its access
   token. The token authenticates every REST call and the realtime connection.
3. Add the bot to your team, and to any channel where it should answer, so it
   receives posts and can be @mentioned.
4. Copy your stable Mattermost user ID (a 26-character ID, not your username)
   from **Profile** or the `/api/v4/users/me` response of a trusted client.

Usernames can change, so the allowlist accepts only user IDs. The same
`allow_user_ids` gate applies in channels: a `@mention` from a non-allowlisted
user is ignored.

## Configure Push

Prefer an environment variable for the token:

```sh
export MATTERMOST_TOKEN='bot-access-token'
```

Configure the channel, the server URL, and an explicit user allowlist:

```toml
channel = "mattermost"
agent = "codex"
assistant_root = "~/Code/assistant"

[mattermost]
url = "https://mattermost.example.com"
allow_user_ids = ["replace-with-a-26-char-mattermost-user-id"]
```

You can instead set `mattermost.token` in the private Push config. Never put the
token in the Git-versioned assistant repository. Run:

```sh
chmod 600 ~/.push/config.toml
push doctor
push
```

At runtime Push resolves the bot user with
[`GET /api/v4/users/me`](https://api.mattermost.com/#tag/users/operation/GetUser)
and authenticates the WebSocket with an `authentication_challenge`. The
WebSocket URL is derived from `mattermost.url` by swapping the HTTP scheme for
`ws`/`wss` and appending `/api/v4/websocket`.

## Multiple bots

To run several Mattermost bots from one Push instance — different servers, or
several bots on the same server — replace the single `[mattermost]` table with
one `[[mattermost]]` block per bot. Each block needs a unique `name`:

```toml
channel = "mattermost"

[[mattermost]]
name = "work"
url = "https://mm.work.example.com"
token = "work-bot-token"
allow_user_ids = ["26-char-user-id"]

[[mattermost]]
name = "privat"
url = "https://mm.privat.example.com"
token = "privat-bot-token"
allow_user_ids = ["26-char-user-id"]
```

Enabling `mattermost` (via `channel` or `channels`) runs every configured
`[[mattermost]]` bot. Each bot has its own identity `mattermost:<name>`, its own
poll cursor, its own SQLite inbox, and its own thread namespace, so two bots
never share sessions even on the same server. The legacy single `[mattermost]`
table keeps the bare identity `mattermost`; the two forms are mutually
exclusive. Each named bot needs an inline `token` (the shared `MATTERMOST_TOKEN`
environment variable applies only to the legacy single-bot form). Address a
specific bot from `[primary_delivery]` with `channel = "mattermost:work"`.

## Delivery and recovery

Push validates the message shape, sender ID, and bot origin before an event can
reach an agent. In a channel or group message it additionally requires an
@mention of the bot, or that the post continues a thread the bot already
answered. Ordinary text posts with no system type are supported.

Thread keys are:

- `mattermost:dm:<channel-id>` — a direct message is one conversation.
- `mattermost:ch:<channel-id>:<root-id>` — each channel or group-message thread
  is its own session.

Named bots prefix these with their identity, e.g. `mattermost:work:dm:<channel-id>`.

Replies go to the Mattermost thread rooted at the originating post, or open one
rooted at the post when it was top-level. When the bot replies it records that
thread, so later replies from allowlisted users are accepted without a fresh
@mention. Active threads are stored in the same private SQLite inbox.

Incoming `posted` events are committed to a private SQLite inbox beside
`state_path` before the receiver notifies the gateway. Ignored events retain
only redacted rejection metadata, not message content. The globally unique post
ID is the durable deduplication key, while a local monotonic row ID drives
ordered cursor recovery. A crash before delivery resumes committed rows above
the saved cursor. Mattermost does not document an idempotency key for
create-post, so a network failure after the server accepts a send can still
produce an ambiguous delivery.

Replies are split at 16,383 Unicode characters, Mattermost's default post
length. Mattermost renders Markdown natively, so replies are sent as raw
Markdown. While the agent works, Push sends a best-effort `user_typing` signal
on the same WebSocket so the conversation shows a typing indicator. In a channel
or group message it also adds a one-time `:hourglass_flowing_sand:` reaction to
the triggering @mention as a persistent acknowledgement that outlives the
ephemeral typing signal. Once the run finishes, that reaction is swapped for
`:white_check_mark:` when the reply was delivered, or `:x:` when the run failed,
timed out, or was stopped. Voice messages and replies are not supported.

Scheduled job results go to `primary_delivery`. Two target forms are supported.

For a direct message, use an allowlisted Mattermost user ID:

```toml
[primary_delivery]
channel = "mattermost"
target = "replace-with-a-26-char-mattermost-user-id"
```

Push opens (or reuses) the bot's direct-message channel with that user through
[`POST /api/v4/channels/direct`](https://api.mattermost.com/#tag/channels/operation/CreateDirectChannel)
before sending.

For a channel, prefix the channel ID with `channel:`:

```toml
[primary_delivery]
channel = "mattermost"
target = "channel:replace-with-a-26-char-channel-id"
```

Push posts a top-level message to that channel. The bot must be a member of the
channel. A channel target is not checked against `allow_user_ids` — it is an
operator-configured destination, so restrict who can edit the config.

## File attachments

**Inbound.** When an accepted message carries file attachments, Push downloads
each one whose extension is on the allow list (text, code, CSV/JSON/YAML/TOML,
Markdown, PDF, and common image types) into an `inbox/` folder in the run's
working directory, up to 10 files and 20 MB each. A note listing their relative
paths (`inbox/<name>`) is appended to the prompt. Push never edits the files
itself — the agent decides, from your message, whether and how to use them. An
`inbox/.gitignore` keeps the downloads out of the assistant's git repository.
Non-whitelisted attachments are ignored, and a message that is only an
attachment (no text) is still processed.

**Outbound.** The agent can return a file by writing it into the working
directory and emitting an attach marker in its reply:

```
Here is the report. [[attach: report.md]]
```

Push removes every `[[attach: <path>]]` marker from the delivered text, reads
the named files (paths must stay inside the working directory), uploads them,
and posts them to the same thread. For the agent to use this, tell it about the
marker in your assistant instructions (`SOUL.md`).

## Linux and service mode

Mattermost mode works on Linux or a VM because it does not depend on the macOS
Messages database. Provide the token through the service environment or a
root-readable credentials file. Protect the token, `state.json`, the audit log,
`push.db`, and the private `assistant_root` repository as credentials. Rotate
the bot token in the System Console immediately if it is exposed. Keep
allowlists narrow because an allowed sender can instruct the configured agent to
use its local tools and credentials.

## Troubleshooting

- If `Mattermost token` fails in `push doctor`, set `MATTERMOST_TOKEN` or
  `mattermost.token`.
- If `Mattermost server URL` fails, set `mattermost.url`.
- If messages are ignored, confirm the bot was added to a team, the exact user
  ID is allowlisted, and the message is a direct message.
- WebSocket reconnects are expected; Push's dedicated receiver reconnects
  automatically.
