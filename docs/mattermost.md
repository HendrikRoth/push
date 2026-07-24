# Mattermost direct messages

Push supports one-to-one Mattermost direct messages. It receives events through
the [WebSocket API](https://developers.mattermost.com/integrate/websocket/) and
sends replies with the
[create-post REST endpoint](https://api.mattermost.com/#tag/posts/operation/CreatePost).
It ignores public and private channels, group messages, and posts made by the
bot itself or by the system.

## Create the Mattermost bot

1. In the System Console, enable **Bot Account Creation**.
2. Under **Integrations → Bot Accounts**, create a bot and copy its access
   token. The token authenticates every REST call and the realtime connection.
3. Add the bot to your team so a user can open a direct message with it.
4. Copy your stable Mattermost user ID (a 26-character ID, not your username)
   from **Profile** or the `/api/v4/users/me` response of a trusted client.

Usernames can change, so the allowlist accepts only user IDs.

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

## Delivery and recovery

Push validates the message shape, direct-message channel type, sender ID, and
bot origin before an event can reach an agent. Ordinary text posts with no
system type are supported.

Each conversation uses the stable key `mattermost:dm:<channel-id>`. Replies go
to the Mattermost thread rooted at the originating post, or to the root when the
message opened one.

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
on the same WebSocket so the conversation shows a typing indicator. Voice
messages and replies are not supported.

For scheduled delivery, use an allowlisted Mattermost user ID:

```toml
[primary_delivery]
channel = "mattermost"
target = "replace-with-a-26-char-mattermost-user-id"
```

Push opens (or reuses) the bot's direct-message channel with that user through
[`POST /api/v4/channels/direct`](https://api.mattermost.com/#tag/channels/operation/CreateDirectChannel)
before sending.

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
