# Ratatoskr

Ratatoskr is an MCP gateway: one local server that fronts all of
your MCP servers behind a small, fixed tool surface.  Like its
namesake — the squirrel relaying messages up and down Yggdrasil —
it carries traffic between agents and the many servers behind it.

## Why

Connecting an agent directly to a dozen MCP servers floods its context
window: every server's full tool list lands in the system prompt of
every session, and every tool result streams back at full size whether
the agent needs it or not.

Ratatoskr replaces that with ~10 fixed meta-tools. Agents discover
upstreams on demand, inspect only the tools they intend to call, and
run multi-step workflows inside a Luau script (sandboxed Lua) — one
tool call for what would otherwise be N round trips.

The tool surface:

| Tool | Purpose |
|---|---|
| `list-servers` | Catalog of configured upstreams, one hint line each, plus health |
| `server-details` | One upstream's identity + full advertised instructions |
| `list-server-tools` | Compact tool summary for one upstream |
| `tool-details` | Full schema for one tool (required before calling it) |
| `inspect-tool-response` | Call a tool once, verbatim, to observe its response shape |
| `execute` | Run a Luau script that calls upstream tools synchronously |
| `reload-config` | Hot-apply config file changes (add/remove/change upstreams) |
| `reconnect-upstream` | Tear down and re-establish one upstream connection |
| `restart-gateway` | Clean process restart (picks up gateway-wide settings) |
| `ping` | Liveness check |

Inside `execute`, each upstream is a Lua global named by its sanitized
`lua_name` (`datadog-us` → `datadog_us`), and a `_gateway` helper
carries JSON/time/URI-template/parallel-dispatch builtins. Scripts must
inspect a tool (via `tool-details` or `inspect-tool-response`) before
calling it — this forces the agent to read the schema instead of
guessing.

Other features:

- **Result offloading** — oversized `execute` / inspect results are
  stored server-side; the agent gets a shape summary and an ID to
  retrieve slices via `_gateway.get_result`.
- **Response-shape hints** — observed response shapes are cached and
  surfaced in `tool-details` so agents stop rediscovering envelope
  formats.
- **Supervision** — stdio upstreams are health-checked and respawned;
  `list-servers` reports per-upstream health, including "needs login"
  with the exact `rato login <name>` command.
- **Resource namespacing** — upstream resources are exposed as
  `mcp-proxy://<lua_name>/<percent-encoded-uri>` and subscriptions are
  ref-counted across downstream sessions.
- **Container hygiene** — `podman run` / `docker run` upstreams get a
  deterministic `--name rato-<Name>` injected, so orphans are visible
  (`podman ps` filter `^rato-`) and reaped before respawn.

## Installation

Requirements: Rust 1.88+. macOS is the first-class platform (launchd
service, default paths); Linux works with explicit paths via
environment variables.

```sh
cargo build --release
# binary at target/release/rato
```

### Running as a macOS launchd service

Copy [`local.ratatoskr.example.plist`](local.ratatoskr.example.plist)
to `~/Library/LaunchAgents/local.ratatoskr.plist`, replacing
`/Users/YOU` with your home directory (launchd does not expand `~` in
plist paths):

```sh
sed "s|/Users/YOU|$HOME|g" local.ratatoskr.example.plist \
  > ~/Library/LaunchAgents/local.ratatoskr.plist
```

Then use the deploy script for every build you want live:

```sh
scripts/deploy.sh
```

It runs the fmt/clippy/test gate, builds a release binary, installs it
atomically to `~/.local/bin/rato`, and does a full `launchctl bootout`
+ `bootstrap` cycle (so plist edits take effect), then waits for the
post-restart "gateway listening" log line. You can add `--skip-validation`
to just build and install, skipping the test suite.

## Configuration

The gateway reads a single JSON config file:

1. `$RATO_CONFIG_PATH`, if set
2. macOS default: `~/Library/Preferences/ratatoskr/config.json`

See [`config.example.json`](config.example.json) for a full example.

### Gateway-wide settings (top level)

| Key | Default | Meaning |
|---|---|---|
| `port` | `8910` | TCP port; the gateway binds `127.0.0.1` only |
| `resultSizeThreshold` | `50000` | Bytes above which results are offloaded; `0` disables |
| `executeDeadlineMs` | `60000` | Wall-clock deadline per `execute` call |
| `upstreamSupervision` | built-ins | Gateway-wide supervisor overrides (ping interval, backoff, …) |

Settings changes require `restart-gateway` (or a redeploy); upstream
add/remove/change applies live via `reload-config` or `SIGHUP`.

### Upstreams (`mcpClients`)

Each entry under `mcpClients` is keyed by a display name (this becomes
the `list-servers` name, the credential-file name, and — sanitized —
the Lua global). Three transports:

**`stdio`** — spawn a local process:

```json
"GitLab": {
  "type": "stdio",
  "command": "podman",
  "args": ["run", "-i", "--rm", "-e", "GITLAB_TOKEN", "registry.example.com/mcp/gitlab-mcp:latest"],
  "env": { "GITLAB_TOKEN": "..." },
  "hint": "GitLab MRs, pipelines, repos for gitlab.example.com"
}
```

**`http`** — remote server, optional static headers (API keys):

```json
"rootly": {
  "type": "http",
  "url": "https://mcp.rootly.com/mcp",
  "headers": { "Authorization": "Bearer ..." }
}
```

**`http-oauth`** — remote server behind OAuth 2.0 (authorization-code +
PKCE). No `headers` field: the Authorization header is owned by the
OAuth layer.

```json
"Datadog": {
  "type": "http-oauth",
  "url": "https://mcp.datadoghq.com/api/unstable/mcp-server/mcp",
  "scopes": []
}
```

Optional `http-oauth` fields:

- `clientId` — pre-registered client ID, for providers that prohibit
  dynamic client registration. Omit entirely to use DCR.
- `clientName` — name shown in the provider's authorized-apps list
  (default: `ratatoskr`).
- `callbackPort` — fixed localhost callback port, for providers with
  strict redirect-URI allowlists (default: ephemeral).

Shared per-upstream fields (any transport):

- `hint` — one-line catalog description shown in `list-servers`.
  Worth writing for every upstream; it's how agents pick.
- `supervision` — per-upstream supervisor overrides, layered over
  `upstreamSupervision`.
- `dangerouslyEnableSampling` — forward `sampling/createMessage` from
  this upstream to downstream clients that support it.

### Authorizing an `http-oauth` upstream

One-time, interactive, human-at-the-keyboard:

```sh
rato login Datadog
```

This opens your browser, runs the OAuth flow against the provider, and
persists credentials to
`~/Library/Application Support/ratatoskr/credentials/<Name>.json`
(override the state dir with `$RATO_STATE_DIR`). It then signals the
running gateway (SIGHUP) to pick up the new credentials. From that
point on, token refresh is silent and headless; you only re-run
`rato login` if the refresh token itself expires or is revoked —
`list-servers` health output will tell you when, and name the exact
command.

## Connecting a coding harness

The gateway speaks MCP streamable-HTTP at
`http://127.0.0.1:8910/mcp`. Register it as an ordinary HTTP MCP
server; **it should usually be the only MCP server your harness
needs**, since everything else lives behind it.

**Claude Code:**

```sh
claude mcp add --transport http ratatoskr http://127.0.0.1:8910/mcp
```

or in `.mcp.json`:

```json
{
  "mcpServers": {
    "ratatoskr": {
      "type": "http",
      "url": "http://127.0.0.1:8910/mcp"
    }
  }
}
```

**Codex CLI** (`~/.codex/config.toml`):

```toml
[mcp_servers.ratatoskr]
url = "http://127.0.0.1:8910/mcp"
```

**Anything else:** any client that supports MCP streamable-HTTP
transport works the same way — point it at the URL above. The gateway
binds loopback only; there is no auth on the downstream side, so do
not port-forward it off the machine.

The server's `initialize` response carries instructions that teach the
agent the discover → inspect → execute loop, so no per-harness prompt
engineering is required. Adding a note to your agent's project context
("infrastructure questions: start with ratatoskr's `list-servers`")
helps discovery.

## Troubleshooting

**Where are the logs?**

- Under launchd with the plist above: `~/Library/Logs/ratatoskr.log`
  (stdout/stderr redirect; JSON lines).
- With `--log-file`: daily-rotated files, default
  `~/Library/Logs/ratatoskr/gateway.log.YYYY-MM-DD`, 14 days retained.
- Verbosity: `RUST_LOG` (default `info,gateway=debug`). Every tool call
  logs one `call_tool complete` line with tool name, duration, outcome,
  and a `req_id` that ties together the dispatch, Lua execution, and
  upstream calls it triggered.

**Is it running?**

```sh
launchctl print gui/$(id -u)/local.ratatoskr   # service state
curl -s http://127.0.0.1:8910/mcp              # connection check
lsof -nP -iTCP:8910 -sTCP:LISTEN               # who holds the port
```

A crash-looping service under `KeepAlive` is usually `EADDRINUSE`: an
orphaned gateway still holds the port. `scripts/deploy.sh` detects and
names the culprit PID; kill it and redeploy.

**An upstream is broken.** Ask the gateway first: `list-servers`
reports per-upstream health (consecutive ping failures, respawn state,
needs-login). `server-details` includes the same for one upstream.
Common cases:

- *stdio upstream flapping* — its process is crashing; check the
  gateway log for the supervisor's respawn lines and the child's
  stderr.
- *`http-oauth` upstream returns auth errors* — refresh token expired
  or revoked. Run `rato login <Name>`. On macOS you also get a
  notification when the gateway detects this.
- *short-lived credentials minted at process start* (SSO-wrapped
  launchers) — use the `reconnect-upstream` tool; `reload-config`
  deliberately skips unchanged upstreams and won't re-run the
  launcher.

**Config changes don't apply.** `reload-config` / SIGHUP handle
upstream changes only; `port`, `resultSizeThreshold`,
`executeDeadlineMs`, and `upstreamSupervision` are read once at boot —
use the `restart-gateway` tool or redeploy. If the config file is
invalid, reload changes nothing and reports the parse error.

**Orphaned containers.** Container-based stdio upstreams are named
`rato-<Name>`:

```sh
podman ps --filter name='^rato-'
```

The gateway reaps the named container before each respawn, but a hard
gateway kill can leave one behind; `podman rm -f` it.

## Environment variables

| Variable | Meaning |
|---|---|
| `RATO_CONFIG_PATH` | Config file path (overrides platform default) |
| `RATO_STATE_DIR` | State dir for OAuth credentials + pidfile (default: `~/Library/Application Support/ratatoskr`) |
| `RUST_LOG` | tracing filter (default `info,gateway=debug`) |

## Development

```sh
cargo clippy --workspace --all-targets   # warnings are not tolerated
cargo test --workspace
cargo fmt --all
```

Crate layout: `gateway` (server, tools, config), `mcp-client`
(upstream connections, supervision, OAuth persistence), `lua-runtime`
(the Luau `execute` sandbox). See [AGENTS.md](AGENTS.md) for
conventions.

## Acknowledgments

Ratatoskr was *heavily* inspired by
[my-cool-proxy](https://github.com/karashiiro/my-cool-proxy) by
[@karashiiro](https://github.com/karashiiro), and we learned a great
deal from her work while building this. Thank you!

## License

MIT
