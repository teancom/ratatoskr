#!/usr/bin/env bash
# Deploy a freshly-built release binary to the launchd-managed location
# and restart the service.
#
# The deploy path (~/.local/bin/rato) is divorced from the
# cargo build output (target/release/rato) on purpose: it
# means `cargo build` / `cargo test` cycles do not touch the binary the
# live gateway is running. Only this script disturbs the running process.
#
# Usage:
#   scripts/deploy.sh           # deploy current target/release binary
#   scripts/deploy.sh --skip-validation
#                               # skip fmt/clippy/test gate (use sparingly)
#
# What it does:
#   1. (unless --skip-validation) runs cargo fmt --check, clippy, and
#      the full test suite. Any failure aborts before touching the
#      running gateway.
#   2. Builds target/release/rato.
#   3. install(1)-s the release binary to ~/.local/bin/ atomically.
#   4. launchctl bootout, wait for the drain, verify nothing still holds
#      the gateway's listen port (an orphaned instance would make the
#      fresh one crash-loop on EADDRINUSE under KeepAlive), then
#      launchctl bootstrap to relaunch the gateway against the
#      new binary AND re-read the plist from disk. This is deliberately
#      not `kickstart -k`: kickstart restarts the process but reuses
#      whatever environment launchd cached the last time the agent was
#      bootstrapped, so plist edits (env-var renames, new env keys,
#      ProgramArguments tweaks) silently fail to take effect. Env-var
#      changes are common enough — and the cost of bootout+bootstrap is
#      a few hundred milliseconds — that we do the full reload every
#      time.
#   5. Polls the log for a post-restart "gateway listening" line so the
#      user gets explicit confirmation rather than a silent window of
#      uncertainty.
#
# Exit codes:
#   0  deploy succeeded and gateway is listening
#   1  validation failure or build failure (gateway untouched)
#   2  gateway did not come back within the health-check timeout, or an
#      orphaned process still holds the listen port after bootout

set -euo pipefail

DEPLOY_PATH="${HOME}/.local/bin/rato"
SOURCE_PATH="target/release/rato"
LAUNCHD_LABEL="local.ratatoskr"
LAUNCHD_PLIST="${HOME}/Library/LaunchAgents/${LAUNCHD_LABEL}.plist"
LOG_PATH="${HOME}/Library/Logs/ratatoskr.log"
HEALTH_TIMEOUT_SECS=45

# Run from repo root so the relative target/ path resolves.
cd "$(dirname "$0")/.."

SKIP_VALIDATION=0
for arg in "$@"; do
  case "$arg" in
    --skip-validation) SKIP_VALIDATION=1 ;;
    *) echo "unknown argument: $arg" >&2; exit 1 ;;
  esac
done

if [[ "$SKIP_VALIDATION" -eq 0 ]]; then
  echo "==> validation gate"
  cargo fmt --check
  cargo clippy --all-targets --all-features --quiet
  cargo test --all --quiet -- --include-ignored
else
  echo "==> validation gate SKIPPED (--skip-validation)"
fi

echo "==> building release binary"
cargo build --release --quiet

# `install -m 755 src dst` does an atomic rename into place, so the
# previous binary is still usable by any process that already opened
# it while the new one takes over on next exec.
echo "==> staging $SOURCE_PATH -> $DEPLOY_PATH"
mkdir -p "$(dirname "$DEPLOY_PATH")"
install -m 755 "$SOURCE_PATH" "$DEPLOY_PATH"

# Record a timestamp BEFORE the restart so the health check knows
# which "gateway listening" line in the log is the post-restart one.
# Timestamps in the log are RFC 3339 UTC; we use a matching prefix.
MARKER_UTC="$(date -u +"%Y-%m-%dT%H:%M:%S")"

if [[ ! -f "$LAUNCHD_PLIST" ]]; then
  echo "!! missing LaunchAgent plist: $LAUNCHD_PLIST" >&2
  exit 1
fi

DOMAIN_TARGET="gui/$(id -u)"
SERVICE_TARGET="${DOMAIN_TARGET}/${LAUNCHD_LABEL}"

# bootout removes the agent from the launchd user domain, draining
# launchd's cached environment and ProgramArguments. The `|| true` is
# load-bearing on a fresh machine where the agent isn't yet bootstrapped
# (bootout exits non-zero in that case, which is fine — bootstrap below
# will set everything up from scratch).
echo "==> booting out $LAUNCHD_LABEL (forcing plist reload)"
launchctl bootout "$SERVICE_TARGET" 2>/dev/null || true

# launchd's bootout is asynchronous: the command returns before the
# service has fully drained from the domain. If we race into bootstrap
# before that finishes, launchd returns the infamous
# `Bootstrap failed: 5: Input/output error`. Poll `launchctl print` and
# wait for it to report the service is gone before proceeding. Cap the
# wait so a genuine stuck teardown surfaces instead of hanging.
echo "==> waiting for bootout to drain"
drain_deadline=$(( $(date -u +%s) + 10 ))
while (( $(date -u +%s) < drain_deadline )); do
  if ! launchctl print "$SERVICE_TARGET" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

# An orphaned gateway (one launchd lost track of, so bootout can't kill
# it) can survive the drain above while still holding the listen port.
# The `launchctl print` poll cannot see it — it only tracks the service
# record, not the socket. If we bootstrap anyway, the new instance
# crash-loops on EADDRINUSE under KeepAlive forever. Fail the deploy
# here and name the culprit PID instead.
GATEWAY_PORT=$(
  python3 -c 'import json,sys; print(json.load(sys.stdin).get("port", 8910))' \
    < "${HOME}/Library/Preferences/ratatoskr/config.json" 2>/dev/null \
    || echo 8910
)
# Poll briefly: the booted-out process may still be releasing its
# socket for a moment after launchd drops the service record. Only a
# hold that persists past the grace window is a genuine orphan.
port_deadline=$(( $(date -u +%s) + 5 ))
port_holder=""
while (( $(date -u +%s) < port_deadline )); do
  port_holder=$(lsof -nP -t -iTCP:"$GATEWAY_PORT" -sTCP:LISTEN || true)
  [[ -z "$port_holder" ]] && break
  sleep 0.5
done
if [[ -n "$port_holder" ]]; then
  echo "!! port $GATEWAY_PORT is still held after bootout by PID(s): $port_holder" >&2
  lsof -nP -iTCP:"$GATEWAY_PORT" -sTCP:LISTEN >&2 || true
  echo "!! refusing to bootstrap into a guaranteed EADDRINUSE crash loop." >&2
  echo "!! kill the holder (kill $port_holder) and re-run the deploy." >&2
  exit 2
fi

echo "==> bootstrapping $LAUNCHD_LABEL"
launchctl bootstrap "$DOMAIN_TARGET" "$LAUNCHD_PLIST"

echo "==> waiting for gateway to come back (timeout: ${HEALTH_TIMEOUT_SECS}s)"
# Poll the log for a "gateway listening" line whose timestamp is at or
# after MARKER_UTC. String comparison works because RFC 3339 timestamps
# sort lexicographically. (RunAtLoad=true in the plist makes bootstrap
# launch the process immediately; we don't need a separate kickstart.)
deadline=$(( $(date -u +%s) + HEALTH_TIMEOUT_SECS ))
while (( $(date -u +%s) < deadline )); do
  # tail -n 40 keeps this bounded even if the log is long. We look for
  # any recent line of the form `<rfc3339> ... gateway listening`.
  latest_listening=$(
    tail -n 40 "$LOG_PATH" 2>/dev/null \
      | grep "gateway listening" \
      | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}' \
      | tail -n 1 \
      || true
  )
  # >= not >: bind-first makes the gateway listen within milliseconds of
  # exec, so the listening line routinely lands in the SAME second as
  # MARKER_UTC. Strict > declared those instant startups dead (observed
  # live on the rename deploy: marker 16:27:27, listening 16:27:27.785).
  if [[ -n "$latest_listening" && ! "$latest_listening" < "$MARKER_UTC" ]]; then
    echo "==> gateway listening (log timestamp: ${latest_listening}Z)"
    exit 0
  fi
  sleep 1
done

echo "!! gateway did NOT return a post-restart 'gateway listening' line within ${HEALTH_TIMEOUT_SECS}s" >&2
echo "!! marker: ${MARKER_UTC}Z" >&2
echo "!! most recent log tail:" >&2
tail -n 10 "$LOG_PATH" >&2 || true
exit 2
