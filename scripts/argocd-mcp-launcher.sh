#!/usr/bin/env bash
# Refresh the CLI session, then launch the MCP server with its current token.
# Reconnect the gateway's argocd upstream to capture a renewed token; reloading
# unchanged gateway configuration does not restart this process.
set -euo pipefail

ARGOCD_SERVER="argocd.tools.vcs.rivianvw.io"
ARGOCD_CLI_CONFIG="${HOME}/.config/argocd/config"

# Capture both streams: CLI output must never enter the MCP stdio channel.
# Do not print raw errors: OAuth responses can contain credentials or tokens.
if CLI_ERROR="$(argocd account get-user-info --grpc-web \
    --config "${ARGOCD_CLI_CONFIG}" --server "${ARGOCD_SERVER}" 2>&1)"; then
  unset CLI_ERROR
else
  if printf '%s\n' "${CLI_ERROR}" | LC_ALL=C grep -Eqi \
      'invalid_grant|unauthenticated|session (has )?(expired|is expired)|token (has |is )?expired|signature (has |is )?expired'; then
    printf '%s\n' \
      'argocd session authentication failed.' \
      "Run: argocd login ${ARGOCD_SERVER} --sso --grpc-web --config \"${ARGOCD_CLI_CONFIG}\"" \
      "Then call the gateway's reconnect-upstream tool for the argocd upstream (not reload-config)." >&2
  else
    printf '%s\n' \
      'argocd session check failed with a network or unclassified error; authentication expiry is not confirmed.' \
      'Check connectivity and CLI configuration, then retry. Raw CLI output is withheld to protect credentials.' >&2
  fi
  exit 1
fi

# The CLI config has a single user entry. Keep token selection scoped to that
# existing layout; never include config contents or parser errors in diagnostics.
if ! TOKEN="$(awk '$0 ~ /auth-token:/ { print $NF; exit }' "${ARGOCD_CLI_CONFIG}" 2>/dev/null)" || [ -z "${TOKEN}" ]; then
  printf '%s\n' 'Could not read auth-token from the configured argocd CLI config.' >&2
  exit 1
fi

export ARGOCD_BASE_URL="https://${ARGOCD_SERVER}"
export ARGOCD_API_TOKEN="${TOKEN}"
exec npx argocd-mcp@latest stdio
