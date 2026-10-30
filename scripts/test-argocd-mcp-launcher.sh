#!/usr/bin/env bash
set -euo pipefail

# Mock commands share this file so the test never invokes real argocd or npx.
case "${0##*/}" in
  argocd)
    expected=(account get-user-info --grpc-web --config "${HOME}/.config/argocd/config" --server argocd.tools.vcs.rivianvw.io)
    [ "$#" -eq "${#expected[@]}" ] || exit 91
    for arg in "${expected[@]}"; do
      [ "$1" = "$arg" ] || exit 92
      shift
    done
    printf '%s\n' 'CLI stdout must not leak: secret-cli-output'
    printf '%s\n' "${MOCK_ERROR}" >&2
    exit "${MOCK_STATUS}"
    ;;
  npx)
    [ "$#" -eq 2 ] && [ "$1" = argocd-mcp@latest ] && [ "$2" = stdio ] || exit 93
    [ "${ARGOCD_BASE_URL}" = https://argocd.tools.vcs.rivianvw.io ] || exit 94
    [ "${ARGOCD_API_TOKEN}" = secret-fixture-token ] || exit 95
    printf '%s\n' 'mock MCP stdio'
    exit 0
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAUNCHER="${1:-${SCRIPT_DIR}/argocd-mcp-launcher.sh}"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/bin" "${TMP}/home with spaces/.config/argocd"
ln -s "${SCRIPT_DIR}/test-argocd-mcp-launcher.sh" "${TMP}/bin/argocd"
ln -s "${SCRIPT_DIR}/test-argocd-mcp-launcher.sh" "${TMP}/bin/npx"
cp "${SCRIPT_DIR}/argocd-test-config.yaml" "${TMP}/home with spaces/.config/argocd/config"
export HOME="${TMP}/home with spaces"
export PATH="${TMP}/bin:${PATH}"

fail() { printf 'FAIL: %s\n' "$*" >&2; exit 1; }
run_case() {
  local name="$1" expected="$2" status=0
  export MOCK_STATUS="$3" MOCK_ERROR="$4"
  bash "${LAUNCHER}" >"${TMP}/stdout" 2>"${TMP}/stderr" || status=$?
  [ "$status" -eq "$expected" ] || fail "${name}: exit ${status}, expected ${expected}"
  if [ "$expected" -ne 0 ]; then
    [ ! -s "${TMP}/stdout" ] || fail "${name}: stdout not empty"
    if LC_ALL=C grep -Eq 'secret-|Bearer|refresh_token' "${TMP}/stderr"; then
      fail "${name}: diagnostic leaked sensitive mock output"
    fi
  fi
}

# Pin known auth failures to an exact, correctly quoted recovery command.
for error in 'oauth2: invalid_grant' 'rpc error: code = Unauthenticated' 'session has expired' 'Token is expired' 'signature has expired'; do
  run_case auth 1 1 "${error} refresh_token=secret-refresh Bearer secret-bearer"
  grep -Fxq "Run: argocd login argocd.tools.vcs.rivianvw.io --sso --grpc-web --config \"${HOME}/.config/argocd/config\"" "${TMP}/stderr" || fail 'auth: missing exact login command'
  grep -Fq 'reconnect-upstream' "${TMP}/stderr" || fail 'auth: missing reconnect instruction'
done

# Pin network and unknown failures to non-auth advice, with no raw output.
for error in 'dial tcp: i/o timeout' 'lookup host: no such host' 'connection refused' 'unexpected response' 'x509: certificate has expired'; do
  run_case network 1 1 "${error} secret-network"
  grep -Fq 'network or unclassified error' "${TMP}/stderr" || fail 'network: wrong classification'
  if grep -Eq 'argocd login|reconnect-upstream' "${TMP}/stderr"; then
    fail 'network: auth recovery advice present'
  fi
done

# Pin successful startup to MCP stdout only, despite noisy CLI output.
run_case success 0 0 'secret-success-error'
[ "$(<"${TMP}/stdout")" = 'mock MCP stdio' ] || fail 'success: stdout polluted'
[ ! -s "${TMP}/stderr" ] || fail 'success: unexpected stderr'

# Pin missing config errors to safe diagnostics rather than login advice.
rm "${HOME}/.config/argocd/config"
run_case missing-config 1 0 ''
grep -Fq 'Could not read auth-token' "${TMP}/stderr" || fail 'missing-config: diagnostic absent'
if grep -Eq 'argocd login|reconnect-upstream' "${TMP}/stderr"; then
  fail 'missing-config: auth recovery advice present'
fi
printf '%s\n' 'PASS: 5 auth cases, 5 network/unclassified cases, success, missing config; stdout and credential checks passed.'
