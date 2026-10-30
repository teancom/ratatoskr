"""Read-only smoke checks against the locally deployed gateway."""
import json
import urllib.request

URL = "http://127.0.0.1:8910/mcp"
headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


def rpc(method, params=None, ident=1):
    body = {"jsonrpc": "2.0", "method": method}
    if params is not None:
        body["params"] = params
    if ident is not None:
        body["id"] = ident
    request = urllib.request.Request(URL, data=json.dumps(body).encode(), headers=headers)
    with urllib.request.urlopen(request, timeout=120) as response:
        session = response.headers.get("Mcp-Session-Id")
        if session:
            headers["Mcp-Session-Id"] = session
        text = response.read().decode()
    if not text:
        return None
    if text.lstrip().startswith("{"):
        return json.loads(text)
    return json.loads(next(line[5:].strip() for line in text.splitlines() if line.startswith("data:") and line[5:].strip().startswith("{")))


rpc("initialize", {"protocolVersion": "2025-03-26", "capabilities": {}, "clientInfo": {"name": "local-smoke", "version": "1"}})
rpc("notifications/initialized", ident=None)
for name, arguments in [("ping", {}), ("list-servers", {})]:
    response = rpc("tools/call", {"name": name, "arguments": arguments})
    print(name, json.dumps(response))
    assert "error" not in response, response
    assert not response["result"].get("isError"), response
    if name == "reconnect-upstream":
        assert "reconnected" in json.dumps(response).lower(), response
