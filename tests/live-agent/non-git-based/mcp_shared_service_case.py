#!/usr/bin/env python3
"""Small direct MCP listener smoke test (daemon must already be running)."""
import argparse, json, socket, time
from pathlib import Path

def call(sock, method, params=None, ident=1):
    msg = {"jsonrpc":"2.0", "id":ident, "method":method}
    if params is not None: msg["params"] = params
    sock.sendall((json.dumps(msg)+"\n").encode())
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(65536)
        if not chunk: raise RuntimeError("MCP EOF before response")
        data += chunk
    return json.loads(data)

def notify(sock, method, params=None):
    msg = {"jsonrpc":"2.0", "method":method}
    if params is not None: msg["params"] = params
    sock.sendall((json.dumps(msg)+"\n").encode())

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--socket", required=True, help="daemon RPC socket; MCP uses .mcp")
    args = p.parse_args(); path = str(Path(args.socket).with_suffix(".mcp"))
    a = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); a.connect(path)
    b = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM); b.connect(path)
    init = {"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"direct-live","version":"1"}}
    assert "result" in call(a, "initialize", init, 1)
    notify(a, "notifications/initialized")
    assert "result" in call(b, "initialize", init, 2)
    notify(b, "notifications/initialized")
    a.close(); time.sleep(.05)
    assert "result" in call(b, "tools/list", {}, 3)
    b.close()
    print(json.dumps({"connections":2,"eof_isolated":True,"tools_list":True}))

if __name__ == "__main__": main()
