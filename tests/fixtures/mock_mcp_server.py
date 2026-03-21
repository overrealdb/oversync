#!/usr/bin/env python3
"""Minimal MCP server for testing. Reads JSON-RPC from stdin, writes to stdout."""
import json
import sys

TOOLS_DATA = [
    {"id": "1", "name": "alice", "score": 95},
    {"id": "2", "name": "bob", "score": 87},
    {"id": "3", "name": "charlie", "score": 72},
]

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        req = json.loads(line)
    except json.JSONDecodeError:
        continue

    method = req.get("method", "")
    req_id = req.get("id")

    if method == "initialize":
        resp = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "mock-mcp", "version": "0.1.0"},
            },
        }
        print(json.dumps(resp), flush=True)

    elif method == "notifications/initialized":
        pass  # no response for notifications

    elif method == "tools/call":
        tool_name = req.get("params", {}).get("name", "")
        content_text = json.dumps(TOOLS_DATA)
        resp = {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "content": [{"type": "text", "text": content_text}]
            },
        }
        print(json.dumps(resp), flush=True)

    elif req_id is not None:
        resp = {
            "jsonrpc": "2.0",
            "id": req_id,
            "error": {"code": -32601, "message": f"unknown method: {method}"},
        }
        print(json.dumps(resp), flush=True)
