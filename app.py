import os
import json
import uuid
import asyncio
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from fastapi import FastAPI, Request, Header, HTTPException, Query
from fastapi.responses import StreamingResponse, JSONResponse, Response

from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError



STORAGE_CONN = os.getenv("STORAGE_CONN", "")
CONTAINER_NAME = os.getenv("BLOB_CONTAINER", "mcp-notes")
BLOB_NAME = os.getenv("BLOB_NAME", "notes.json")

def _blob_client():
    if not STORAGE_CONN:
        raise RuntimeError("STORAGE_CONN is not set")
    bsc = BlobServiceClient.from_connection_string(STORAGE_CONN)
    cc = bsc.get_container_client(CONTAINER_NAME)
    try:
        cc.create_container()
    except Exception:
        pass
    return cc.get_blob_client(BLOB_NAME)

def load_notes() -> List[dict]:
    bc = _blob_client()
    try:
        data = bc.download_blob().readall()
        return json.loads(data.decode("utf-8"))
    except ResourceNotFoundError:
        return []
    except Exception:
        return []

def save_notes(notes: List[dict]) -> None:
    bc = _blob_client()
    payload = json.dumps(notes, ensure_ascii=False).encode("utf-8")
    bc.upload_blob(payload, overwrite=True)


APP = FastAPI(title="MCP ToDo/Notes (PoC)")



# ====== セッション管理（SSE 接続ごとの送信キュー）=====
_sessions: Dict[str, "asyncio.Queue[str]"] = {}
_sessions_lock = asyncio.Lock()

def _sse_event(event: str, data: str) -> str:
    return f"event: {event}\ndata: {data}\n\n"

async def _enqueue(session_id: str, msg_obj: Dict[str, Any]) -> None:
    async with _sessions_lock:
        q = _sessions.get(session_id)
    if not q:
        return
    await q.put(json.dumps(msg_obj, ensure_ascii=False))

# ====== MCP(JSON-RPC) 最小ハンドラ ======
def _jsonrpc_error(id_value, code: int, message: str):
    return {"jsonrpc": "2.0", "id": id_value, "error": {"code": code, "message": message}}

def _jsonrpc_result(id_value, result: Any):
    return {"jsonrpc": "2.0", "id": id_value, "result": result}

def _tool_list() -> Dict[str, Any]:
    return {
        "tools": [
            {
                "name": "add_note",
                "description": "Add a note to blob-backed notes store",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "text": {"type": "string"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["text"],
                },
            },
            {
                "name": "list_notes",
                "description": "List notes (optionally filter by tag/limit)",
                "inputSchema": {
                    "type": "object",
                    "properties": {"limit": {"type": "integer"}, "tag": {"type": "string"}},
                },
            },
            {
                "name": "delete_note",
                "description": "Delete a note by its ID",
                "inputSchema": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                    "required": ["id"],
                },
            },
        ]
    }

def _call_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    # ここはあなたの既存の load_notes/save_notes を呼ぶ前提
    if name == "add_note":
        text = args.get("text")
        if not text:
            raise ValueError("text is required")
        tags = args.get("tags") or []
        notes = load_notes()
        note = {
            "id": str(uuid.uuid4()),
            "text": text,
            "tags": tags,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        notes.append(note)
        save_notes(notes)
        return {"content": [{"type": "text", "text": f"Added: {note['id']}"}], "note": note}

    if name == "list_notes":
        limit = args.get("limit")
        tag = args.get("tag")
        notes = list(reversed(load_notes()))
        if tag:
            notes = [n for n in notes if tag in (n.get("tags") or [])]
        if isinstance(limit, int):
            notes = notes[:limit]
        return {"content": [{"type": "text", "text": json.dumps(notes, ensure_ascii=False)}], "notes": notes}

    if name == "delete_note":
        note_id = args.get("id")
        if not note_id:
            raise ValueError("id is required")
        notes = load_notes()
        original_len = len(notes)
        notes = [n for n in notes if n.get("id") != note_id]
        if len(notes) == original_len:
            raise KeyError(f"Note {note_id} not found")
        save_notes(notes)
        return {"content": [{"type": "text", "text": f"Deleted: {note_id}"}]}

    raise KeyError(f"Unknown tool: {name}")

def _handle_mcp_jsonrpc(msg: Dict[str, Any]) -> Dict[str, Any]:
    idv = msg.get("id")
    method = msg.get("method")
    params = msg.get("params") or {}

    if msg.get("jsonrpc") != "2.0" or not method:
        return _jsonrpc_error(idv, -32600, "Invalid Request")

    if method == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "serverInfo": {"name": "mcp-todo", "version": "0.1"},
            "capabilities": {"tools": {}},
        }
        return _jsonrpc_result(idv, result)

    if method in ("tools/list", "tools.list"):
        return _jsonrpc_result(idv, _tool_list())

    if method in ("tools/call", "tools.call"):
        tool_name = params.get("name")
        arguments = params.get("arguments") or {}
        if not tool_name:
            return _jsonrpc_error(idv, -32602, "Missing params.name")
        try:
            res = _call_tool(tool_name, arguments)
            return _jsonrpc_result(idv, res)
        except ValueError as e:
            return _jsonrpc_error(idv, -32602, str(e))
        except KeyError as e:
            return _jsonrpc_error(idv, -32601, str(e))
        except Exception as e:
            return _jsonrpc_error(idv, -32000, f"Server error: {e}")

    return _jsonrpc_error(idv, -32601, f"Method not found: {method}")

# ====== SSE endpoint (legacy MCP SSE transport) ======
@APP.get("/sse")
async def sse(request: Request):

    session_id = uuid.uuid4().hex
    q: asyncio.Queue[str] = asyncio.Queue()

    async with _sessions_lock:
        _sessions[session_id] = q


    # ✅ proxy配下では base_url が http になりがちなので forwarded headers を優先
    proto = request.headers.get("x-forwarded-proto") or request.url.scheme or "https"
    host = request.headers.get("x-forwarded-host") or request.headers.get("host")
    post_url = f"{proto}://{host}/messages?session_id={session_id}"


    async def event_stream():
        try:
            # ★必須: endpoint イベントで POST 先を通知（最初に送る）[1](https://dev.classmethod.jp/articles/mcp-sse/)[2](https://www.grizzlypeaksoftware.com/library/debugging-mcp-connections-and-transport-issues-msnc7pdr)
            yield _sse_event("endpoint", post_url)

            yield _sse_event("ready", "{\"message\":\"connected\"}")

            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(q.get(), timeout=15.0)
                    # ★応答は message イベントで返す [1](https://dev.classmethod.jp/articles/mcp-sse/)[2](https://www.grizzlypeaksoftware.com/library/debugging-mcp-connections-and-transport-issues-msnc7pdr)
                    yield _sse_event("message", payload)
                except asyncio.TimeoutError:
                    yield _sse_event("ping", "{}")
        finally:
            async with _sessions_lock:
                _sessions.pop(session_id, None)

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@APP.post("/messages")
async def messages(
    request: Request,
    session_id: str = Query(...),
):

    async with _sessions_lock:
        q = _sessions.get(session_id)
    if not q:
        raise HTTPException(status_code=400, detail="Invalid or expired session_id")

    msg = await request.json()

    # ★ここが重要：関数名ミス修正（handle_mcp_jsonrpc → _handle_mcp_jsonrpc）
    resp = _handle_mcp_jsonrpc(msg)

    await _enqueue(session_id, resp)

    return Response(status_code=200)
