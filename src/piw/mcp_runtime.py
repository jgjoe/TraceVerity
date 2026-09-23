from __future__ import annotations

import asyncio
import copy
import sys
import threading
from concurrent.futures import Future
from pathlib import Path
from typing import Any, Coroutine, TypeVar

import mcp.types as types
from mcp import Client
from mcp.client.stdio import StdioServerParameters

from .datasets import DEFAULT_REGISTRY_ROOT
from .tools import CoreToolSurface, canonical_bytes, canonical_json

_T = TypeVar("_T")


class McpProtocolError(RuntimeError):
    pass


def _definition_from_tool(tool: types.Tool) -> dict[str, Any]:
    return {
        "description": tool.description,
        "name": tool.name,
        "parameters": tool.input_schema,
    }


def _structured_envelope(result: types.CallToolResult) -> dict[str, Any]:
    envelope = result.structured_content
    if not isinstance(envelope, dict) or not all(
        isinstance(key, str) for key in envelope
    ):
        raise McpProtocolError("MCP tool result omitted a structured JSON object")
    expected_text = canonical_json(envelope)
    text_content = [
        item.text for item in result.content if isinstance(item, types.TextContent)
    ]
    if text_content != [expected_text]:
        raise McpProtocolError("MCP text content is not the canonical structured envelope")
    if result.is_error != ("error" in envelope):
        raise McpProtocolError("MCP error flag disagrees with the Core envelope")
    return envelope


class McpToolRuntime:
    """Managed synchronous facade over one official MCP stdio client lifecycle."""

    transport = "stdio"

    def __init__(
        self,
        database_path: Path,
        registry_root: Path = DEFAULT_REGISTRY_ROOT,
        *,
        startup_timeout_seconds: float = 30,
        request_timeout_seconds: float = 300,
    ) -> None:
        self._database_path = database_path.resolve()
        self._registry_root = Path(registry_root).resolve()
        if not self._database_path.is_file():
            raise FileNotFoundError(self._database_path)
        self._startup_timeout_seconds = startup_timeout_seconds
        self._request_timeout_seconds = request_timeout_seconds
        self._definitions: list[dict[str, Any]] | None = None
        self._protocol_version: str | None = None
        self._client: Client | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._startup_error: BaseException | None = None
        self._ready = threading.Event()
        self._terminated = threading.Event()
        self._closed = False
        self._thread = threading.Thread(
            target=self._thread_main,
            name="piw-mcp-stdio-client",
            daemon=False,
        )
        self._thread.start()
        if not self._ready.wait(self._startup_timeout_seconds):
            self.close()
            raise McpProtocolError("MCP stdio client did not initialize in time")
        if self._startup_error is not None:
            self.close()
            raise McpProtocolError("MCP stdio client initialization failed") from self._startup_error

    @property
    def protocol_version(self) -> str:
        if self._protocol_version is None:
            raise McpProtocolError("MCP client is not initialized")
        return self._protocol_version

    @property
    def is_alive(self) -> bool:
        return self._thread.is_alive()

    @property
    def closed(self) -> bool:
        return self._closed and self._terminated.is_set()

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._run_client())
        except BaseException as exc:
            self._startup_error = exc
            self._ready.set()
        finally:
            self._client = None
            self._terminated.set()

    async def _run_client(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        parameters = StdioServerParameters(
            command=sys.executable,
            args=[
                "-m",
                "piw.mcp_server",
                "--database",
                str(self._database_path),
                "--registry-root",
                str(self._registry_root),
            ],
            cwd=str(Path.cwd().resolve()),
        )
        async with Client(
            parameters,
            read_timeout_seconds=self._request_timeout_seconds,
        ) as client:
            listing = await client.list_tools(cache_mode="refresh")
            if listing.next_cursor is not None:
                raise McpProtocolError("MCP tool list was unexpectedly paginated")
            expected = CoreToolSurface.definitions()
            actual = [_definition_from_tool(tool) for tool in listing.tools]
            if canonical_bytes(actual) != canonical_bytes(expected):
                raise McpProtocolError("MCP tool names, descriptions, or input schemas differ from Core")
            self._definitions = copy.deepcopy(expected)
            self._protocol_version = client.protocol_version
            self._client = client
            self._ready.set()
            await self._stop_event.wait()

    def _submit(self, coroutine: Coroutine[Any, Any, _T]) -> _T:
        if self._closed or self._loop is None or self._client is None:
            coroutine.close()
            raise McpProtocolError("MCP stdio client is closed")
        future: Future[_T] = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        try:
            return future.result(timeout=self._request_timeout_seconds)
        except BaseException:
            future.cancel()
            raise

    async def _call_tool(self, tool: str, arguments: dict[str, Any] | None) -> dict[str, Any]:
        if self._client is None:
            raise McpProtocolError("MCP stdio client is not initialized")
        result = await self._client.call_tool(
            tool,
            arguments,
            read_timeout_seconds=self._request_timeout_seconds,
        )
        return _structured_envelope(result)

    def definitions(self) -> list[dict[str, Any]]:
        if self._definitions is None:
            raise McpProtocolError("MCP tool definitions are unavailable")
        return copy.deepcopy(self._definitions)

    def dispatch(self, tool: Any, arguments: Any) -> dict[str, Any]:
        if not isinstance(tool, str) or (
            arguments is not None and not isinstance(arguments, dict)
        ):
            # A model may emit a malformed request before MCP is invoked.
            # Normalize it through the exact Core tool validator so the Agent
            # sees the same fail-closed envelope on both transports.
            return CoreToolSurface(self._database_path, self._registry_root).dispatch(
                tool, arguments
            )
        return self._submit(self._call_tool(tool, arguments))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._loop is not None and self._stop_event is not None:
            self._loop.call_soon_threadsafe(self._stop_event.set)
        self._thread.join(timeout=10)
        if self._thread.is_alive():
            raise McpProtocolError("MCP stdio subprocess lifecycle did not terminate cleanly")

    def __enter__(self) -> McpToolRuntime:
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.close()
