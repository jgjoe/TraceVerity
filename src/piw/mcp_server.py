from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from .tools import CoreToolSurface, canonical_json

SERVER_NAME = "process-intelligence-workbench"
SERVER_VERSION = "0.1.0"


def create_server(database_path: Path) -> Server[Any]:
    surface = CoreToolSurface(database_path)

    async def list_tools(
        _context: Any, _params: types.PaginatedRequestParams | None
    ) -> types.ListToolsResult:
        return types.ListToolsResult(
            tools=[
                types.Tool(
                    name=definition["name"],
                    description=definition["description"],
                    input_schema=definition["parameters"],
                )
                for definition in surface.definitions()
            ]
        )

    async def call_tool(
        _context: Any, params: types.CallToolRequestParams
    ) -> types.CallToolResult:
        envelope = surface.dispatch(params.name, params.arguments)
        return types.CallToolResult(
            content=[types.TextContent(text=canonical_json(envelope))],
            structured_content=envelope,
            is_error="error" in envelope,
        )

    return Server(
        SERVER_NAME,
        version=SERVER_VERSION,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )


async def serve(database_path: Path) -> None:
    server = create_server(database_path)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description="Run the PIW MCP stdio server.")
    command.add_argument("--database", type=Path, required=True)
    return command


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    database_path = args.database.resolve()
    if not database_path.is_file():
        raise FileNotFoundError(database_path)
    asyncio.run(serve(database_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
