import argparse
import asyncio

from .clients import create_clients
from .commands import COMMANDS
from .settings import get_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Ontology experiments CLI")
    sub = parser.add_subparsers(dest="command")
    for cmd in COMMANDS:
        cmd.register(sub)
    return parser


async def main():
    parser = _build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    settings = get_settings()
    clients = create_clients(settings)

    try:
        await args.run(args, clients, settings)
    finally:
        await clients.close()


if __name__ == "__main__":
    asyncio.run(main())
