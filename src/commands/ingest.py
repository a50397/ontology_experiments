"""Ingest a document into Qdrant and Neo4j."""

import argparse

from ..clients import Clients
from ..ingestion import ingest_document
from ..settings import Settings


def register(subparsers: argparse._SubParsersAction):
    parser = subparsers.add_parser("ingest", help="Ingest a document file")
    parser.add_argument("file", help="Path to the document to ingest")
    parser.set_defaults(run=run)


async def run(args: argparse.Namespace, clients: Clients, settings: Settings):
    doc_id = await ingest_document(file_path=args.file, clients=clients)
    print(f"Ingested document: {doc_id}")
