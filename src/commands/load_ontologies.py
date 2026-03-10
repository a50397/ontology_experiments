"""Load ontologies from a directory into Qdrant and Neo4j."""

import argparse

from ..clients import Clients
from ..ontology_registry import load_ontology_directory
from ..settings import Settings


def register(subparsers: argparse._SubParsersAction):
    parser = subparsers.add_parser("load_ontologies", help="Load ontologies from a directory")
    parser.add_argument("path", nargs="?", help="Directory containing ontology files (default: from settings)")
    parser.set_defaults(run=run)


async def run(args: argparse.Namespace, clients: Clients, settings: Settings):
    path = args.path or settings.ontologies_path
    schemas = await load_ontology_directory(
        directory=path,
        qdrant=clients.qdrant,
        neo4j_driver=clients.neo4j,
        neo4j_db=clients.neo4j_db,
    )
    print(f"Loaded {len(schemas)} ontologies.")
