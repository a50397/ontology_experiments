"""CLI command registry. Import all command modules here."""

from . import load_ontologies, ingest

COMMANDS = [
    load_ontologies,
    ingest,
]
