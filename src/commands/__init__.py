"""CLI command registry. Import all command modules here."""

from . import load_ontologies

COMMANDS = [
    load_ontologies,
]
