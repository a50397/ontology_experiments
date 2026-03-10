"""
ingestion

Document ingestion pipeline: extract, match to an ontology,
and store vectors + graph structure.
"""

from .matcher import match_document_to_ontology, OntologyMatch
from .pipeline import ingest_document

__all__ = [
    "ingest_document",
    "match_document_to_ontology",
    "OntologyMatch",
]
