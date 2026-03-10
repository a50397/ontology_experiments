"""
ontology_registry

Load ontology files, extract their schemas (classes,
properties, relationships), embed them, and register
in the system so documents can be matched to the right
ontology at ingestion time.
"""

from pathlib import Path

from qdrant_client import AsyncQdrantClient

from .models import OntologyClass, OntologyProperty, OntologySchema
from .parser import parse_ontology_file, SUPPORTED_EXTENSIONS
from .embedding import embed_ontology, embed_text, embedding_config
from .qdrant import delete_from_qdrant, register_in_qdrant
from .neo4j import delete_from_neo4j, register_in_neo4j

__all__ = [
    "OntologyClass",
    "OntologyProperty",
    "OntologySchema",
    "parse_ontology_file",
    "embed_ontology",
    "embed_text",
    "embedding_config",
    "delete_from_qdrant",
    "register_in_qdrant",
    "delete_from_neo4j",
    "register_in_neo4j",
    "load_ontology_directory",
    "SUPPORTED_EXTENSIONS",
]


async def load_ontology_directory(
    directory: str | Path,
    qdrant: AsyncQdrantClient,
    neo4j_driver,
    neo4j_db: str | None = None,
):
    """
    Scan a directory for ontology files (.owl, .ttl, .n3, .nt, .rdf, .xml),
    parse each one, embed it, and register in both Qdrant and Neo4j.
    """
    directory = Path(directory)
    schemas = []

    for ontology_file in sorted(directory.rglob("*")):
        if ontology_file.suffix.lower() not in SUPPORTED_EXTENSIONS:
            continue
        print(f"Loading ontology: {ontology_file.name}")
        schema = parse_ontology_file(ontology_file)
        # Embed first (pure compute) so a failure here won't leave storage partially deleted.
        schema = await embed_ontology(schema)
        await delete_from_qdrant(qdrant, schema.ontology_id)
        await delete_from_neo4j(neo4j_driver, schema.ontology_id, database=neo4j_db)
        await register_in_qdrant(qdrant, schema)
        await register_in_neo4j(neo4j_driver, schema, database=neo4j_db)
        schemas.append(schema)
        print(
            f"  → {len(schema.classes)} classes, "
            f"{len(schema.properties)} properties"
        )

    return schemas
