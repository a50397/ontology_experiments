"""
pipeline.py

Ingestion pipeline that:
1. Extracts a document with Kreuzberg
2. Matches to the best ontology via vector search
3. Stores vectors in Qdrant + graph in Neo4j
"""

import hashlib
import re
from pathlib import Path

from kreuzberg import (
    extract_file,
    ExtractionConfig,
    ChunkingConfig,
)
from qdrant_client.models import PointStruct

from ..clients import Clients
from ..ontology_registry.embedding import embedding_config
from .matcher import match_document_to_ontology, OntologyMatch

COLLECTION = "documents"

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_cypher_identifier(value: str, context: str) -> None:
    """Raise ValueError if *value* is not a safe Neo4j label / relationship-type identifier.

    Neo4j does not support parameterised labels or relationship types, so any
    value that will be interpolated into a Cypher query must be validated before
    use.  Only identifiers that consist solely of ASCII letters, digits and
    underscores (and start with a letter or underscore) are accepted.
    """
    if not _SAFE_IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Unsafe Cypher identifier in {context}: {value!r}. "
            "Only ASCII letters, digits and underscores are allowed, "
            "and the identifier must start with a letter or underscore."
        )


# ── Neo4j helpers ────────────────────────────────────────


async def _create_document_node(
    session, doc_id: str, file_name: str, onto_match: OntologyMatch | None
):
    """Create the Document node and optionally link it to an Ontology."""
    await session.run(
        """
        MERGE (d:Document {doc_id: $doc_id})
        SET d.file_name = $fname,
            d.ontology_id = $oid,
            d.ontology_name = $oname,
            d.ingested_at = datetime()
        """,
        doc_id=doc_id,
        fname=file_name,
        oid=onto_match.ontology_id if onto_match else None,
        oname=onto_match.ontology_name if onto_match else None,
    )

    if onto_match:
        await session.run(
            """
            MATCH (d:Document {doc_id: $doc_id})
            MATCH (o:Ontology {ontology_id: $oid})
            MERGE (d)-[:GOVERNED_BY]->(o)
            """,
            doc_id=doc_id,
            oid=onto_match.ontology_id,
        )


async def _create_chunk_node(session, chunk_id: str, text: str, index: int, doc_id: str):
    """Create a Chunk node linked to its Document, plus a NEXT edge from the previous chunk."""
    await session.run(
        """
        MERGE (c:Chunk {chunk_id: $cid})
        SET c.text = $text, c.chunk_index = $idx
        WITH c
        MATCH (d:Document {doc_id: $did})
        MERGE (d)-[:HAS_CHUNK]->(c)
        """,
        cid=chunk_id,
        text=text,
        idx=index,
        did=doc_id,
    )

    if index > 0:
        await session.run(
            """
            MATCH (a:Chunk {chunk_id: $prev})
            MATCH (b:Chunk {chunk_id: $curr})
            MERGE (a)-[:NEXT]->(b)
            """,
            prev=f"{doc_id}_chunk_{index - 1}",
            curr=chunk_id,
        )


async def _store_entities(
    session,
    entities_data: dict,
    chunk_id: str,
    doc_id: str,
    ontology_id: str,
):
    """Create Entity nodes in Neo4j linked to the ontology class and source chunk."""
    for entity in entities_data.get("entities", []):
        _validate_cypher_identifier(entity["class"], "entity class")
        entity_id = f"{doc_id}_{entity['class']}_{entity['name']}"

        await session.run(
            f"""
            MERGE (e:Entity:{entity['class']} {{entity_id: $eid}})
            SET e.name = $name,
                e.ontology_id = $oid
            WITH e
            MATCH (oc:OntologyClass {{name: $class_name}})
            MERGE (e)-[:INSTANCE_OF]->(oc)
            WITH e
            MATCH (c:Chunk {{chunk_id: $chunk_id}})
            MERGE (c)-[:MENTIONS]->(e)
            """,
            eid=entity_id,
            name=entity["name"],
            oid=ontology_id,
            class_name=entity["class"],
            chunk_id=chunk_id,
        )

        for prop_name, prop_val in entity.get("properties", {}).items():
            await session.run(
                """
                MATCH (e:Entity {entity_id: $eid})
                SET e += $props
                """,
                eid=entity_id,
                props={prop_name: prop_val},
            )

    for rel in entities_data.get("relationships", []):
        _validate_cypher_identifier(rel["relation"], "relationship type")
        await session.run(
            f"""
            MATCH (a:Entity {{name: $from_name}})
            MATCH (b:Entity {{name: $to_name}})
            WHERE a.ontology_id = $oid AND b.ontology_id = $oid
            MERGE (a)-[r:`{rel['relation']}`]->(b)
            """,
            from_name=rel["from_entity"],
            to_name=rel["to_entity"],
            oid=ontology_id,
        )


# ── Qdrant helpers ───────────────────────────────────────


def _build_chunk_point(
    chunk_id: str,
    embedding: list[float],
    text: str,
    doc_id: str,
    index: int,
    file_name: str,
    onto_match: OntologyMatch | None,
) -> PointStruct:
    return PointStruct(
        id=chunk_id,
        vector=embedding,
        payload={
            "doc_id": doc_id,
            "chunk_index": index,
            "text": text,
            "file_name": file_name,
            "ontology_id": onto_match.ontology_id if onto_match else None,
            "ontology_name": onto_match.ontology_name if onto_match else None,
        },
    )


# ── Public API ───────────────────────────────────────────


async def ingest_document(
    file_path: str | Path,
    clients: Clients,
    entities_data_per_chunk: dict[int, dict] | None = None,
) -> str:
    """
    Ontology-aware document ingestion.

    Args:
        file_path: Path to the document to ingest.
        clients: Shared database clients.
        entities_data_per_chunk: Optional mapping of chunk index to
            pre-extracted entity data dicts.

    Returns:
        The document ID (hex digest).
    """
    file_path = Path(file_path)

    # Extract, chunk, and embed the document
    config = ExtractionConfig(
        chunking=ChunkingConfig(
            max_chars=1000,
            max_overlap=200,
            embedding=embedding_config(),
        ),
    )
    result = await extract_file(file_path, config=config)
    doc_id = hashlib.sha256(file_path.read_bytes()).hexdigest()[:16]

    # Find the best-matching ontology
    onto_match = await match_document_to_ontology(
        doc_text=result.content,
        qdrant=clients.qdrant,
    )

    # Persist to Neo4j and Qdrant
    async with clients.neo4j.session(database=clients.neo4j_db) as session:
        await _create_document_node(session, doc_id, file_path.name, onto_match)

        points = []
        for i, chunk in enumerate(result.chunks):
            chunk_id = f"{doc_id}_chunk_{i}"

            points.append(_build_chunk_point(
                chunk_id, chunk.embedding, chunk.content,
                doc_id, i, file_path.name, onto_match,
            ))
            await _create_chunk_node(session, chunk_id, chunk.content, i, doc_id)

            if onto_match and entities_data_per_chunk and i in entities_data_per_chunk:
                await _store_entities(
                    session, entities_data_per_chunk[i],
                    chunk_id, doc_id, onto_match.ontology_id,
                )

        await clients.qdrant.upsert(collection_name=COLLECTION, points=points)

    return doc_id
