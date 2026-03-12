"""
pipeline.py

Ingestion pipeline that:
1. Extracts a document with Kreuzberg
2. Matches to the best ontology via vector search
3. Stores vectors in Qdrant + graph in Neo4j
"""

import hashlib
import logging
import re
import uuid
from pathlib import Path

from kreuzberg import (
    extract_file,
    ExtractionConfig,
    ChunkingConfig,
)
from qdrant_client.models import PointStruct

from ..clients import Clients
from ..ontology_registry.embedding import embedding_config
from ..qdrant_utils import QdrantCollection
from .matcher import match_document_to_ontology, OntologyMatch

logger = logging.getLogger(__name__)

COLLECTION = "documents"

_NON_IDENTIFIER_CHAR_RE = re.compile(r"[^A-Za-z0-9_]")

_collection = QdrantCollection(COLLECTION)


def _safe_cypher_label(value: str) -> str:
    """Deterministically map *value* to a safe Cypher label / relationship type.

    Replaces non-identifier characters with underscores and ensures the result
    starts with a letter or underscore.  The original name is always preserved
    in node/relationship properties, so the mapping only needs to be consistent,
    not reversible.
    """
    if not value:
        return "_empty"
    sanitized = _NON_IDENTIFIER_CHAR_RE.sub("_", value)
    if sanitized[0].isdigit():
        sanitized = f"_{sanitized}"
    if sanitized != value:
        logger.debug("Sanitized Cypher identifier %r -> %r", value, sanitized)
    return sanitized


# ── Neo4j helpers ────────────────────────────────────────


async def _create_document_node(
    tx, doc_id: str, file_name: str, onto_match: OntologyMatch | None
):
    """Create the Document node and optionally link it to an Ontology."""
    result = await tx.run(
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
    await result.consume()

    if onto_match:
        result = await tx.run(
            """
            MATCH (d:Document {doc_id: $doc_id})
            MATCH (o:Ontology {ontology_id: $oid})
            MERGE (d)-[:GOVERNED_BY]->(o)
            """,
            doc_id=doc_id,
            oid=onto_match.ontology_id,
        )
        await result.consume()


async def _create_chunk_node(tx, chunk_id: str, text: str, index: int, doc_id: str):
    """Create a Chunk node linked to its Document, plus a NEXT edge from the previous chunk."""
    result = await tx.run(
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
    await result.consume()

    if index > 0:
        result = await tx.run(
            """
            MATCH (a:Chunk {chunk_id: $prev})
            MATCH (b:Chunk {chunk_id: $curr})
            MERGE (a)-[:NEXT]->(b)
            """,
            prev=f"{doc_id}_chunk_{index - 1}",
            curr=chunk_id,
        )
        await result.consume()


async def _store_entities(
    tx,
    entities_data: dict,
    chunk_id: str,
    doc_id: str,
    ontology_id: str,
):
    """Create Entity nodes in Neo4j linked to the ontology class and source chunk.

    Entity creation still runs one query per entity because the dynamic
    label (``Entity:<class>``) cannot be parameterised in Cypher.
    Properties are set in the same query to avoid extra round-trips.
    Relationships are UNWIND-batched into a single query per relation type.
    """
    entity_id_by_name: dict[str, str] = {}

    for entity in entities_data.get("entities", []):
        class_label = _safe_cypher_label(entity["class"])
        entity_id = f"{doc_id}_{class_label}_{entity['name']}"
        props = dict(entity.get("properties", {}))

        # Check that the ontology class exists before creating the entity.
        result = await tx.run(
            """
            MATCH (o:Ontology {ontology_id: $oid})-[:DEFINES_CLASS]->(oc {name: $class_name})
            RETURN oc.iri AS iri
            """,
            oid=ontology_id,
            class_name=entity["class"],
        )
        record = await result.single()
        if record is None:
            logger.warning(
                "Skipped entity %r (class %r): ontology class not found in ontology %s",
                entity["name"],
                entity["class"],
                ontology_id,
            )
            continue

        entity_id_by_name[entity["name"]] = entity_id

        result = await tx.run(
            f"""
            MATCH (oc:OntologyClass {{iri: $oc_iri}})
            MATCH (c:Chunk {{chunk_id: $chunk_id}})
            MERGE (e:Entity:{class_label} {{entity_id: $eid}})
            SET e += $props
            SET e.name = $name,
                e.ontology_id = $oid
            MERGE (e)-[:INSTANCE_OF]->(oc)
            MERGE (c)-[:MENTIONS]->(e)
            """,
            eid=entity_id,
            name=entity["name"],
            oid=ontology_id,
            oc_iri=record["iri"],
            chunk_id=chunk_id,
            props=props,
        )
        await result.consume()

    # Group relationships by relation type so each type can be
    # UNWIND-batched in a single query.
    rels_by_type: dict[str, list[dict[str, str]]] = {}
    for rel in entities_data.get("relationships", []):
        from_eid = entity_id_by_name.get(rel["from_entity"])
        to_eid = entity_id_by_name.get(rel["to_entity"])
        if not from_eid or not to_eid:
            continue
        rels_by_type.setdefault(rel["relation"], []).append(
            {"from_eid": from_eid, "to_eid": to_eid}
        )

    for rel_type, pairs in rels_by_type.items():
        safe_rel_type = _safe_cypher_label(rel_type)
        result = await tx.run(
            f"""
            UNWIND $pairs AS pair
            MATCH (a:Entity {{entity_id: pair.from_eid}})
            MATCH (b:Entity {{entity_id: pair.to_eid}})
            MERGE (a)-[:`{safe_rel_type}`]->(b)
            """,
            pairs=pairs,
        )
        await result.consume()


# ── Qdrant helpers ───────────────────────────────────────


_CHUNK_UUID_NAMESPACE = uuid.UUID("a3f1b2c4-5d6e-7f80-9a1b-2c3d4e5f6a7b")


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
        id=str(uuid.uuid5(_CHUNK_UUID_NAMESPACE, chunk_id)),
        vector=embedding,
        payload={
            "chunk_id": chunk_id,
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

    file_stat = file_path.stat()
    hasher = hashlib.sha256()
    hasher.update(str(file_path.resolve()).encode("utf-8"))
    hasher.update(str(file_stat.st_mtime_ns).encode("utf-8"))
    doc_id = hasher.hexdigest()[:16]

    # Find the best-matching ontology
    onto_match = await match_document_to_ontology(
        doc_text=result.content,
        qdrant=clients.qdrant,
    )

    # Persist to Neo4j and Qdrant
    points = []

    async def _write_graph(tx):
        await _create_document_node(tx, doc_id, file_path.name, onto_match)

        for i, chunk in enumerate(result.chunks):
            chunk_id = f"{doc_id}_chunk_{i}"

            points.append(_build_chunk_point(
                chunk_id, chunk.embedding, chunk.content,
                doc_id, i, file_path.name, onto_match,
            ))
            await _create_chunk_node(tx, chunk_id, chunk.content, i, doc_id)

            if onto_match and entities_data_per_chunk and i in entities_data_per_chunk:
                await _store_entities(
                    tx, entities_data_per_chunk[i],
                    chunk_id, doc_id, onto_match.ontology_id,
                )

    async with clients.neo4j.session(database=clients.neo4j_db) as session:
        await session.execute_write(_write_graph)

    if points:
        await _collection.ensure(clients.qdrant, vector_size=len(points[0].vector))
        await clients.qdrant.upsert(collection_name=COLLECTION, points=points)

    return doc_id
