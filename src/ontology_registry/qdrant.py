"""Qdrant storage operations for ontology schemas."""

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)

from .models import OntologySchema


ONTO_COLLECTION = "ontologies"

_cached_vector_size: int | None = None


async def _ensure_collection(qdrant: AsyncQdrantClient, vector_size: int | None = None) -> bool:
    """Ensure collection exists correctly once per process to avoid polling Qdrant on every operation."""
    global _cached_vector_size

    # Already confirmed to exist — just validate size if requested.
    if _cached_vector_size is not None:
        if vector_size is not None and vector_size != _cached_vector_size:
            raise ValueError(
                f"Embedding dimension {vector_size} does not match "
                f"existing collection vector size {_cached_vector_size}"
            )
        return True

    collections = [c.name for c in (await qdrant.get_collections()).collections]
    if ONTO_COLLECTION not in collections:
        if vector_size is None:
            return False  # Cannot create it and it doesn't exist
        await qdrant.create_collection(
            collection_name=ONTO_COLLECTION,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )
        _cached_vector_size = vector_size
    else:
        existing_size = (await qdrant.get_collection(ONTO_COLLECTION)).config.params.vectors.size
        if vector_size is not None and existing_size != vector_size:
            raise ValueError(
                f"Embedding dimension {vector_size} does not match "
                f"existing collection vector size {existing_size}"
            )
        _cached_vector_size = existing_size

    return True


async def delete_from_qdrant(qdrant: AsyncQdrantClient, ontology_id: str):
    """Delete all ontology points matching an ontology_id from Qdrant."""
    if not await _ensure_collection(qdrant):
        return
    await qdrant.delete(
        collection_name=ONTO_COLLECTION,
        points_selector=Filter(
            must=[FieldCondition(key="ontology_id", match=MatchValue(value=ontology_id))]
        ),
    )


async def register_in_qdrant(qdrant: AsyncQdrantClient, schema: OntologySchema):
    """Store ontology embedding in a dedicated collection."""
    if not schema.embedding:
        raise ValueError(
            f"Ontology '{schema.name}' has no embedding. "
            "Run embed_ontology() before registering in Qdrant."
        )

    await _ensure_collection(qdrant, vector_size=len(schema.embedding))

    await qdrant.upsert(
        collection_name=ONTO_COLLECTION,
        points=[
            PointStruct(
                id=int(schema.ontology_id, base=16),
                vector=schema.embedding,
                payload={
                    "ontology_id": schema.ontology_id,
                    "name": schema.name,
                    "iri": schema.iri,
                    "file_path": schema.file_path,
                    "class_names": [c.name for c in schema.classes],
                    "property_names": [p.name for p in schema.properties],
                    "description": schema.description_text,
                },
            )
        ],
    )
