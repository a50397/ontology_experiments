"""Qdrant storage operations for ontology schemas."""

import uuid

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)

from ..qdrant_utils import QdrantCollection
from .models import OntologySchema

_ONTO_UUID_NAMESPACE = uuid.UUID("b4a2c3d5-6e7f-8091-a2b3-c4d5e6f7a8b9")


ONTO_COLLECTION = "ontologies"

_collection = QdrantCollection(ONTO_COLLECTION)


async def delete_from_qdrant(qdrant: AsyncQdrantClient, ontology_id: str):
    """Delete all ontology points matching an ontology_id from Qdrant."""
    if not await _collection.ensure(qdrant):
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

    await _collection.ensure(qdrant, vector_size=len(schema.embedding))

    await qdrant.upsert(
        collection_name=ONTO_COLLECTION,
        points=[
            PointStruct(
                id=str(uuid.uuid5(_ONTO_UUID_NAMESPACE, schema.ontology_id)),
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
