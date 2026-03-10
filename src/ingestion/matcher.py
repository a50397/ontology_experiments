"""
matcher.py

Given extracted document text, determine which ontology
(from the registry) best describes it.

Embeds a document excerpt via Kreuzberg, then performs
vector similarity search against ontology embeddings
in Qdrant to find the best-matching ontology.
"""

from dataclasses import dataclass, field

from qdrant_client import AsyncQdrantClient

from ..ontology_registry.embedding import embed_text
from ..ontology_registry.qdrant import ONTO_COLLECTION

EXCERPT_LENGTH = 2000


@dataclass
class OntologyCandidate:
    """A single candidate returned by vector search."""
    ontology_id: str
    name: str
    iri: str
    score: float
    class_names: list[str] = field(default_factory=list)
    property_names: list[str] = field(default_factory=list)
    description: str = ""


@dataclass
class OntologyMatch:
    """The selected best-match ontology for a document."""
    ontology_id: str
    ontology_name: str
    vector_score: float
    matched_classes: list[str] = field(default_factory=list)
    matched_properties: list[str] = field(default_factory=list)


async def _search_ontologies(
    doc_text: str,
    qdrant: AsyncQdrantClient,
    top_k: int = 3,
) -> list[OntologyCandidate]:
    """Embed a document excerpt and search the ontology collection."""
    excerpt = doc_text[:EXCERPT_LENGTH]
    query_vector = await embed_text(excerpt)

    hits = await qdrant.search(
        collection_name=ONTO_COLLECTION,
        query_vector=query_vector,
        limit=top_k,
    )

    return [
        OntologyCandidate(
            ontology_id=hit.payload["ontology_id"],
            name=hit.payload["name"],
            iri=hit.payload["iri"],
            score=hit.score,
            class_names=hit.payload.get("class_names", []),
            property_names=hit.payload.get("property_names", []),
            description=hit.payload.get("description", ""),
        )
        for hit in hits
    ]


async def match_document_to_ontology(
    doc_text: str,
    qdrant: AsyncQdrantClient,
    min_vector_score: float = 0.3,
) -> OntologyMatch | None:
    """
    Match a document to the best ontology via vector similarity.

    Returns None if no ontology scores above the threshold.
    """
    candidates = await _search_ontologies(doc_text, qdrant)

    for candidate in candidates:
        if candidate.score >= min_vector_score:
            return OntologyMatch(
                ontology_id=candidate.ontology_id,
                ontology_name=candidate.name,
                vector_score=candidate.score,
                matched_classes=candidate.class_names,
                matched_properties=candidate.property_names,
            )

    return None
