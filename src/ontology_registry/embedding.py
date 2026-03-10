"""Embed ontology descriptions using Kreuzberg."""

from kreuzberg import (
    extract_bytes,
    ExtractionConfig,
    ChunkingConfig,
    EmbeddingConfig,
    EmbeddingModelType,
)

from .models import OntologySchema


def embedding_config() -> EmbeddingConfig:
    """Shared embedding configuration used across the project."""
    return EmbeddingConfig(
        model=EmbeddingModelType.preset("balanced"),
        normalize=True,
    )


async def embed_text(text: str, max_chars: int = 5000) -> list[float]:
    """Embed arbitrary text and return a single vector.

    For texts that produce multiple chunks, vectors are
    mean-pooled and L2-normalised.
    """
    config = ExtractionConfig(
        chunking=ChunkingConfig(
            max_chars=max_chars,
            embedding=embedding_config(),
        ),
    )
    result = await extract_bytes(
        text.encode("utf-8"),
        mime_type="text/plain",
        config=config,
    )
    chunks = result.chunks
    if len(chunks) == 1:
        return chunks[0].embedding

    dim = len(chunks[0].embedding)
    pooled = [
        sum(c.embedding[i] for c in chunks) / len(chunks) for i in range(dim)
    ]
    norm = sum(x * x for x in pooled) ** 0.5
    return [x / norm for x in pooled] if norm > 0 else pooled


async def embed_ontology(schema: OntologySchema) -> OntologySchema:
    """Embed the ontology description text using Kreuzberg."""
    schema.embedding = await embed_text(schema.description_text, max_chars=8000)
    return schema
