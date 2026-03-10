"""Shared Qdrant collection management."""

import asyncio

from qdrant_client import AsyncQdrantClient
from qdrant_client.models import VectorParams, Distance


class QdrantCollection:
    """Lazy-ensures a Qdrant collection exists with the right vector size.

    Each instance manages one collection and caches the confirmed vector
    size so that repeated calls skip the Qdrant round-trip.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self._cached_vector_size: int | None = None
        self._ensure_lock = asyncio.Lock()

    async def ensure(
        self, qdrant: AsyncQdrantClient, vector_size: int | None = None
    ) -> bool:
        """Ensure the collection exists, optionally creating it.

        Returns ``True`` if the collection exists (or was just created).
        Returns ``False`` only when ``vector_size`` is ``None`` and the
        collection doesn't exist yet (cannot create without a size).

        Raises ``ValueError`` on vector-dimension mismatch.
        """
        if self._cached_vector_size is not None:
            if vector_size is not None and vector_size != self._cached_vector_size:
                raise ValueError(
                    f"Embedding dimension {vector_size} does not match "
                    f"existing {self.name!r} collection vector size {self._cached_vector_size}"
                )
            return True

        async with self._ensure_lock:
            if self._cached_vector_size is not None:
                if vector_size is not None and vector_size != self._cached_vector_size:
                    raise ValueError(
                        f"Embedding dimension {vector_size} does not match "
                        f"existing {self.name!r} collection vector size {self._cached_vector_size}"
                    )
                return True

            collections = [c.name for c in (await qdrant.get_collections()).collections]
            if self.name not in collections:
                if vector_size is None:
                    return False
                await qdrant.create_collection(
                    collection_name=self.name,
                    vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
                )
                self._cached_vector_size = vector_size
            else:
                existing_size = (await qdrant.get_collection(self.name)).config.params.vectors.size
                if vector_size is not None and existing_size != vector_size:
                    raise ValueError(
                        f"Embedding dimension {vector_size} does not match "
                        f"existing {self.name!r} collection vector size {existing_size}"
                    )
                self._cached_vector_size = existing_size

        return True
