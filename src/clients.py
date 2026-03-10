"""Shared client initialization."""

from dataclasses import dataclass

from neo4j import AsyncGraphDatabase, AsyncDriver
from qdrant_client import AsyncQdrantClient

from .settings import Settings


@dataclass
class Clients:
    qdrant: AsyncQdrantClient
    neo4j: AsyncDriver
    neo4j_db: str

    async def close(self):
        await self.neo4j.close()
        await self.qdrant.close()


def create_clients(settings: Settings) -> Clients:
    qdrant = AsyncQdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        grpc_port=settings.qdrant_grpc_port,
        api_key=settings.qdrant_api_key or None,
        https=settings.qdrant_ssl,
    )
    neo4j_driver = AsyncGraphDatabase.driver(
        f"bolt://{settings.neo4j_host}:{settings.neo4j_bolt_port}",
        auth=(settings.neo4j_user, settings.neo4j_password),
    )

    return Clients(qdrant=qdrant, neo4j=neo4j_driver, neo4j_db=settings.neo4j_db)
