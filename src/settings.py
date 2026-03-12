from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    model_config = SettingsConfigDict(env_file=_ENV_FILE, env_file_encoding='utf-8', extra='ignore')

    log_level: str = 'INFO'
    ontologies_path: str = './ontologies'
    qdrant_api_key: str = ''
    qdrant_host: str = 'localhost'
    qdrant_port: int = 6333
    qdrant_grpc_port: int = 6334
    qdrant_ssl: bool = False
    neo4j_user: str = 'neo4j'
    neo4j_password: str = ''
    neo4j_host: str = 'localhost'
    neo4j_http_port: int = 7474
    neo4j_bolt_port: int = 7687
    neo4j_db: str = 'neo4j'

@lru_cache
def get_settings() -> Settings:
    return Settings()