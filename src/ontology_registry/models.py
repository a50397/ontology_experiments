"""Data models for ontology schemas."""

from dataclasses import dataclass, field


@dataclass
class OntologyClass:
    """A class extracted from an ontology."""
    name: str
    iri: str
    label: str | None = None
    comment: str | None = None
    parents: list[str] = field(default_factory=list)


@dataclass
class OntologyProperty:
    """A property (object or data) from an ontology."""
    name: str
    iri: str
    prop_type: str  # "object" or "data"
    domain: list[str] = field(default_factory=list)
    range: list[str] = field(default_factory=list)
    label: str | None = None


@dataclass
class OntologySchema:
    """Full schema extracted from one ontology file."""
    ontology_id: str
    name: str
    iri: str
    file_path: str
    classes: list[OntologyClass] = field(default_factory=list)
    properties: list[OntologyProperty] = field(default_factory=list)
    description_text: str = ""  # serialized for embedding
    embedding: list[float] | None = None
