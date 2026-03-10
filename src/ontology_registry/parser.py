"""Parse ontology files and extract class/property schemas."""

import hashlib
from pathlib import Path

from rdflib import Graph, RDF, RDFS, OWL, URIRef

from .models import OntologyClass, OntologyProperty, OntologySchema


SUPPORTED_EXTENSIONS = {".owl", ".rdf", ".xml", ".ttl", ".n3", ".nt"}


def _local_name(iri: str) -> str:
    """Extract the local name (fragment or last path segment) from an IRI."""
    s = str(iri)
    if "#" in s:
        return s.rsplit("#", 1)[-1]
    return s.rsplit("/", 1)[-1]


def _first_literal(g: Graph, subject, predicate) -> str | None:
    """Get the first literal value for a subject/predicate, or None."""
    for obj in g.objects(subject, predicate):
        return str(obj)
    return None


def parse_ontology_file(ontology_path: str | Path) -> OntologySchema:
    """
    Parse an ontology file (.owl, .ttl, .n3, .nt, .rdf, .xml)
    and extract the full class/property schema using rdflib.
    """
    ontology_path = Path(ontology_path)
    g = Graph()
    g.parse(ontology_path)

    # Detect ontology IRI
    ontology_iri = None
    for s in g.subjects(RDF.type, OWL.Ontology):
        ontology_iri = str(s)
        break

    # Use IRI or absolute path as stable ID (not content hash, which changes on edits)
    stable_key = ontology_iri or str(ontology_path.resolve())
    ontology_id = hashlib.sha256(stable_key.encode()).hexdigest()[:12]

    classes = _extract_classes(g)
    properties = _extract_properties(g)
    description = _build_description(g, ontology_path, ontology_iri, classes, properties)

    onto_name = ontology_path.stem
    if ontology_iri:
        onto_name = _local_name(ontology_iri) or onto_name

    return OntologySchema(
        ontology_id=ontology_id,
        name=onto_name,
        iri=ontology_iri or "",
        file_path=str(ontology_path),
        classes=classes,
        properties=properties,
        description_text=description,
    )


def _extract_classes(g: Graph) -> list[OntologyClass]:
    """Extract OWL and RDFS classes from the graph."""
    classes = []
    class_iris: set[str] = set()

    for cls_iri in g.subjects(RDF.type, OWL.Class):
        if not isinstance(cls_iri, URIRef):
            continue
        iri = str(cls_iri)
        if iri.startswith("http://www.w3.org/"):
            continue
        class_iris.add(iri)
        classes.append(OntologyClass(
            name=_local_name(iri),
            iri=iri,
            label=_first_literal(g, cls_iri, RDFS.label),
            comment=_first_literal(g, cls_iri, RDFS.comment),
            parents=[
                str(parent)
                for parent in g.objects(cls_iri, RDFS.subClassOf)
                if isinstance(parent, URIRef)
                and parent != OWL.Thing
                and parent != RDFS.Resource
            ],
        ))

    # Also pick up rdfs:Class definitions not typed as owl:Class
    for cls_iri in g.subjects(RDF.type, RDFS.Class):
        if not isinstance(cls_iri, URIRef):
            continue
        iri = str(cls_iri)
        if iri in class_iris or iri.startswith("http://www.w3.org/"):
            continue
        class_iris.add(iri)
        classes.append(OntologyClass(
            name=_local_name(iri),
            iri=iri,
            label=_first_literal(g, cls_iri, RDFS.label),
            comment=_first_literal(g, cls_iri, RDFS.comment),
            parents=[
                str(parent)
                for parent in g.objects(cls_iri, RDFS.subClassOf)
                if isinstance(parent, URIRef)
            ],
        ))

    return classes


def _extract_properties(g: Graph) -> list[OntologyProperty]:
    """Extract object and data properties from the graph."""
    properties = []
    seen_props: set[str] = set()

    for prop_type_iri, prop_type_label in [
        (OWL.ObjectProperty, "object"),
        (OWL.DatatypeProperty, "data"),
        (RDF.Property, "data"),
    ]:
        for prop_iri in g.subjects(RDF.type, prop_type_iri):
            if not isinstance(prop_iri, URIRef):
                continue
            iri = str(prop_iri)
            if iri in seen_props or iri.startswith("http://www.w3.org/"):
                continue
            seen_props.add(iri)
            properties.append(OntologyProperty(
                name=_local_name(iri),
                iri=iri,
                prop_type=prop_type_label,
                domain=[
                    str(d)
                    for d in g.objects(prop_iri, RDFS.domain)
                    if isinstance(d, URIRef)
                ],
                range=[
                    str(r)
                    for r in g.objects(prop_iri, RDFS.range)
                    if isinstance(r, URIRef)
                ],
                label=_first_literal(g, prop_iri, RDFS.label),
            ))

    return properties


def _build_description(
    g: Graph,
    ontology_path: Path,
    ontology_iri: str | None,
    classes: list[OntologyClass],
    properties: list[OntologyProperty],
) -> str:
    """Build a human-readable description text for embedding."""
    onto_name = ontology_path.stem
    if ontology_iri:
        onto_name = _local_name(ontology_iri) or onto_name

    desc_parts = [f"Ontology: {onto_name}"]

    if ontology_iri:
        onto_comment = _first_literal(g, g.resource(ontology_iri).identifier, RDFS.comment)
        if onto_comment:
            desc_parts.append(f"Description: {onto_comment}")

    desc_parts.append("\nClasses:")
    for cls in classes:
        line = f"  - {cls.label or cls.name}"
        if cls.comment:
            line += f": {cls.comment}"
        if cls.parents:
            line += f" (subclass of: {', '.join(_local_name(p) for p in cls.parents)})"
        desc_parts.append(line)

    desc_parts.append("\nRelationships:")
    for prop in properties:
        if prop.prop_type == "object" and prop.domain and prop.range:
            desc_parts.append(
                f"  - {' | '.join(_local_name(d) for d in prop.domain)} "
                f"--[{prop.label or prop.name}]--> "
                f"{' | '.join(_local_name(r) for r in prop.range)}"
            )

    desc_parts.append("\nAttributes:")
    for prop in properties:
        if prop.prop_type == "data" and prop.domain:
            desc_parts.append(
                f"  - {' | '.join(_local_name(d) for d in prop.domain)}"
                f".{prop.label or prop.name}"
            )

    return "\n".join(desc_parts)
