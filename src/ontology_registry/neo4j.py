"""Neo4j storage operations for ontology schemas."""

from .models import OntologySchema


_DETACH_ONTOLOGY_QUERY = """
MATCH (o:Ontology {ontology_id: $oid})
DETACH DELETE o
"""

_CLEANUP_ORPHANED_SCHEMA_QUERY = """
MATCH (c:OntologyClass)
WHERE NOT EXISTS { (:Ontology)-[:DEFINES_CLASS]->(c) }
  AND NOT EXISTS { (:Entity)-[:INSTANCE_OF]->(c) }
DETACH DELETE c
WITH 1 AS _
MATCH (p:OntologyProperty)
WHERE NOT EXISTS { (:Ontology)-[:DEFINES_PROPERTY]->(p) }
DETACH DELETE p
"""


async def delete_from_neo4j(driver, ontology_id: str, database: str | None = None):
    """Delete an ontology and clean up classes/properties that are no longer referenced."""
    async def _delete(tx):
        result = await tx.run(_DETACH_ONTOLOGY_QUERY, oid=ontology_id)
        await result.consume()
        result = await tx.run(_CLEANUP_ORPHANED_SCHEMA_QUERY)
        await result.consume()
        
    async with driver.session(database=database) as session:
        await session.execute_write(_delete)


async def replace_in_neo4j(driver, schema: OntologySchema, database: str | None = None):
    """
    Atomically replace an ontology in Neo4j: delete the old data and
    write the new schema in a single transaction so the graph is never
    in a partially-deleted state.
    """
    classes_data, subclass_data, props_data, domain_data, range_data = _prepare_schema_data(schema)

    async def _replace(tx):
        # Detach the ontology node (removes DEFINES_* edges) but leave
        # classes/properties intact so Entity relationships survive.
        result = await tx.run(_DETACH_ONTOLOGY_QUERY, oid=schema.ontology_id)
        await result.consume()

        # Write the new schema — re-MERGEs classes/properties and recreates
        # DEFINES_* edges for classes that still exist in the new schema.
        await _write_ontology_data(
            tx, schema, classes_data, subclass_data, props_data, domain_data, range_data
        )

        # Now clean up classes/properties that are no longer defined by any
        # ontology and have no entity references.
        result = await tx.run(_CLEANUP_ORPHANED_SCHEMA_QUERY)
        await result.consume()

    async with driver.session(database=database) as session:
        await session.execute_write(_replace)


async def _write_ontology_data(tx, schema, classes_data, subclass_data, props_data, domain_data, range_data):
    """Write ontology schema data within an existing transaction."""
    # 1. Create/update the Ontology node.
    result = await tx.run(
        """
        MERGE (o:Ontology {ontology_id: $oid})
        SET o.name = $name,
            o.iri = $iri,
            o.file_path = $file_path
        """,
        oid=schema.ontology_id,
        name=schema.name,
        iri=schema.iri,
        file_path=schema.file_path,
    )
    await result.consume()

    # 2. Batch-create classes and link to ontology.
    if classes_data:
        result = await tx.run(
            """
            MATCH (o:Ontology {ontology_id: $oid})
            UNWIND $classes AS cls
            MERGE (c:OntologyClass {iri: cls.iri})
            SET c.name = cls.name,
                c.label = cls.label,
                c.comment = cls.comment
            MERGE (o)-[:DEFINES_CLASS]->(c)
            """,
            oid=schema.ontology_id,
            classes=classes_data,
        )
        await result.consume()

    # 3. Batch-create SUBCLASS_OF relationships.
    if subclass_data:
        result = await tx.run(
            """
            UNWIND $rels AS rel
            MATCH (child:OntologyClass {iri: rel.child_iri})
            MATCH (parent:OntologyClass {iri: rel.parent_iri})
            MERGE (child)-[:SUBCLASS_OF]->(parent)
            """,
            rels=subclass_data,
        )
        await result.consume()

    # 4. Batch-create properties and link to ontology.
    if props_data:
        result = await tx.run(
            """
            MATCH (o:Ontology {ontology_id: $oid})
            UNWIND $props AS prop
            MERGE (p:OntologyProperty {iri: prop.iri})
            SET p.name = prop.name,
                p.label = prop.label,
                p.prop_type = prop.prop_type
            MERGE (o)-[:DEFINES_PROPERTY]->(p)
            """,
            oid=schema.ontology_id,
            props=props_data,
        )
        await result.consume()

    # 5. Batch-create HAS_DOMAIN relationships.
    if domain_data:
        result = await tx.run(
            """
            UNWIND $rels AS rel
            MATCH (p:OntologyProperty {iri: rel.prop_iri})
            MATCH (c:OntologyClass {iri: rel.cls_iri})
            MERGE (p)-[:HAS_DOMAIN]->(c)
            """,
            rels=domain_data,
        )
        await result.consume()

    # 6. Batch-create HAS_RANGE relationships.
    if range_data:
        result = await tx.run(
            """
            UNWIND $rels AS rel
            MATCH (p:OntologyProperty {iri: rel.prop_iri})
            MATCH (c:OntologyClass {iri: rel.cls_iri})
            MERGE (p)-[:HAS_RANGE]->(c)
            """,
            rels=range_data,
        )
        await result.consume()


def _prepare_schema_data(schema: OntologySchema):
    """Pre-compute the data lists needed by _write_ontology_data."""
    classes_data = [
        {"iri": cls.iri, "name": cls.name, "label": cls.label, "comment": cls.comment}
        for cls in schema.classes
    ]
    subclass_data = [
        {"child_iri": cls.iri, "parent_iri": p}
        for cls in schema.classes
        for p in cls.parents
    ]
    props_data = [
        {"iri": prop.iri, "name": prop.name, "label": prop.label, "prop_type": prop.prop_type}
        for prop in schema.properties
    ]
    domain_data = [
        {"prop_iri": prop.iri, "cls_iri": d}
        for prop in schema.properties
        for d in prop.domain
    ]
    range_data = [
        {"prop_iri": prop.iri, "cls_iri": r}
        for prop in schema.properties
        for r in prop.range
    ]
    return classes_data, subclass_data, props_data, domain_data, range_data


async def register_in_neo4j(driver, schema: OntologySchema, database: str | None = None):
    """
    Materialize the ontology schema as a graph in Neo4j.
    This creates the 'template' that documents will be
    mapped onto during ingestion.

    Uses UNWIND-based batching to minimize round-trips.
    """
    classes_data, subclass_data, props_data, domain_data, range_data = _prepare_schema_data(schema)

    async def _write_ontology(tx):
        await _write_ontology_data(
            tx, schema, classes_data, subclass_data, props_data, domain_data, range_data
        )

    async with driver.session(database=database) as session:
        await session.execute_write(_write_ontology)
