"""Neo4j storage operations for ontology schemas."""

from .models import OntologySchema


async def delete_from_neo4j(driver, ontology_id: str, database: str | None = None):
    """Delete all ontology nodes matching an ontology_id and their classes/properties from Neo4j."""
    async with driver.session(database=database) as session:
        # Delete the ontology node (and its relationships).
        # Then clean up any class/property nodes that are no longer
        # referenced by any remaining ontology.
        result = await session.run(
            """
            MATCH (o:Ontology {ontology_id: $oid})
            OPTIONAL MATCH (o)-[:DEFINES_CLASS]->(c:OntologyClass)
            OPTIONAL MATCH (o)-[:DEFINES_PROPERTY]->(p:OntologyProperty)
            DETACH DELETE o
            WITH collect(DISTINCT c) AS classes, collect(DISTINCT p) AS props
            UNWIND classes AS c
            WITH c, props
            WHERE c IS NOT NULL AND NOT EXISTS { (:Ontology)-[:DEFINES_CLASS]->(c) }
            DETACH DELETE c
            WITH props
            UNWIND props AS p
            WITH p
            WHERE p IS NOT NULL AND NOT EXISTS { (:Ontology)-[:DEFINES_PROPERTY]->(p) }
            DETACH DELETE p
            """,
            oid=ontology_id,
        )
        await result.consume()


async def register_in_neo4j(driver, schema: OntologySchema, database: str | None = None):
    """
    Materialize the ontology schema as a graph in Neo4j.
    This creates the 'template' that documents will be
    mapped onto during ingestion.

    Uses UNWIND-based batching to minimize round-trips.
    """
    classes_data = [
        {
            "iri": cls.iri,
            "name": cls.name,
            "label": cls.label,
            "comment": cls.comment,
        }
        for cls in schema.classes
    ]
    subclass_data = [
        {"child_iri": cls.iri, "parent_iri": p}
        for cls in schema.classes
        for p in cls.parents
    ]
    props_data = [
        {
            "iri": prop.iri,
            "name": prop.name,
            "label": prop.label,
            "prop_type": prop.prop_type,
        }
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

    async with driver.session(database=database) as session:
        # 1. Create/update the Ontology node.
        result = await session.run(
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
            result = await session.run(
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
            result = await session.run(
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
            result = await session.run(
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
            result = await session.run(
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
            result = await session.run(
                """
                UNWIND $rels AS rel
                MATCH (p:OntologyProperty {iri: rel.prop_iri})
                MATCH (c:OntologyClass {iri: rel.cls_iri})
                MERGE (p)-[:HAS_RANGE]->(c)
                """,
                rels=range_data,
            )
            await result.consume()
