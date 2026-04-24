import logging
import os
import subprocess
from datetime import datetime

import networkx as nx
from dotenv import load_dotenv

# from kgs_rnd_ontoverse.utils.models import Collection
# Defining node models with neomodel
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from neomodel import ArrayProperty, IntegerProperty, RelationshipTo, StringProperty, StructuredNode, config, db
from tqdm import tqdm

load_dotenv()

logger = logging.getLogger(__name__)


class Paper(StructuredNode):
    abstract = StringProperty()
    authors = StringProperty()
    collectionTags = StringProperty()
    date = StringProperty()
    doi = StringProperty()
    issue = StringProperty()
    itemID = IntegerProperty(unique_index=True)
    journal_abbreviation = StringProperty()
    nodeID = StringProperty(unique_index=True)
    pages = StringProperty()
    title = StringProperty()
    visibilityLevel = StringProperty()
    visibilityLevelTopic = StringProperty()
    volume = StringProperty()
    similarPapers = ArrayProperty()

    # Relationships
    similar_papers = RelationshipTo("Paper", "SIMILAR")
    within_topic = RelationshipTo("Paper", "WITHIN_TOPIC")
    between_topic = RelationshipTo("Paper", "BETWEEN_TOPIC")


class PaperClone(StructuredNode):
    abstract = StringProperty()
    nodeID = StringProperty(unique_index=True)
    visibilityLevel = StringProperty()
    visibilityLevelTopic = StringProperty()
    collectionTags = StringProperty()
    itemID = IntegerProperty()
    clone_number = IntegerProperty()
    similarPapers = ArrayProperty()

    # Relationships
    similar_papers = RelationshipTo("PaperClone", "SIMILAR")
    within_topic = RelationshipTo("PaperClone", "WITHIN_TOPIC")
    between_topic = RelationshipTo("PaperClone", "BETWEEN_TOPIC")


class Collection(StructuredNode):
    collectionID = StringProperty(unique_index=True)
    collectionName = StringProperty()
    graphLevel = IntegerProperty()
    parent_of = RelationshipTo("Collection", "PARENT_OF")


class CollectionRelationships:
    parent_of = RelationshipTo("Collection", "PARENT_OF")
    member_of = RelationshipTo("Collection", "MEMBER_OF")


class PaperRelationships:
    member_of = RelationshipTo("Collection", "MEMBER_OF")


def _verify_and_log_neo4j_connectivity() -> None:
    """
    Log two distinct steps: (1) Bolt server reachable with credentials,
    (2) named database exists and accepts a session.
    """
    # Neo4j driver 5+ does not allow user:password in the URI; pass auth= explicitly.
    bolt_uri = "bolt://localhost:7687"
    password = os.getenv("NEO4J_PASSWORD")
    database_name = config.DATABASE_NAME
    driver = GraphDatabase.driver(bolt_uri, auth=("neo4j", password))
    try:
        driver.verify_connectivity()
        logger.info(
            "Neo4j server: connected and authenticated over Bolt (credentials accepted by the server)."
        )
    except Exception as e:
        logger.error("Neo4j server: could not connect or authenticate: %s", e)
        raise

    try:
        with driver.session(database=database_name) as session:
            session.run("RETURN 1 AS ok").consume()
        logger.info(
            "Neo4j database: found named database '%s' (session opened successfully).",
            database_name,
        )
    except Neo4jError as e:
        code = getattr(e, "code", "") or ""
        if "DatabaseNotFound" in code or "DatabaseNotFound" in str(e):
            logger.error(
                "Neo4j database: no database named '%s' on this server (create it or fix NEO4J_DB in .env).",
                database_name,
            )
        else:
            logger.error("Neo4j database: could not open database '%s': %s", database_name, e)
        raise
    finally:
        driver.close()


def neo4j_connection() -> None:
    """Configure neomodel for Neo4j (URL and database name only; no data changes)."""
    config.DATABASE_URL = f"bolt://neo4j:{os.getenv('NEO4J_PASSWORD')}@localhost:7687"
    database_name = os.getenv("NEO4J_DB")
    config.DATABASE_NAME = database_name
    _verify_and_log_neo4j_connectivity()


def purge_neo4j_if_configured() -> None:
    """
    Empty the configured Neo4j database when NEO4J_PURGE is True.

    Call once after loading config / environment so purge is explicit and separate
    from connection setup.
    """
    neo4j_connection()
    if os.environ.get("NEO4J_PURGE") != "True":
        return
    database_name = os.getenv("NEO4J_DB")
    logger.info("NEO4J_PURGE is True: clearing all data from Neo4j database '%s'.", database_name)
    db.cypher_query("MATCH (n) DETACH DELETE n")


def create_paper_nodes(
    toGraph: nx.Graph, item_collection_dict: dict[int, list[int]], oncodsai_library: dict[int, object]
) -> None:
    """
    Create paper nodes in bulk for the Neo4j database.

    :param toGraph: The graph containing nodes to be added.
    :param item_collection_dict: A dictionary mapping item IDs to their collection tags.
    :param oncodsai_library: A dictionary containing additional attributes for each item ID.
    """
    logger.info("Creating Paper Nodes in bulk")

    paper_data = []
    paper_clone_data = []

    for node in tqdm(toGraph.nodes.data(), desc="Preparing Paper Nodes for Batch Creation", leave=True):
        try:
            itemID, level, cloneNumber = node[0].split("_", 3)
            itemID = int(itemID)
            features = node[1]

            node_data = {
                "nodeID": node[0],
                "visibilityLevel": features["visibilityLevel"],
                "visibilityLevelTopic": features["visibilityLevelTopic"],
                "collectionTags": ",".join(map(str, item_collection_dict[itemID])),
                "additional_attributes": oncodsai_library[itemID].attributes if itemID in oncodsai_library else {},
            }

            if features["type"] == "Paper":
                paper_data.append(node_data)
            elif features["type"] == "PaperClone":
                paper_clone_data.append(node_data)

        except Exception as e:
            logger.error(f"Error processing node {node[0]}: {e}")
            pass
    
    save_nodes_bulk(paper_data, "Paper")
    save_nodes_bulk(paper_clone_data, "PaperClone")
    create_indexes()


def save_nodes_bulk(node_data: list[dict], label: str) -> None:
    """
    Save nodes in bulk using a Cypher query.

    :param node_data: A list of dictionaries containing node properties.
    :param label: The label of the node type (e.g., "Paper" or "PaperClone").
    """
    logger.info(f"Saving {label} nodes in bulk")
    cypher_query = f"""
    UNWIND $nodes AS node
    MERGE (n:{label} {{nodeID: node.nodeID}})
    SET n += node.properties
    """
    params = {
        "nodes": [
            {
                "nodeID": node["nodeID"],
                "properties": {
                    "visibilityLevel": node["visibilityLevel"],
                    "visibilityLevelTopic": node["visibilityLevelTopic"],
                    "collectionTags": node["collectionTags"],
                    **node["additional_attributes"],
                },
            }
            for node in node_data
        ]
    }
    db.cypher_query(cypher_query, params)


def nodes_update_graphlevel(topicGraph: nx.DiGraph) -> None:
    """
    Annotate the topic nodes with their graph level in the hierarchy.
    :param topicGraph: The directed graph representing the topic hierarchy.
    """
    logger.info("Annotating the topic nodes with Topic hierarchy graph level")
    treeLevels = nx.single_source_shortest_path_length(topicGraph, "ROOT")

    for collectionID, level in treeLevels.items():
        currentNode = Collection.nodes.get(collectionID=str(collectionID))
        currentNode.graphLevel = str(level)
        currentNode.save()


def create_indexes() -> None:
    """Create indexes for the Paper and PaperClone nodes in Neo4j."""
    logger.info("Creating indexes for Paper and PaperClone nodes")
    db.cypher_query("create index paper_itemID if not exists for (p:Paper) on p.itemID")
    db.cypher_query("create index paper_nodeID if not exists for (p:Paper) on p.nodeID")
    db.cypher_query("create index paperClone_nodeID if not exists for (p:PaperClone) on p.nodeID")
    db.cypher_query("create index paperClone_itemID if not exists for (p:PaperClone) on p.itemID")


def create_collection_nodes(topicGraph: nx.DiGraph) -> None:
    """
    Create collection nodes in Neo4j based on the topic graph.

    :param topicGraph: The directed graph containing topic nodes.
    """
    logger.info("Creating Collection Nodes")
    Collection(collectionID="ROOT", collectionName="Topics").save()

    labels = nx.get_node_attributes(topicGraph, "topicName")
    for collectionID, collectionName in labels.items():
        Collection(collectionID=str(collectionID), collectionName=collectionName).save()


def create_parent_of_relationship(topic_graph: nx.DiGraph) -> None:
    """
    Create PARENT_OF relationships between collection nodes in Neo4j.
    :param topic_graph: The directed graph representing the topic hierarchy.
    """
    logger.info("Creating PARENT_OF relationships")
    for parent, child in tqdm(
        topic_graph.edges,
        desc="Creating relationships <PARENT_OF> betweem Collection nodes",
        leave=True,
    ):
        db.cypher_query(
            f"""MATCH (a:Collection) WHERE a.collectionID = '{parent}'
            MATCH (b:Collection) WHERE b.collectionID = '{child}'
            MERGE (a)-[r:PARENT_OF]->(b)"""
        )


def create_member_of_relationship() -> None:
    """
    Create MEMBER_OF relationships for papers in Neo4j.
    """
    logger.info("Creating MEMBER_OF relationships")
    PAPER_MEMBER_OF_COLLECTION = """
        MATCH (c:Collection)
        MATCH (p:Paper)
        WHERE c.collectionID = p.visibilityLevelTopic 
        CREATE (p)-[r:MEMBER_OF]->(c);
    """
    db.cypher_query(PAPER_MEMBER_OF_COLLECTION)
    PAPERCLONE_MEMBER_OF_COLLECTION = """
        MATCH (c:Collection)
        MATCH (p:PaperClone)
        WHERE c.collectionID = p.visibilityLevelTopic
        CREATE (p)-[r:MEMBER_OF]->(c);
    """
    db.cypher_query(PAPERCLONE_MEMBER_OF_COLLECTION)


def process_batch(relationship_type):
    query = f"""
    UNWIND $edges AS edge
    MATCH (a:PaperClone) WHERE a.nodeID = edge.start_id
    MATCH (b:PaperClone) WHERE b.nodeID = edge.end_id
    MERGE (a)-[r:{relationship_type}]->(b)
    SET r.edgeWeight = edge.edgeWeight
    """
    return query


def create_paper_clone_relationships(
    within_edges: list[tuple], between_edges: list[tuple], batch_size: int = 1000
) -> None:
    """
    Create relationships between PaperClone nodes in Neo4j.
    :param withinEdges: A list of tuples representing edges within topics.
    :param betweenEdges: A list of tuples representing edges between topics.
    """
    # for start_id, _weight, end_id in tqdm(
    #     withinEdges,
    #     desc="Creating SIMILAR_TO_WITHIN_TOPIC edges within PaperClone nodes",
    #     leave=True,
    #     unit="edges",
    #     ):
    #     db.cypher_query(
    #         f"""MATCH (a:PaperClone) WHERE a.nodeID = '{start_id}'
    #         MATCH (b:PaperClone) WHERE b.nodeID = '{end_id}'
    #         MERGE (a)-[r:SIMILAR_TO_WITHIN_TOPIC]->(b)
    #         SET r.edgeWeight = {_weight['edgeWeight']}"""
    #     )
    for i in tqdm(
        range(0, len(within_edges), batch_size),
        desc="Creating SIMILAR_TO_WITHIN_TOPIC edges within PaperClone nodes in batches",
    ):
        batch = [
            {"start_id": start_id, "end_id": end_id, "edgeWeight": _weight["edgeWeight"]}
            for start_id, _weight, end_id in within_edges[i : i + batch_size]
        ]
        db.cypher_query(process_batch("SIMILAR_TO_WITHIN_TOPIC"), params={"edges": batch})

    # for start_id, _weight, end_id in tqdm(
    #     betweenEdges,
    #     desc="Creating SIMILAR_TO_BETWEEN_TOPIC edges between PaperClone nodes",
    #     leave=True,
    #     unit="edges",
    #     ):
    #     db.cypher_query(
    #         f"""MATCH (a:PaperClone) WHERE a.nodeID = '{start_id}'
    #         MATCH (b:PaperClone) WHERE b.nodeID = '{end_id}'
    #         MERGE (a)-[r:SIMILAR_TO_BETWEEN_TOPIC]->(b)
    #         SET r.edgeWeight = {_weight['edgeWeight']}"""
    #     )
    for i in tqdm(
        range(0, len(between_edges), batch_size),
        desc="Creating SIMILAR_TO_BETWEEN_TOPIC edges between PaperClone nodes in batches",
    ):
        batch = [
            {"start_id": start_id, "end_id": end_id, "edgeWeight": _weight["edgeWeight"]}
            for start_id, _weight, end_id in between_edges[i : i + batch_size]
        ]
        db.cypher_query(process_batch("SIMILAR_TO_BETWEEN_TOPIC"), params={"edges": batch})


def create_between_topic_relationships(betweenEdges: list[tuple]) -> None:
    """
    Create between-topic relationships in Neo4j.

    :param betweenEdges: A list of tuples representing edges between topics.
    """
    for start_id, _weight, end_id in tqdm(
        betweenEdges,
        desc="Creating SIMILAR_TO_BETWEEN_TOPIC edges between Paper and PaperClone nodes",
        leave=True,
        unit="edges",
    ):
        db.cypher_query(
            f"""
            MATCH (a:Paper {{nodeID: '{start_id}'}})
            MATCH (b:PaperClone {{nodeID: '{end_id}'}})
            CREATE (a)-[r:SIMILAR_TO_BETWEEN_TOPIC]->(b)
            SET r.edgeWeight = {_weight['edgeWeight']}
        """
        )
        db.cypher_query(
            f"""
            MATCH (a:PaperClone {{nodeID: '{start_id}'}})
            MATCH (b:Paper {{nodeID: '{end_id}'}})
            CREATE (a)-[r:SIMILAR_TO_BETWEEN_TOPIC]->(b)
            SET r.edgeWeight = {_weight['edgeWeight']}
        """
        )


def create_within_topic_relationships(withinEdges: list[tuple]) -> None:
    """
    Create within-topic relationships in Neo4j.

    :param withinEdges: A list of tuples representing edges within topics.
    """
    for start_id, _weight, end_id in tqdm(
        withinEdges,
        desc="Creating SIMILAR_TO_WITHIN_TOPIC edges between Paper and PaperClone nodes",
        leave=True,
        unit="edges",
    ):
        db.cypher_query(
            f"""
            MATCH (a:Paper {{nodeID: '{start_id}'}})
            MATCH (b:PaperClone {{nodeID: '{end_id}'}})
            CREATE (a)-[r:SIMILAR_TO_WITHIN_TOPIC]->(b)
            SET r.edgeWeight = {_weight['edgeWeight']}
        """
        )
        db.cypher_query(
            f"""
            MATCH (a:PaperClone {{nodeID: '{start_id}'}})
            MATCH (b:Paper {{nodeID: '{end_id}'}})
            CREATE (a)-[r:SIMILAR_TO_WITHIN_TOPIC]->(b)
            SET r.edgeWeight = {_weight['edgeWeight']}
        """
        )


def clear_neo4j_database() -> None:
    """
    Clear all nodes and relationships from the Neo4j database.
    """
    neo4j_connection()
    logger.info("Clearing all data from the Neo4j database...")
    db.cypher_query("MATCH (n) DETACH DELETE n")
    logger.info("Neo4j database cleared.")


def create_neo4j_backup(
    path_to_neo4j_admin_tool: str = "neo4j-admin",
    backup_path: str = "pipeline_data/neo4j_backups",
) -> None:
    """
    Create a backup of the Neo4j database.

    :param path_to_neo4j_admin_tool: Path to the Neo4j admin tool.
    :param backup_path: Directory path where the backup will be stored.
    """
    try:
        today = datetime.today().strftime("%Y-%m-%d")
        backup_path = f"{backup_path}/{today}"
        os.makedirs(backup_path, exist_ok=True)

        logger.info(f"Creating backup of the Neo4j database at {backup_path}...")

        result = subprocess.run(
            ["sudo", path_to_neo4j_admin_tool, "database", "dump", f"--to-path={backup_path}", "neo4j", "--verbose"],
            check=True,
            capture_output=True,
        )
        logger.info("Backup created successfully")
        logger.debug(result.stdout.decode())
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create backup: {e.stderr.decode()}")
        raise


def create_matching_paper_relationships() -> None:
    """
    Add MATCHING_PAPER relationships between papers in Neo4j.
    """
    logger.info("Adding MATCHING_PAPER relationships")
    # Add multi-presence connections using a Cypher query
    MATCHING_PAPER_QUERY = """
    MATCH (a)
    MATCH (b)
    WHERE a.itemID = b.itemID AND ID(a) <> ID(b)
    MERGE (a)<-[r:MATCHING_PAPER]->(b)
    """
    db.cypher_query(MATCHING_PAPER_QUERY)
