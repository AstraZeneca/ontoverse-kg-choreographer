import itertools as IT
import logging
import os
import pickle as pk
import sqlite3 as sq
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache

import networkx as nx
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from kgs_rnd_ontoverse.utils.models import BibliographicObject

logger = logging.getLogger(__name__)


def block_by_common_cui(annotations: dict) -> set[tuple[str, str]]:
    """
    Generate candidate pairs of papers sharing common CUIs.

    :param dict annotations: dictionary containing CUI annotations for papers.
    :return: A set of candidate pairs of paper IDs.
    """
    # cui_to_papers = defaultdict(list)
    # for paper_id, cuis in annotations.items():
    #     for cui in cuis:
    #         cui_to_papers[cui].append(paper_id)
    # candidate_pairs = {
    #     pair for papers in cui_to_papers.values() if len(papers) > 1 for pair in IT.combinations(papers, 2)
    # }
    # create every combination of pairs to compare
    pairs = IT.combinations(annotations.keys(), 2)
    return pairs


def build_bibliographic_objects(connection: sq.Connection, item_ids: list[int]) -> dict[int, BibliographicObject]:
    """
    Build bibliographic objects for a list of item IDs.

    :param connection: SQLite connection to the database.
    :param item_ids: List of item IDs to process.
    :return: dictionary of item IDs mapped to their BibliographicObject instances.
    """
    library = {}
    for item_id in item_ids:
        current_biblio = BibliographicObject("paper")
        item_details_df = cached_pull_zotero_item_details(connection, item_id)
        author_details_df = cached_pull_zotero_author_details(connection, item_id)
        current_biblio.add_attributes({"itemID": item_id})
        if "title" in item_details_df.index:
            current_biblio.add_attributes({"title": item_details_df.at["title", "value"]})
        authors = author_details_df["lastName"].tolist() if not author_details_df.empty else []
        current_biblio.add_attributes({"authors": authors})
        fields_list = [
            "date",
            "journalAbbreviation",
            "volume",
            "issue",
            "pages",
            "DOI",
            "url",
            "ISBN",
            "ISSN",
            "abstract",
            "publicationTitle",
            "abstractNote",
        ]
        for field in fields_list:
            if field in item_details_df.index:
                current_biblio.add_attributes({field: item_details_df.at[field, "value"]})
        library[item_id] = current_biblio
    return library


def build_item_level_topics(item_topics: list[int], level_nodes: dict[int, list[int]]) -> dict[int, list[int]]:
    """
    Group item topics by their levels.

    :param item_topics: List of topic IDs associated with an item.
    :param level_nodes: dictionary mapping levels to node IDs.
    :return: dictionary mapping levels to lists of topic IDs.
    """
    item_level_topics = defaultdict(list)
    for topic in item_topics:
        for level, nodes in level_nodes.items():
            if topic in nodes:
                item_level_topics[level].append(topic)
    return item_level_topics


@lru_cache(maxsize=128)
def cached_pull_zotero_author_details(con: sq.Connection, item_id: str) -> pd.DataFrame:
    """
    Fetch author details and cache the results.

    :param con: SQLite connection to the database.
    :param item_id: Item ID for which to fetch author details.
    :return: DataFrame containing author details.
    """
    query = (
        "SELECT creators.creatorID, orderIndex, firstName, lastName "
        "FROM itemCreators JOIN creators ON itemCreators.creatorID = creators.creatorID "
        "WHERE itemID = ? ORDER BY orderIndex ASC"
    )
    return execute_query(con, query, params=(item_id,))


@lru_cache(maxsize=128)
def cached_pull_zotero_item_details(con: sq.Connection, item_id: str) -> pd.DataFrame:
    """
    Fetch item details and cache the results.

    :param con: SQLite connection to the database.
    :param item_id: Item ID for which to fetch details.
    :return: DataFrame containing item details.
    """
    query = (
        "SELECT fieldName, value FROM itemData "
        "JOIN fields ON itemData.fieldID = fields.fieldID "
        "JOIN itemDataValues ON itemData.valueID = itemDataValues.valueID "
        "WHERE itemID = ?"
    )
    return execute_query(con, query, params=(item_id,))


def create_topics_digraph(connection: sq.Connection, topic_graph: nx.DiGraph, top_topics_list: list[int]) -> nx.DiGraph:
    """
    Recursively create the topic digraph.

    :param connection: SQLite connection to the database.
    :param topic_graph: NetworkX directed graph to be populated.
    :param top_topics_list: List of top topic IDs to process.
    :return: The populated directed graph.
    """
    logger.info("Creating the topic digraph...")
    for top_topic_id in tqdm(top_topics_list, desc="Processing Top Topics", leave=True):
        logger.debug(f"Pulling topics for collection ID: {top_topic_id}")
        topic_graph.add_edge("ROOT", top_topic_id)
        get_collection_from_cache_or_db(connection, top_topic_id, zotero_library=connection, topic_graph=topic_graph)
    return topic_graph


def compute_overlap(pair: tuple[str, str], annotations_df: pd.DataFrame) -> tuple[tuple[str, str], int]:
    """
    Compute the overlap between the sets of CUIs for two papers.

    :param pair: tuple containing two paper IDs.
    :param annotations_df: DataFrame with CUI annotations.
    :return: tuple of the pair and the overlap count.
    """
    a, b = pair
    return (pair, len(set(annotations_df[a]).intersection(set(annotations_df[b]))))


def create_level_one_nodes(item_id: int, count: int, topic_ids: list[int]) -> list[str]:
    """
    Create nodes for level 1.

    :param item_id: The ID of the item.
    :param count: Number of nodes to create.
    :param topic_ids: List of topic IDs associated with the item.
    :return: List of level 1 node IDs.
    """
    return [f"{item_id}_1_{i+1}" for i in range(count)]


def create_paper_clone_nodes(item_id: int, level: int, count: int) -> list[str]:
    """
    Create PaperClone nodes for levels other than level 1.

    :param item_id: The ID of the item.
    :param level: The level for which nodes are being created.
    :param count: Number of PaperClone nodes to create.
    :return: List of PaperClone node IDs.
    """
    return [f"{item_id}_{level}_{i+1}" for i in range(count)]


def establish_zotero_connection(path: str) -> sq.Connection:
    """
    Establish and return a SQLite connection to the Zotero database.

    :param path: Path to the Zotero SQLite database.
    :return: SQLite connection object.
    """
    con = sq.connect(path)
    logger.info("Connected to Zotero SQLite database.")
    return con


def execute_query(con: sq.Connection, query: str, params: tuple = (), **kwargs) -> pd.DataFrame:
    """
    Execute a SQL query and return a DataFrame.

    :param con: SQLite connection to the database.
    :param query: SQL query string.
    :param params: Optional tuple of parameters for the query.
    :return: DataFrame with query results.
    """
    try:
        logger.debug(f"Executing query: {query}")
        return pd.read_sql_query(con=con, sql=query, params=params, **kwargs)
    except Exception as e:
        logger.error(f"Error executing query: {query}")
        raise e


def explore_similarity_results(
    scoring_dics_pickle_path: str,
    score_cutoff: int = 8,
) -> None:
    """
    Evaluate and log statistics on similarity results based on a given score cutoff.

    :param score_cutoff: The minimum score for similarity evaluation.
    """
    with open(scoring_dics_pickle_path, "rb") as fh:
        scoring_results = pk.load(fh)
    score_counter = sum(1 for score in scoring_results.values() if score >= score_cutoff)
    unique_papers = {item for pair, score in scoring_results.items() if score >= score_cutoff for item in pair}
    logger.info(f"Number of high scoring pairs with score >= {score_cutoff}: {score_counter}")
    logger.info(f"Number of unique papers with score >= {score_cutoff}: {len(unique_papers)}")


def find_zotero_library(con: sq.Connection, library_name: str="OncoDSAILib") -> str:
    """
    Find and return the Zotero library ID.

    :param con: SQLite connection to the database.
    :return: Zotero library ID as a string.
    """
    logger.info("Finding the Zotero library ID.")

    query = (
        "SELECT libraries.libraryID FROM libraries, groups "
        f"WHERE libraries.libraryID = groups.libraryID AND groups.name = '{library_name}'"
    )
    query_df = execute_query(con, query)
    OncoDSAI_library_id = str(query_df["libraryID"][0])
    return OncoDSAI_library_id


def get_zotero_sqlite_path() -> str:
    """
    Load and return the Zotero SQLite path from environment variables.

    :return: Path to the Zotero SQLite database.
    """
    load_dotenv()
    path = os.getenv("ZOTERO_SQLITE_PATH")
    if not path:
        logger.error("ZOTERO_SQLITE_PATH not found in the environment")
        raise ValueError("ZOTERO_SQLITE_PATH environment variable is missing.")
    return path


def get_collection_from_cache_or_db(
    connection: sq.Connection,
    collection_id: int,
    zotero_cache: dict[int, list[int]],
    zotero_library: str,
    topic_graph: nx.DiGraph,
) -> dict[int, list[int]]:
    """
    Get collection data from cache or database.

    :param connection: SQLite connection to the database.
    :param collection_id: Collection ID to fetch.
    :param zotero_cache: Cache dictionary for collection data.
    :param zotero_library: Zotero library ID.
    :param topic_graph: Topic graph to add the collection.
    :return: Collection data from cache or fetched from database.
    """
    if collection_id not in zotero_cache:
        logger.debug(f"Collection id {collection_id} not present in cache")
        a = pull_zotero_topics_recursive_iter(
            connection=connection,
            library_id=zotero_library,
            topic_graph=topic_graph,
            collection_id=collection_id,
        )
        zotero_cache[collection_id] = a
    return zotero_cache[collection_id]


def lazy_load_bibliographic_objects(connection: sq.Connection, item_ids: list[int]) -> Iterable[BibliographicObject]:
    """
    Generator to yield bibliographic objects one at a time.

    :param connection: SQLite connection to the database.
    :param item_ids: List of item IDs to process.
    :return: Generator yielding BibliographicObject instances.
    """

    for item_id in item_ids:
        current_biblio = BibliographicObject("paper")
        item_details_df = pull_zotero_item_details(connection, item_id)
        author_details_df = pull_zotero_author_details(connection, item_id)
        current_biblio.add_attributes({"itemID": item_id})
        if "title" in item_details_df.index:
            current_biblio.add_attributes({"title": item_details_df.at["title", "value"]})
        authors = author_details_df["lastName"].tolist() if not author_details_df.empty else []
        current_biblio.add_attributes({"authors": authors})
        fields_list = [
            "date",
            "journalAbbreviation",
            "volume",
            "issue",
            "pages",
            "DOI",
            "url",
            "ISBN",
            "ISSN",
            "abstract",
            "publicationTitle",
            "abstractNote",
        ]
        for field in fields_list:
            if field in item_details_df.index:
                current_biblio.add_attributes({field: item_details_df.at[field, "value"]})
        yield current_biblio


def parallel_pairwise_comparisons(
    candidate_pairs: set[tuple[str, str]], annotations_df: pd.DataFrame, num_threads: int = 4
) -> dict[tuple[str, str], int]:
    """
    Perform parallel computation of pairwise CUI overlaps for given candidate pairs.

    :param candidate_pairs: A set of tuples where each tuple contains a pair of paper IDs.
    :param annotations_df: A DataFrame containing annotations for each paper.
    :param num_threads: The number of threads to use for parallel processing.
    :return: A dictionary mapping each pair of paper IDs to their overlap score.
    """
    results = {}
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_pair = {executor.submit(compute_overlap, pair, annotations_df): pair for pair in candidate_pairs}
        for future in tqdm(as_completed(future_to_pair), total=len(future_to_pair), desc="Comparing pairs"):
            pair = future_to_pair[future]
            try:
                pair, score = future.result()
                results[pair] = score
            except Exception as e:
                logger.error(f"Error in processing pair {pair}: {e}")
    return results


def pull_zotero_all_topics(con: sq.Connection, library_id: str) -> pd.DataFrame:
    """
    Fetch all topics (collections) from the Zotero library.

    :param con: SQLite connection object to the Zotero database.
    :param library_id: The ID of the Zotero library from which topics are being fetched.
    :return: A DataFrame containing all topics with columns such as 'collectionID' and 'collectionName'.
    """
    query = f"SELECT DISTINCT collectionID, collectionName " f"FROM collections WHERE libraryID = {library_id}"
    return execute_query(con, query, index_col="collectionID")


def pull_zotero_author_details(con: sq.Connection, item_id: str) -> pd.DataFrame:
    """
    Fetch author details for a specific item ID from the Zotero database.

    :param con: SQLite connection object to the Zotero database.
    :param item_id: The ID of the item for which author details are being fetched.
    :return: A DataFrame containing author details
    with columns such as 'creatorID', 'orderIndex', 'firstName', and 'lastName'.
    """
    query = (
        "SELECT creators.creatorID, orderIndex, firstName, lastName "
        "FROM itemCreators JOIN creators ON itemCreators.creatorID = creators.creatorID "
        "WHERE itemID = ? ORDER BY orderIndex ASC"
    )
    return execute_query(con, query, params=(item_id,), index_col="creatorID")


def pull_zotero_item_details(con: sq.Connection, item_id: str) -> pd.DataFrame:
    """
    Fetch item details for a specific item ID from the Zotero database.

    :param con: SQLite connection object to the Zotero database.
    :param item_id: The ID of the item for which details are being fetched.
    :return: A DataFrame containing item details with columns such as 'fieldName' and 'value'.
    """
    query = (
        "SELECT fieldName, value FROM itemData "
        "JOIN fields ON itemData.fieldID = fields.fieldID "
        "JOIN itemDataValues ON itemData.valueID = itemDataValues.valueID "
        "WHERE itemID = ?"
    )
    return execute_query(con, query, params=(item_id,))


def pull_item_type(con: sq.Connection, item_id: str) -> pd.DataFrame:
    """
    Fetch item details for a specific item ID from the Zotero database.

    :param con: SQLite connection object to the Zotero database.
    :param item_id: The ID of the item for which details are being fetched.
    :return: A DataFrame containing item details with columns such as 'fieldName' and 'value'.
    """
    query = (
        "SELECT i.itemID, it.typeName AS itemType "
        "FROM items i "
        "JOIN itemTypes it ON i.itemTypeID = it.itemTypeID "
        "WHERE i.itemID = ?"
    )
    return execute_query(con, query, params=(item_id,))


def pull_zotero_tags(con: sq.Connection, library_id: str) -> pd.DataFrame:
    """
    Fetch tags associated with items in the Zotero library.

    :param con: SQLite connection object to the Zotero database.
    :param library_id: The ID of the Zotero library from which tags are being fetched.
    :return: A DataFrame containing item IDs and their associated collection tags.
    """
    query = (
        f"SELECT DISTINCT itemID, GROUP_CONCAT(colItems.collectionID) AS collectionTags "
        f"FROM collectionItems AS colItems, collections AS colls "
        f"WHERE libraryID = {library_id} AND colItems.collectionID = colls.collectionID "
        f"GROUP BY itemID"
    )
    return execute_query(con, query, index_col="itemID")


def pull_zotero_top_topics(con: sq.Connection, library_id: str) -> pd.DataFrame:
    """
    Pull top topics (collections) from the Zotero library.

    :param con: SQLite connection object to the Zotero database.
    :param library_id: The ID of the Zotero library from which top topics are being fetched.
    :return: A DataFrame containing collection IDs and their names, indexed by `collectionID`.
    """
    query = (
        f"SELECT collectionId, collectionName FROM collections "
        f"WHERE libraryID = {library_id} AND parentCollectionId IS NULL"
    )
    return execute_query(con, query, index_col="collectionID")


def pull_zotero_topics_recursive_iter(
    connection: sq.Connection, library_id: str, topic_graph: nx.DiGraph, collection_id: int
) -> list[int]:
    """
    Recursively pull topics and build a directed graph of collections.

    :param connection: SQLite connection object.
    :param library_id: The ID of the Zotero library.
    :param topic_graph: The NetworkX directed graph to which topics and edges will be added.
    :param collection_id: The current collection ID to process.
    :return: List of processed collection IDs.
    """
    logger.debug(f"Pulling topics for collection ID: {collection_id}")

    stack = [collection_id]
    processed_collections = []

    while stack:
        current_id = stack.pop()
        processed_collections.append(current_id)

        query = (
            f"SELECT collectionId, collectionName FROM collections "
            f"WHERE libraryID = {library_id} AND parentCollectionId = {current_id}"
        )

        result = execute_query(connection, query, index_col="collectionID")
        children = list(result.index)
        logger.debug(f"Found {len(children)} child collections for collection ID: {current_id}")

        for child_id in children:
            if not topic_graph.has_edge(current_id, child_id):
                topic_graph.add_edge(current_id, child_id)
                stack.append(child_id)

    return processed_collections


def pull_zotero_unique_item_ids(con: sq.Connection, library_id: str) -> list[int]:
    """
    Retrieve unique item IDs from the Zotero library.

    :param con: SQLite connection to the database.
    :param library_id: Zotero library ID.
    :return: List of unique item IDs.
    """
    query = (
        f"SELECT DISTINCT itemID FROM collectionItems "
        f"WHERE collectionID IN (SELECT collectionID FROM collections WHERE libraryID = {library_id})"
    )
    return pd.read_sql_query(query, con)["itemID"].tolist()


def save_pickle(filename: str, obj: object, overwrite: bool = True) -> bool:
    """
    Save an object to a pickle file.

    :param filename: The filename for the pickle file (must end with .pk).
    :param obj: The object to be pickled.
    :param overwrite: Whether to overwrite the file if it exists.
    :return: True if the file was saved successfully, False otherwise.
    :raises ValueError: If the filename does not have a .pk extension.
    """
    if not filename.endswith(".pk"):
        logger.error("Invalid file extension. Must be .pk.")
        raise ValueError("Pickle file must have a .pk extension.")

    if os.path.exists(filename) and not overwrite:
        logger.info(f"{filename} already exists. Skipping save.")
        return False

    print(f"Saving object to {filename}")
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as f:
        pk.dump(obj, f)
    return True
