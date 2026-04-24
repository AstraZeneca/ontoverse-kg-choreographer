import logging
import os
from pathlib import Path
import pickle
import time
import os
import argparse

import networkx as nx
from tqdm import tqdm
import yaml

from kgs_rnd_ontoverse.paperNER import OntoverseNERPipeline
from kgs_rnd_ontoverse.paperSimilarity import PaperSimilarityPipeline
from kgs_rnd_ontoverse.utils.kg_db import (
    clear_neo4j_database,
    create_collection_nodes,
    create_member_of_relationship,
    create_paper_nodes,
    create_parent_of_relationship,
    neo4j_connection,
    nodes_update_graphlevel,
    purge_neo4j_if_configured,
)
from kgs_rnd_ontoverse.utils.models import BibliographicObject
from kgs_rnd_ontoverse.utils.networkx import (
    build_item_collection_dict,
    build_tags_dict,
    build_topic_occupancy_graph,
    compute_paper_and_clones_stats,
    count_occupancy_THG,
    fetch_tree_levels,
)
from kgs_rnd_ontoverse.utils.zotero import (
    establish_zotero_connection,
    explore_similarity_results,
    find_zotero_library,
    get_collection_from_cache_or_db,
    get_zotero_sqlite_path,
    pull_item_type,
    pull_zotero_all_topics,
    pull_zotero_author_details,
    pull_zotero_item_details,
    pull_zotero_tags,
    pull_zotero_top_topics,
    pull_zotero_unique_item_ids,
    save_pickle,
)

logger = logging.getLogger(__name__)


class OntoversePipeline:
    """Pipeline for processing Ontoverse data and populating Neo4j database."""

    def __init__(
        self,
        zotero_sqlite_path: str,
        pipeline_data_path: str, #"./pipeline_data/pipeline_artifacts/20250113/"
        similar_paper_cutoff: int,
        zotero_library_name: str = None,
        overwrite: bool = True,
    ):
        """Initialize OntoversePipeline with SQLite path and pipeline instances."""
        self.zotero_sqlite_path: str = zotero_sqlite_path
        self.similar_paper_cutoff = similar_paper_cutoff
        self.pipeline_data_path = pipeline_data_path
        self.overwrite: bool = overwrite
        self.zotero_library: Optional[int] = None
        self.zotero_library_name: Optional[str] = zotero_library_name
        self.zotero_cache: dict = {}
        self.oncodsai_library_dict: dict | None = None
        self.item_collection_dict: dict | None = None
        self.topic_graph: nx.DiGraph | None = None
        self.topic_occupancy_graph: nx.Graph | None = None

    def run(self) -> None:
        """Execute the entire pipeline."""
        logger.info("Starting Ontoverse Pipeline")
        try:
            self.initialize_and_load()
            self.build_topic_hierarchy_graph()
            self.build_topic_occupancy_graph_method()
            self.populate_to_neo4j()
            self.run_ner_pipeline()
            self.run_paper_similarity_pipeline()
            logger.info("Ontoverse Pipeline completed successfully")
        except Exception as e:
            logger.error(f"Pipeline failed due to: {e}")
            try:
                logger.info(
                    "Clearing the knowledge graph in Neo4j after pipeline failure (error cleanup)."
                )
                clear_neo4j_database()
            except Exception as e:
                logger.error(f"Failed to clear Neo4j database during error cleanup: {e}")
            raise
        finally:
            logger.info("Pipeline run finished")

    def initialize_and_load(self) -> None:
        """Initialize and load bibliographic objects from a zotero sqlite.
        The library is loaded in a python dictionary datatype, that will be used in the
        named entity recognition pipeline and the paper similarity pipeline."""
        with establish_zotero_connection(self.zotero_sqlite_path) as connection:
            self.zotero_library = find_zotero_library(connection, self.zotero_library_name)
            zotero_item_ids_list = pull_zotero_unique_item_ids(con=connection, library_id=self.zotero_library)
            self.oncodsai_library_dict = {}
            start = time.time()
            # fields_list = [
            #         "date",
            #         "journalAbbreviation",
            #         "volume",
            #         "issue",
            #         "pages",
            #         "DOI",
            #         "url",
            #         "ISBN",
            #         "ISSN",
            #         "abstract",
            #         "publicationTitle",
            #         "abstractNote",
            #     ]
            # for biblio_obj in lazy_load_bibliographic_objects(connection, zotero_item_ids_list):
            for item_id in zotero_item_ids_list:
                current_biblio = BibliographicObject("paper")
                current_biblio.add_attributes({"itemID": item_id})
                
                item_type_df = pull_item_type(connection, item_id)
                item_type = item_type_df.at[0, "itemType"]
                current_biblio.add_attributes({"itemType": item_type})
                logger.debug(f"Item ID: {item_id} - Item Type: {item_type}")      
                  
                author_details_df = pull_zotero_author_details(connection, item_id)
                for _, row in author_details_df.iterrows():
                    logger.debug(f'{row["firstName"]}, {row["firstName"][0] if row[f"firstName"] else None}.')

                authors = (
                    [f'{row["lastName"]}, {row["firstName"][0] if row[f"firstName"] else None}.' for _, row in author_details_df.iterrows()]
                    if not author_details_df.empty 
                    else ""
                )

                if len(authors) == 1:
                    final_authors = authors[0]
                if len(authors) == 2:
                    final_authors = f"{authors[0]} and {authors[1]}"
                if len(authors) > 2:
                    final_authors = f"{authors[0]} et al."
                if authors == "":
                    final_authors = ""

                current_biblio.add_attributes({"authors": final_authors})

                all_authors = (
                    [f"{row['lastName']} {row['firstName']}" for _, row in author_details_df.iterrows()]
                    if not author_details_df.empty 
                    else []
                )
                current_biblio.add_attributes({"all_authors": all_authors})

                item_details_df = pull_zotero_item_details(connection, item_id)
                item_details_df = item_details_df.set_index("fieldName")
                item_fields = item_details_df.index.to_list()
                
                if "date" in item_fields:
                    date = item_details_df.at["date", "value"]
                    logger.debug(f"Date: {date}")
                    current_biblio.add_attributes({"date": date})


                if "title" in item_fields:
                    item_title = item_details_df.at["title", "value"]
                else:
                    item_title = f"Paper from {final_authors if final_authors else 'Unknown'}"
                current_biblio.add_attributes({"title": item_title})
                
                if "date" in item_fields:
                    date = item_details_df.at["date", "value"]
                    logger.debug(f"Date: {date}")
                else:
                    date = ""
                current_biblio.add_attributes({"date": date})

                if "abstractNote" in item_fields:
                    abstract = item_details_df.at["abstractNote", "value"]
                elif "abstract" in item_fields:
                    abstract = item_details_df.at["abstract", "value"]
                else:
                    abstract = item_title
                current_biblio.add_attributes({"abstract": abstract})
                
                if "url" in item_fields:
                    url = item_details_df.at["url", "value"]
                elif "DOI" in item_fields:
                    url = item_details_df.at["DOI", "value"]
                else:
                    url = ""
                current_biblio.add_attributes({"url": url})
                
                # Attributes that depend on the item type
                if item_type == "conferencePaper":
                    if "conferenceName" in item_fields:
                        conference_name = item_details_df.at["conferenceName", "value"]
                        current_biblio.add_attributes({"conferenceName": conference_name})
                    elif "proceedingsTitle" in item_fields:
                        conference_name = item_details_df.at["proceedingsTitle", "value"]
                        current_biblio.add_attributes({"conferenceName": conference_name})
                    else:
                        conference_name = "No conference"
                    current_biblio.add_attributes({"conferenceName": conference_name})
                    current_biblio.add_attributes({"journalAbbreviation": conference_name})

                if item_type == "preprint":
                    if "repository" in item_fields:
                        repository = item_details_df.at["repository", "value"]
                    current_biblio.add_attributes({"repository": repository})
                    current_biblio.add_attributes({"journalAbbreviation": repository})

                    if "institution" in item_fields:
                        institution = item_details_df.at["institution", "value"]
                    else:
                        institution = "No institution"
                    current_biblio.add_attributes({"institution": institution})

                if "journalAbbreviation" in item_fields:
                    journal = item_details_df.at["journalAbbreviation", "value"]
                elif "publicationTitle" in item_fields:
                    journal = item_details_df.at["publicationTitle", "value"]
                else:
                    journal = "No journal"
                current_biblio.add_attributes({"journal": journal})

                self.oncodsai_library_dict[item_id] = current_biblio
            end = time.time()

            logger.info(f"{len(self.oncodsai_library_dict)} items in library - {round(end - start, 1)} s to parse")
            save_pickle(f"{self.pipeline_data_path}/ontoverse_library.pk", self.oncodsai_library_dict, overwrite=True)

    def build_topic_hierarchy_graph(self) -> None:
        """Build the topic graph from the Zotero data."""
        self.topic_graph = nx.DiGraph(name="topic_graph")
        self.topic_graph.add_node("ROOT")
        with establish_zotero_connection(self.zotero_sqlite_path) as connection:
            zotero_top_topics_df = pull_zotero_top_topics(con=connection, library_id=self.zotero_library)
            zotero_top_topics_ids = list(zotero_top_topics_df.index)
            logger.info(
                f"There are {len(zotero_top_topics_ids)} Top topics in the Zotero library: " f"{zotero_top_topics_ids}"
            )
            logger.info(" Building the Topics Hierarchy Graph (THG).")
            for top_topic_id in tqdm(zotero_top_topics_ids, desc="Processing Top Topics", leave=True):
                logger.debug(f"Pulling sub topics for top topic (collection ID): {top_topic_id}")
                self.topic_graph.add_edge("ROOT", top_topic_id)
                get_collection_from_cache_or_db(
                    connection=connection,
                    collection_id=top_topic_id,
                    zotero_cache=self.zotero_cache,
                    zotero_library=self.zotero_library,
                    topic_graph=self.topic_graph,
                )
            logger.info(
                f"Topic graph created with {self.topic_graph.number_of_nodes()} "
                f"nodes and {self.topic_graph.number_of_edges()} edges"
            )

            logger.debug("Adding topic names to the graph...")
            all_topics_df = pull_zotero_all_topics(con=connection, library_id=self.zotero_library)
            topic_name_mapping_dict = all_topics_df.to_dict()["collectionName"]
            nx.set_node_attributes(self.topic_graph, topic_name_mapping_dict, "topicName")

            topiclists = {
                topTopicID: nx.bfs_tree(self.topic_graph, topTopicID).nodes() for topTopicID in zotero_top_topics_ids
            }
            for topTopicID, nodes in topiclists.items():
                logger.debug(f"Topic {topTopicID} (n={len(nodes)}) topic-list: {nodes}")

            logger.debug("Building item collection dictionary...")
            zotero_tags_df = pull_zotero_tags(con=connection, library_id=self.zotero_library)
            item_collection_tags_dict = build_tags_dict(zotero_tags_df)
            self.item_collection_dict = build_item_collection_dict(item_collection_tags_dict, self.topic_graph)

    def build_topic_occupancy_graph_method(self, path_to_pickle: Path=None) -> None:
        """Build the Topic Hierarchy Graph (THG) and Topic Occupancy Graph (TOG)."""
        topic_graph_level_nodes = fetch_tree_levels(self.topic_graph)
        multi_occupancy_counts = count_occupancy_THG(topic_graph_level_nodes, self.item_collection_dict)
        if path_to_pickle:
            path_to_pickle = Path(path_to_pickle)
            if path_to_pickle.exists() and not self.overwrite:
                with open(path_to_pickle, 'rb') as tog:
                    self.topic_occupancy_graph = pickle.load(tog)
        else:
            self.topic_occupancy_graph = build_topic_occupancy_graph(
                multi_occupancy_counts,
                self.item_collection_dict,
                topic_graph_level_nodes,
            )
            compute_paper_and_clones_stats(self.item_collection_dict, self.topic_occupancy_graph)

    def populate_to_neo4j(self) -> None:
        """Populate Neo4j with nodes and relationships."""
        logger.info("Populating Neo4j with nodes and relationships...")
        neo4j_connection()

        try:
            create_paper_nodes(
                self.topic_occupancy_graph,
                self.item_collection_dict,
                self.oncodsai_library_dict,
            )
            create_collection_nodes(
                self.topic_graph,
            )
            # create_paper_indexes()    # Not needed, they are created in the create_paper_nodes function
            nodes_update_graphlevel(self.topic_graph)
            logger.info("Finished updating collection graph levels.")
            create_parent_of_relationship(self.topic_graph)
            logger.info("Finished creating parent-of relationships.")
            create_member_of_relationship()
            logger.info("Finished creating member-of relationships.")
        except Exception as e:
            logger.error(f"Error during dependent steps: {e}")
            raise

        logger.info("Finished populating Neo4j.")

    def run_ner_pipeline(self) -> None:
        """Run the Named Entity Recognition pipeline."""
        logger.info(
            f"Running Named Entity Recognition pipeline. Using inherited data path -> {self.pipeline_data_path}"
        )
        self.ner_pipeline = OntoverseNERPipeline(pipeline_data_path=self.pipeline_data_path, overwrite=self.overwrite)
        self.ner_pipeline.run()

    def run_paper_similarity_pipeline(self, neo4j_graph=None) -> None:
        """Run the paper similarity pipeline."""
        if not neo4j_graph:
            neo4j_graph = neo4j_connection()
        logger.info("Running Paper Similarity pipeline...")
        self.paper_similarity_pipeline = PaperSimilarityPipeline(
            similar_paper_cutoff=self.similar_paper_cutoff,
            pipeline_data_path=self.pipeline_data_path,
            overwrite=self.overwrite,
        )
        self.paper_similarity_pipeline.run(self.topic_occupancy_graph)
        logger.info("Exploring similarity results")
        explore_similarity_results(
            scoring_dics_pickle_path=self.paper_similarity_pipeline.scoring_results_dict_pickle_path
        )


if __name__ == "__main__":
    from setup_logger import setup_logging

    logger = setup_logging()
    logger.info("Starting Ontoverse Pipeline")

    # Initialize parser
    parser = argparse.ArgumentParser(description = "Code to push an Ontoverse 'verse' into neo4j, an " \
    "argument pointing to a config is required")

    # Adding optional argument
    parser.add_argument("-c", "--config_path", help = "Path to configuration file")

    args = parser.parse_args()

    with open(args.config_path, 'r') as f:
        params = yaml.safe_load(f)

        if not isinstance(params, dict):
            raise ValueError("Config file must contain a YAML mapping at the top level.")

        if "NEO4J_DB" in params:
            raise ValueError(
                "NEO4J_DB must be set in the environment (.env or export), not in the YAML config. "
                "Remove NEO4J_DB from the YAML file."
            )

        if "NEO4J_PURGE" in params:
            if params["NEO4J_PURGE"] not in ["True", "False"]:    
                raise ValueError("NEO4J_PURGE in params.yml must be a string value ('True' or 'False').")
            os.environ["NEO4J_PURGE"] = params["NEO4J_PURGE"]
        else:
            os.environ["NEO4J_PURGE"] = "True"

    # Check necessary environment variables are set
    required_env_vars = ["NEO4J_PASSWORD", "NEO4J_DB", "ZOTERO_SQLITE_PATH"]

    for var in required_env_vars:
        if var not in os.environ:
            raise EnvironmentError(f"Environment variable {var} is not set. Please set it before running the script.")
        if os.getenv(var) == "":
            raise EnvironmentError(f"Environment variable {var} should not be empty.")

    purge_neo4j_if_configured()

    zotero_sqlite_path = get_zotero_sqlite_path()
    logger.info(f"Zotero SQLite path: {zotero_sqlite_path}")
    ontoverse_pipeline = OntoversePipeline(
        zotero_sqlite_path,
        similar_paper_cutoff=params["similar_paper_cutoff"],
        zotero_library_name=params["zotero_library_name"],
        pipeline_data_path=params["pipeline_artifact_location"],
        overwrite=False,
    )
    ontoverse_pipeline.run()
