"""
This script will do the pairwise comparison of papers by CUI overlap:
  input - the cuiAnnnotationsCombined.pickle file created by the biomedical-NLP script (scispacyRun.ipynb)
  output - the soringdict.pickle file continaing pairwise scores cutOff at the user specified value
  
  FIXED VERSION: Properly uses similar_paper_cutoff parameter and adds diagnostics
"""

import itertools as IT
import logging
import pickle as pk

import networkx as nx
import pandas as pd
from tqdm import tqdm

from kgs_rnd_ontoverse.utils.kg_db import (
    Paper,
    PaperClone,
    create_between_topic_relationships,
    create_matching_paper_relationships,
    create_paper_clone_relationships,
    create_within_topic_relationships,
)
from kgs_rnd_ontoverse.utils.zotero import (
    save_pickle,
)

logger = logging.getLogger(__name__)


class PaperSimilarityPipeline:
    """
    This class calculates the paper similarity as the number of shared
    Concept Unique Identifiers (CUI) terms between papers.
    It does a pairwise comparison of papers by CUI overlap:
    The required input is the cuiAnnotationsCombined.pickle file
    created by OntoverseNERPipeline class in the paperNER.py script.
    """

    def __init__(self, similar_paper_cutoff: int, pipeline_data_path: str, overwrite: bool = False):
        self.similar_paper_cutoff = similar_paper_cutoff
        self.pipeline_data_path = pipeline_data_path
        self.overwrite = True
        self.combined_annotations_path = f"{self.pipeline_data_path}/cuiAnnotationsCombined.pk"
        self.combined_annotations_dict: dict = None

        self.scoring_results: dict = None
        self.scoring_results_dict_pickle_path = f"{self.pipeline_data_path}/scoringdict.pk"

        self.topic_occupancy_graph = None
        self.topic_occupancy_graph_nodes = None

        self.within_edges_file_path = f"{self.pipeline_data_path}/withinEdges.pk"
        self.between_edges_file_path = f"{self.pipeline_data_path}/betweenEdges.pk"
        self.scoring_results: dict = None
        self.high_scoring_edges: dict = None
        self.similar_papers_dict: dict = None

    def run(self, topic_occupancy_graph: nx.DiGraph):

        self.topic_occupancy_graph = topic_occupancy_graph
        logger.info("Performing paper similarity with optimized pairwise comparison.")
        logger.info(f"Using similarity cutoff threshold: {self.similar_paper_cutoff}")

        # Step 1: Load combined annotations
        # these paper-concept annotations have been combined in the NER pipeline stage
        # Check if combined annotations are already loaded
        self.combined_annotations_dict = self.load_combined_annotations()

        # Step 2: Create scoring results
        self.scoring_results = self.create_scoring_results_dict()
        save_pickle(self.scoring_results_dict_pickle_path, self.scoring_results)
        
        # ADDED: Analyze score distribution BEFORE filtering
        self.analyze_score_distribution()

        # Step 3: Find high scoring edges - FIXED: Now passes self.similar_paper_cutoff
        self.high_scoring_edges: dict = self.find_high_scoring_edges(cutoff=self.similar_paper_cutoff)

        self.topic_occupancy_graph_nodes = nx.nodes(self.topic_occupancy_graph)
        logger.info(f"Number of nodes in topic occupancy graph: {len(self.topic_occupancy_graph_nodes)}")

        # Step 5: Create within and between topic edges using high scoring edges
        if not self.overwrite:
            logger.info("Loading within and between edges from file instead of recomputing")
            with open(self.within_edges_file_path, "rb") as fh:
                within_edges_list = pk.load(fh)
            with open(self.between_edges_file_path, "rb") as fh:
                between_edges_list = pk.load(fh)
        else:
            # Careful! Very very slow 12:42'
            within_edges_list, between_edges_list = self.create_topic_edges()
            self.save_edge_files(within_edges_list, between_edges_list)

        # Step 6: Populate edges to Neo4j
        self.populate_edges_to_neo4j(within_edges_list, between_edges_list)

        # Step 7: Compute similar papers dictionary
        self.similar_papers_dict: dict = self.compute_similar_papers_dict(self.similar_paper_cutoff)

        # Step 8: Update Neo4j nodes with similar papers
        self.update_neo4j_nodes()

        # Step 9: Create matching relationships between Paper and PaperClone nodes
        create_matching_paper_relationships()

    def load_combined_annotations(self) -> dict:
        logger.info(f"Loading the dictionary containing paper annotations from {self.combined_annotations_path}")
        combined_annotations = pd.read_pickle(self.combined_annotations_path)
        logger.info(f"Loaded annotations for {len(combined_annotations)} papers")
        return combined_annotations

    def create_scoring_results_dict(self) -> dict:
        logger.info("Creating scoring results")
        pairs = IT.combinations(self.combined_annotations_dict.keys(), 2)
        # lamda to do the set comparison and return length of overlap
        nt = lambda a, b: len(  # noqa: E731
            set(self.combined_annotations_dict[a]).intersection(set(self.combined_annotations_dict[b]))
        )
        # generate a dict of scores for paper similarity with itemID pair as a tuple
        scoring_results = dict([(t, nt(*t)) for t in pairs])
        logger.info(f"Number of scored pairs: {len(scoring_results)}")
        return scoring_results

    def analyze_score_distribution(self) -> None:
        """
        NEW METHOD: Analyze the distribution of similarity scores to diagnose issues
        """
        if not self.scoring_results:
            logger.warning("No scoring results to analyze")
            return
        
        scores = list(self.scoring_results.values())
        if not scores:
            logger.warning("Empty scoring results")
            return
        
        import numpy as np
        
        logger.info("=" * 70)
        logger.info("SIMILARITY SCORE DISTRIBUTION ANALYSIS:")
        logger.info("=" * 70)
        logger.info(f"  Total paper pairs analyzed: {len(scores)}")
        logger.info(f"  Min score: {min(scores)}")
        logger.info(f"  Max score: {max(scores)}")
        logger.info(f"  Mean score: {np.mean(scores):.2f}")
        logger.info(f"  Median score: {np.median(scores):.2f}")
        logger.info(f"  25th percentile: {np.percentile(scores, 25):.2f}")
        logger.info(f"  75th percentile: {np.percentile(scores, 75):.2f}")
        logger.info(f"  90th percentile: {np.percentile(scores, 90):.2f}")
        logger.info(f"  95th percentile: {np.percentile(scores, 95):.2f}")
        logger.info("")
        logger.info("  Pairs by score threshold:")
        
        for threshold in [3, 4, 5, 6, 7, 8, 9, 10]:
            count = sum(1 for s in scores if s >= threshold)
            pct = count / len(scores) * 100 if len(scores) > 0 else 0
            status = " ← CURRENT THRESHOLD" if threshold == self.similar_paper_cutoff else ""
            logger.info(f"    Score >= {threshold:2d}: {count:5d} pairs ({pct:5.2f}%){status}")
        
        logger.info("=" * 70)
        
        # Warning if no high-scoring pairs
        high_score_count = sum(1 for s in scores if s >= self.similar_paper_cutoff)
        if high_score_count == 0:
            logger.warning("")
            logger.warning("⚠" * 35)
            logger.warning(f"  WARNING: NO PAIRS FOUND WITH SCORE >= {self.similar_paper_cutoff}")
            logger.warning("  This will result in ZERO similarity relationships!")
            logger.warning("  ")
            logger.warning("  Possible causes:")
            logger.warning("    1. Papers are too diverse (few shared concepts)")
            logger.warning("    2. Abstracts are missing or poor quality")
            logger.warning("    3. NER extraction found few concepts")
            logger.warning("    4. Threshold is too high for this dataset")
            logger.warning("  ")
            logger.warning("  Recommendations:")
            # Find the threshold that would give at least some edges
            for test_threshold in range(self.similar_paper_cutoff - 1, 0, -1):
                test_count = sum(1 for s in scores if s >= test_threshold)
                if test_count > 0:
                    logger.warning(f"    - Lower threshold to {test_threshold} (would give {test_count} pairs)")
                    break
            logger.warning("    - Check paper abstract quality")
            logger.warning("    - Review NER annotation results")
            logger.warning("⚠" * 35)
            logger.warning("")

    def find_high_scoring_edges(self, cutoff: int = None) -> dict:
        """
        FIXED: Now properly accepts cutoff parameter with None default
        If cutoff is not provided, uses self.similar_paper_cutoff
        """
        if cutoff is None:
            cutoff = self.similar_paper_cutoff
            
        logger.info(f"Finding high scoring edges (score >= {cutoff})")
        high_scoring_edges = dict()
        for i in self.scoring_results:
            if self.scoring_results[i] >= cutoff:
                high_scoring_edges[i] = self.scoring_results[i]
            else:
                pass
        logger.info(f"{len(high_scoring_edges)} high scoring edges found")
        
        # Additional diagnostic
        if len(high_scoring_edges) == 0:
            logger.error(f"⚠️  CRITICAL: Zero edges found with cutoff >= {cutoff}")
            logger.error("   This will cause similarity calculation to fail!")
        
        return high_scoring_edges

    def create_topic_edges(self) -> list[list, list]:
        """
        This method defines the edges based on the scoring criteria
        This is a slow calculation and could be sped up considerably
        with dict comprehensions and bulk import features
        Now it takes 12:42' to run
        """
        logger.info("Creating within and between topic edges")
        # find all of the nodeIDs that match the edge start and end itemIDs
        # lists to hold all of the within and between edges to be created
        within_edges, between_edges = [], []

        for edge in tqdm(self.high_scoring_edges, desc="Creating within and between topic edges", unit="edges"):
            paper1, paper2 = edge
            node_list_1, node_list_2 = self.find_edge_node_ids(paper1, paper2)
            current_edge_Weight = self.high_scoring_edges[edge]

            currentWithinEdges, currentBetweenEdges = self.create_edges(node_list_1, node_list_2, current_edge_Weight)
            for current_edge in currentWithinEdges:
                within_edges.append(current_edge)
            for current_edge in currentBetweenEdges:
                between_edges.append(current_edge)
        return within_edges, between_edges

    def find_edge_node_ids(self, paper1: str, paper2: str) -> tuple[list[str], list[str]]:
        """
        Finds and returns lists of node IDs matching the given item IDs within the provided graph nodes.

        :param itemID1: The first paper ID to search for in the nodes.
        :param itemID2: The second paper ID to search for in the nodes.
        :return: A tuple containing two lists of node IDs that match itemID1 and itemID2 respectively.
        """
        node_list1 = []
        node_list2 = []

        for node in self.topic_occupancy_graph_nodes:
            try:
                listitemID, level, clnum = node.split("_", 3)
                if listitemID == paper1:
                    node_list1.append(node)
                elif listitemID == paper2:
                    node_list2.append(node)
            except ValueError:
                pass

        return node_list1, node_list2

    def create_edges(self, node_list1: list, node_list2: list, edge_weight):
        # cartesian product
        list = IT.product(node_list1, node_list2)

        current_within_edges = []
        current_between_edges = []
        visibility_level_topic_node_dict = nx.get_node_attributes(self.topic_occupancy_graph, "visibilityLevelTopic")
        for node_pairs in list:
            node1, node2 = node_pairs
            item_id1, item_id1_level, clnum = node1.split("_", 3)
            item_id2, item_id2_level, clnum = node2.split("_", 3)
            # only allow edges in the same level
            if item_id1_level == item_id2_level:
                # filter these to only keep the highScoringEdges
                if (item_id1, item_id2) in self.high_scoring_edges:
                    # check the topics of the nodes
                    item_id1_topic = visibility_level_topic_node_dict[node1]
                    item_id2_topic = visibility_level_topic_node_dict[node2]
                    # print(itemID1,itemID1Topic,itemID2,itemID2Topic,nodePairs)
                    if item_id1_topic == item_id2_topic:
                        # create a within topic edge
                        current_within_edges.append((node1, {"edgeWeight": edge_weight}, node2))
                        # print("within topic - ",itemID1,itemID1Topic,itemID2,itemID2Topic,nodePairs,edgeWeight)
                    else:
                        # create a between topic edge
                        current_between_edges.append((node1, {"edgeWeight": edge_weight}, node2))
                        # print("between topic - ",itemID1,itemID1Topic,itemID2,itemID2Topic,nodePairs,edgeWeight)
                else:
                    pass
            else:
                pass
        return (current_within_edges, current_between_edges)

    def save_edge_files(self, within_edges, between_edges) -> None:
        save_pickle(self.within_edges_file_path, within_edges)
        save_pickle(self.between_edges_file_path, between_edges)
        logger.info(f"There are {len(within_edges)} within topic edges.")
        logger.info(f"There are {len(between_edges)} between topic edges.")

    def populate_edges_to_neo4j(self, within_edges_list: list, between_edges_list: list):
        # There are two iterations in this function:
        # The first one takes 1:05' whilst the second one takes 9:27:02!!!!! to run
        create_paper_clone_relationships(within_edges_list, between_edges_list)

        # This currently takes ? to run
        create_between_topic_relationships(between_edges_list)

        # This currently takes ? to run
        create_within_topic_relationships(within_edges_list)

    def compute_similar_papers_dict(self, similar_paper_cutoff) -> dict:
        similar_papers_dict = {}
        for (itemID1, itemID2), score in tqdm(
            self.scoring_results.items(), leave=True, desc="Computing similar papers", unit="pairs"
        ):
            if score >= similar_paper_cutoff:
                similar_papers_dict.setdefault(itemID1, set()).add(itemID2)
                similar_papers_dict.setdefault(itemID2, set()).add(itemID1)

        return {k: list(v) for k, v in similar_papers_dict.items()}

    def update_neo4j_nodes(self):
        # Update the nodes using neomodel queries
        for itemID, similar_ids in tqdm(
            self.similar_papers_dict.items(), leave=True, desc="Updating similar papers", unit="papers"
        ):
            similar_ids_str = list(map(str, similar_ids))  # Convert IDs to strings for storage

            # Query and update Paper nodes
            paper_nodes = Paper.nodes.filter(itemID=int(itemID))
            for paper_node in paper_nodes:
                paper_node.similarPapers = similar_ids_str
                paper_node.save()

            # Query and update PaperClone nodes
            paper_clone_nodes = PaperClone.nodes.filter(itemID=int(itemID))
            for paper_clone_node in paper_clone_nodes:
                paper_clone_node.similarPapers = similar_ids_str
                paper_clone_node.save()

# there are  54805 potential edges with score >= 6 [ 3754 papers]
# there are  33298 potential edges with score >= 7 [ 3362 papers]
# there are  20220 potential edges with score >= 8 [ 2947 papers]

# will add edges for papers where papers share 6 or more terms
# that equates to 3754 papers (3754/6835 - 54.6% of papers)
# that would be 54,805 edges - giving an average degree of 8.1
