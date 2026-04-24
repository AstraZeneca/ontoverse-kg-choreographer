import itertools as IT
import logging
from collections import defaultdict
import os

import networkx as nx
import pandas as pd
from tqdm import tqdm
import pickle

logger = logging.getLogger(__name__)


def add_edges_for_clones(toGraph: nx.Graph, levelNodes: list[str], topicIDs: list[int], level: int) -> nx.Graph:
    """
    Add edges for PaperClone nodes in levels other than level 1.

    :param toGraph: The graph to which edges will be added.
    :param levelNodes: list of node IDs for this level.
    :param topicIDs: list of topic IDs associated with this level.
    :param level: The level number.
    :return: Updated graph with edges added.
    """
    if len(levelNodes) == 1:
        toGraph.add_node(
            levelNodes[0],
            type="PaperClone",
            visibilityLevel=level,
            visibilityLevelTopic=str(topicIDs.pop()),
        )
    else:
        nodeConnections = list(IT.combinations(levelNodes, 2))
        tempGraph = nx.Graph()
        tempGraph.add_edges_from(nodeConnections, type="CLONE_CONNECTIONS")
        nodeAttributeMapping = {
            nodeId: {"type": "PaperClone", "visibilityLevel": level, "visibilityLevelTopic": str(topicIDs.pop())}
            for nodeId in levelNodes
        }
        nx.set_node_attributes(tempGraph, nodeAttributeMapping)
        toGraph = nx.compose(toGraph, tempGraph)
    return toGraph


def add_edges_for_level_one(to_graph: nx.Graph, level_nodes: list[str], topic_ids: list[int]) -> nx.Graph:
    """
    Add edges for level 1 nodes.

    :param to_graph: Graph to which the edges will be added.
    :param level_nodes: list of node IDs for level 1.
    :param topic_ids: list of topic IDs for level 1.
    :return: Updated graph with edges added.
    """
    if len(level_nodes) == 1:
        to_graph.add_node(level_nodes[0], type="Paper", visibilityLevel=1, visibilityLevelTopic=str(topic_ids.pop()))
    else:
        node_connections = list(IT.combinations(level_nodes, 2))
        temp_graph = nx.Graph()
        temp_graph.add_edges_from(node_connections, type="CLONE_CONNECTIONS")
        root_node = level_nodes.pop()
        nx.set_node_attributes(
            temp_graph,
            {root_node: {"type": "Paper", "visibilityLevel": 1, "visibilityLevelTopic": str(topic_ids.pop())}},
        )
        for node in level_nodes:
            nx.set_node_attributes(
                temp_graph,
                {node: {"type": "PaperClone", "visibilityLevel": 1, "visibilityLevelTopic": str(topic_ids.pop())}},
            )
        to_graph = nx.compose(to_graph, temp_graph)
    return to_graph


def build_item_collection_dict(
    item_collection_tags_dict: dict[int, list[int]], topic_graph: nx.DiGraph
) -> dict[int, list[int]]:
    """Build a dictionary mapping items to collections, with transitive closure for topics."""
    item_collection_dict: dict[int, list[int]] = {}
    bfs_cache = {}  # Cache BFS results to avoid redundant graph traversal

    for item_id, explicit_collection_annotation in item_collection_tags_dict.items():
        paperTopics = []
        for collection_id in explicit_collection_annotation:
            if collection_id not in bfs_cache:
                bfs_cache[collection_id] = list(nx.bfs_tree(topic_graph, collection_id, reverse=True).nodes())
            paperTopics.extend(bfs_cache[collection_id])
        item_collection_dict[item_id] = list(set(paperTopics))  # Make unique topics

    return item_collection_dict


def build_tags_dict(zotero_tags_df: pd.DataFrame) -> dict[int, list[int]]:
    """
    Build a dictionary mapping item IDs to their associated collection tags.

    :param zotero_tags_df: DataFrame containing Zotero tags.
    :return: dictionary mapping item IDs to lists of collection tags.
    """
    zotero_tags_df["collectionTags"] = zotero_tags_df["collectionTags"].str.split(",")
    zotero_tags_df = zotero_tags_df.explode("collectionTags")
    zotero_tags_df["collectionTags"] = zotero_tags_df["collectionTags"].astype(int)
    return zotero_tags_df.groupby("itemID")["collectionTags"].apply(list).to_dict()


def build_topic_occupancy_graph(
    multi_occupancy_counts: dict[int, dict[int, int]],
    item_collection_dict: dict[int, list[int]],
    topic_graph_level_nodes: dict[int, list[int]],
) -> nx.Graph:
    """
    Build the Topic Occupancy Graph (TOG) using occupancy counts and item-collection mappings.

    :param multi_occupancy_counts: dictionary with item IDs and occupancy counts.
    :param item_collection_dict: dictionary mapping items to associated topics.
    :param topic_graph_level_nodes: dictionary with levels as keys and topics as values.
    :return: TOG as a networkx Graph.
    """
    logger.info("Building the Topic Occupancy Graph (TOG).")
    to_graph = nx.Graph()

    for item_id in tqdm(list(multi_occupancy_counts.keys()), desc="Building TOG"):
        item_topic_list = item_collection_dict[item_id]

        # item_level_topics = build_item_level_topics(itemTopiclist, topic_graph_level_nodes)
        # item_level_topics = {}
        item_level_topics = defaultdict(list)
        for topic in item_topic_list:
            for level in list(topic_graph_level_nodes.keys()):
                if topic in topic_graph_level_nodes[level]:
                    if level in item_level_topics:
                        item_level_topics[level].append(topic)
                    # if there's not already a topic for this paper at this level
                    else:
                        item_level_topics[level] = [topic]
        # this is the paper level dict()
        itemLevelOccupancyCounts = multi_occupancy_counts[item_id]
        # for each level of the THG
        for level in list(itemLevelOccupancyCounts.keys()):
            # print(f'level:',level,'occupancy:',itemLevelOccupancyCounts[level])
            # remember we can have multi occupancy at any THG level
            # add one or more nodes and connect them with an edge if more than one
            # we have the special case for level = 1 where at least one of the nodes will be the original Paper node
            if level == 1:
                # one of these is going to be a Paper node the others will be PaperClone nodes
                levelNodes = []
                # NB python 0 counting
                for i in range(itemLevelOccupancyCounts[level]):
                    i += 1
                    # create nodes
                    # create an id for the node itemID_level_[i]
                    levelNodes.append(str(item_id) + "_" + str(level) + "_" + str(i))
                # use itertools combinations to create pairwise edge list
                nodeConnections = list(IT.combinations(levelNodes, 2))
                # this is the condition where the paper is only in one topic in the top level
                if len(nodeConnections) == 0:
                    # just add the node - NB this is a top level (original) paper node type=Paper
                    to_graph.add_node(
                        levelNodes[0],
                        type="Paper",
                        visibilityLevel=level,
                        visibilityLevelTopic=str(item_level_topics[level].pop()),
                    )
                # here we need to label one of the nodes as a root node, the ohters as clone nodes
                # to do this I'm going to try to create a separate temporary graph and then combine it with the
                # togGraph using the nx.compose() function
                else:
                    # temp graph
                    tempGraph = nx.Graph()
                    # create the graph, note all connections are 'CLONE_CONNECTIONS'
                    tempGraph.add_edges_from(nodeConnections, type="CLONE_CONNECTIONS")
                    # pick a random node replace label with label='Paper' i.e. one remains the "root" paper
                    # pull all the node_ids
                    nodeIds = list(tempGraph.nodes)
                    # set the type of the first node in this tempGraph back to type='Paper' and add the
                    nodeAttributeMapping = {
                        nodeIds.pop(): {
                            "type": "Paper",
                            "visibilityLevel": level,
                            "visibilityLevelTopic": str(item_level_topics[level].pop()),
                        }
                    }
                    for nodeId in nodeIds:
                        # also add the individual paper topicIDs to each node for the level 'popping' one off at a time
                        nodeAttributeMapping[nodeId] = {
                            "type": "PaperClone",
                            "visibilityLevel": level,
                            "visibilityLevelTopic": str(item_level_topics[level].pop()),
                        }
                    # now update the nodes
                    nx.set_node_attributes(tempGraph, nodeAttributeMapping)
                    # add the tempGraph back into the main one
                    to_graph = nx.compose(to_graph, tempGraph)
            # otherwise at all other levels all nodes are by definition clones (as currently implemented)
            else:
                levelNodes = []
                # these are all PaperClone nodes
                for i in range(itemLevelOccupancyCounts[level]):
                    i += 1
                    # create nodes
                    # create an id for the node itemID_level_[i]
                    levelNodes.append(str(item_id) + "_" + str(level) + "_" + str(i))
                # if there is only one node in the level
                if len(levelNodes) == 1:
                    # just add the node
                    to_graph.add_node(
                        levelNodes[0],
                        type="PaperClone",
                        visibilityLevel=level,
                        visibilityLevelTopic=str(item_level_topics[level].pop()),
                    )
                # otherwise create the tempGraph as above
                else:
                    nodeConnections = list(IT.combinations(levelNodes, 2))
                    # need to iterate through to make sure the node type is correct
                    tempGraph = nx.Graph()
                    # create the graph, note all connections are 'CLONE_CONNECTIONS'
                    tempGraph.add_edges_from(nodeConnections, type="CLONE_CONNECTIONS")
                    nodeAttributeMapping = dict()
                    nodeIds = list(tempGraph.nodes)
                    for nodeId in nodeIds:
                        # also add the individual paper topicIDs to each node for the level 'popping' one off at a time
                        nodeAttributeMapping[nodeId] = {
                            "type": "PaperClone",
                            "visibilityLevel": level,
                            "visibilityLevelTopic": str(item_level_topics[level].pop()),
                        }
                    # now update the nodes
                    nx.set_node_attributes(tempGraph, nodeAttributeMapping)
                    # add the tempGraph back into the main one
                    to_graph = nx.compose(to_graph, tempGraph)
        else:
            pass

        # for level, topic_ids in item_level_topics.items():
        #     level_count = occupancy_counts[level]
        #     if level == 1:
        #         level_nodes = [f"{item_id}_1_{i+1}" for i in range(level_count)]
        #         toGraph = add_edges_for_level_one(toGraph, level_nodes, topic_ids)
        #     else:
        #         level_nodes = [f"{item_id}_{level}_{i+1}" for i in range(level_count)]
        #         toGraph = add_edges_for_clones(toGraph, level_nodes, topic_ids, level)
    print("build topic occupancy graph")

    return to_graph


def compute_paper_and_clones_stats(itemCollectionTags: dict[int, list[int]], toGraph: nx.DiGraph) -> None:
    """
    Compute and log statistics on the number of original paper nodes and clone nodes.

    :param itemCollectionTags: dictionary mapping item IDs to collection tags.
    :param toGraph: The TOG graph.
    """
    originalPaperNodeNumber = len(itemCollectionTags)
    togPaperNodeNumber = nx.number_of_nodes(toGraph)
    logger.info(
        f"There are {originalPaperNodeNumber} original paper nodes "
        f"and {(togPaperNodeNumber - originalPaperNodeNumber)} clone nodes"
    )

    togGraphEdgeNumber = nx.number_of_edges(toGraph)
    logger.info(f"The TOG graph has {togPaperNodeNumber} nodes and {togGraphEdgeNumber} edges.")

def count_occupancy_THG(
    topicGraphLevelNodes: dict[int, list[int]],
    item_collection_dict: dict[int, list[int]],
) -> dict[int, dict[int, int]]:
    """
    Count the number of times each item appears at each level of the Topic Hierarchy Graph (THG).

    :param topicGraphLevelNodes: dictionary mapping levels to topic nodes.
    :param item_collection_dict: dictionary mapping item IDs to topics.
    :return: dictionary with item IDs and their occupancy counts at each level.
    """
    multiOccupancyCounts = {}
    # for item_id, topicCollection in item_collection_dict.items():
    # itemLevelCounts = {
    #     level: topicCollection.count(topic)
    #     for level, levelNodes in topicGraphLevelNodes.items()
    #     for topic in topicCollection if topic in levelNodes
    # }
    # multiOccupancyCounts[item_id] = itemLevelCounts
    # this contains a dict for each paper with counts per level of the THG
    for itemID in list(item_collection_dict.keys()):
        itemLevelCounts = dict()
        topicCollection = item_collection_dict[itemID]
        # iterate through this and convert into the level count dict
        for topic in topicCollection:
            if topic == "ROOT":
                pass
            else:
                # map topic to level using topicGraphLevelNodes
                for level in list(topicGraphLevelNodes.keys()):
                    levelTopiclist = topicGraphLevelNodes[level]
                    if topic in levelTopiclist:
                        # if there's already a count in the level
                        if level in itemLevelCounts:
                            itemLevelCounts[level] += 1
                        # if there isn't a count already in the level
                        else:
                            itemLevelCounts[level] = 1
                    # if the topic isn't in the current level
                    else:
                        pass
        multiOccupancyCounts[itemID] = itemLevelCounts
    return multiOccupancyCounts


def fetch_tree_levels(topicGraph: nx.DiGraph) -> dict[int, list[int]]:
    """
    Fetch the tree levels for the topic nodes and create a dictionary mapping levels to topic IDs.

    :param topicGraph: Directed graph representing the topic hierarchy.
    :return: dictionary mapping levels to topic node IDs.
    """
    treeLevels = nx.single_source_shortest_path_length(topicGraph, "ROOT")
    maxDepth = max(treeLevels.values())
    topicGraphLevelNodes = {level: [k for k, v in treeLevels.items() if v == level] for level in range(0, maxDepth + 1)}
    return topicGraphLevelNodes
