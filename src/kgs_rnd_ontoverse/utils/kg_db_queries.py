CREATE_PAPER_INDEXES_QUERY = [
    "create index paper_itemID if not exists for (p:Paper) on p.itemID;",
    "create index paper_nodeID if not exists for (p:Paper) on p.nodeID;",
    "create index paperClone_nodeID if not exists for (p:PaperClone) on p.nodeID;",
    "create index paperClone_itemID if not exists for (p:PaperClone) on p.itemID;",
]

COLLECTION_PARENT_OF_COLLECTION = """
    MATCH (parent:Collection)
    WHERE parent.collectionID = '{parent}'
    MATCH (child:Collection)
    WHERE child.collectionID = '{child}'
    CREATE (parent)-[r:PARENT_OF]->(child);
"""

PAPER_MEMBER_OF_COLLECTION = """
    MATCH (c:Collection)
    MATCH (p:Paper)
    WHERE c.collectionID = p.visibilityLevelTopic 
    CREATE (p)-[r:MEMBER_OF]->(c);
"""

PAPERCLONE_MEMBER_OF_COLLECTION = """
    MATCH (c:Collection)
    MATCH (p:PaperClone)
    WHERE c.collectionID = p.visibilityLevelTopic
    CREATE (p)-[r:MEMBER_OF]->(c);
"""

MATCHING_PAPER_QUERY = """
    MATCH (a)
    MATCH (b)
    WHERE a.itemID = b.itemID AND ID(a) <> ID(b)
    MERGE (a)<-[r:MATCHING_PAPER]->(b)
"""
