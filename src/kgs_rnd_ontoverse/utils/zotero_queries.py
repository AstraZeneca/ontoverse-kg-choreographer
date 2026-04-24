"""Parameterized SQLite query fragments for Zotero databases.

Each function returns ``(sql, params)`` for use with ``sqlite3.Cursor.execute(sql, params)``.
"""


def get_zotero_library_id_query(group_name: str) -> tuple[str, tuple[str, ...]]:
    sql = (
        "SELECT libraries.libraryID FROM libraries, groups "
        "WHERE libraries.libraryID = groups.libraryID AND groups.name = ?"
    )
    return sql, (group_name,)


def get_item_ids_query(library_id: int) -> tuple[str, tuple[int, ...]]:
    sql = (
        "SELECT DISTINCT itemID FROM collectionItems, "
        "(SELECT DISTINCT collectionID FROM collections WHERE libraryID = ?) AS onco "
        "WHERE onco.collectionID = collectionItems.collectionID"
    )
    return sql, (library_id,)


def get_topics_query(library_id: int) -> tuple[str, tuple[int, ...]]:
    sql = "SELECT DISTINCT collectionID, collectionName FROM collections WHERE libraryID = ?"
    return sql, (library_id,)


def get_top_topics_query(library_id: int) -> tuple[str, tuple[int, ...]]:
    sql = (
        "SELECT collectionId, collectionName FROM collections "
        "WHERE libraryID = ? AND parentCollectionId IS NULL"
    )
    return sql, (library_id,)


def get_all_tags_query(library_id: int) -> tuple[str, tuple[int, ...]]:
    sql = (
        "SELECT DISTINCT itemID, group_concat(tags.tag) AS tags "
        "FROM itemTags, tags "
        "WHERE itemTags.tagID = tags.tagID AND libraryID = ? "
        "GROUP BY itemID"
    )
    return sql, (library_id,)


def get_item_details_query(item_id: int) -> tuple[str, tuple[int, ...]]:
    sql = (
        "SELECT fieldName, value FROM itemData, fields, itemDataValues "
        "WHERE itemID = ? AND itemData.fieldID = fields.fieldID "
        "AND itemData.valueID = itemDataValues.valueID"
    )
    return sql, (item_id,)


def get_authors_query(item_id: int) -> tuple[str, tuple[int, ...]]:
    sql = (
        "SELECT creators.creatorID, orderIndex, firstName, lastName "
        "FROM itemCreators, creators "
        "WHERE itemID = ? AND itemCreators.creatorID = creators.creatorID "
        "ORDER BY orderIndex ASC"
    )
    return sql, (item_id,)


def get_collection_names_query(library_id: int, collection_id: int) -> tuple[str, tuple[int, int]]:
    sql = (
        "SELECT collectionId, collectionName FROM collections "
        "WHERE libraryID = ? AND parentCollectionId = ?"
    )
    return sql, (library_id, collection_id)
