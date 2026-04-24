# # Defining node models with neomodel
# from neomodel import IntegerProperty, RelationshipTo, StringProperty, StructuredNode


class BibliographicObject:
    """
    Class to represent a bibliographic item such as a paper, book, etc.
    """

    def __init__(self, name: str):
        """
        Initialize a bibliographic object with a specified name.
        :param str name: object label e.g., paper, book, etc.
        """
        self.name = name
        self.attributes = {}

    def add_attributes(self, attribute: dict) -> None:
        """
        Add attributes to the bibliographic object.
        :param dict attribute: dictionary containing attribute names and values
        """
        self.attributes.update(attribute)


# class Paper(StructuredNode):
#     nodeID = StringProperty(unique_index=True)
#     visibilityLevel = StringProperty()
#     visibilityLevelTopic = StringProperty()
#     collectionTags = StringProperty()
#     itemID = IntegerProperty(unique_index=True)
#     title = StringProperty()
#     authors = StringProperty()

#     # Relationships
#     similar_papers = RelationshipTo('Paper', 'SIMILAR')
#     within_topic = RelationshipTo('Paper', 'WITHIN_TOPIC')
#     between_topic = RelationshipTo('Paper', 'BETWEEN_TOPIC')

# class PaperClone(StructuredNode):
#     nodeID = StringProperty(unique_index=True)
#     visibilityLevel = StringProperty()
#     visibilityLevelTopic = StringProperty()
#     collectionTags = StringProperty()
#     itemID = IntegerProperty()
#     clone_number = IntegerProperty()

#     # Relationships
#     similar_papers = RelationshipTo('PaperClone', 'SIMILAR')
#     within_topic = RelationshipTo('PaperClone', 'WITHIN_TOPIC')
#     between_topic = RelationshipTo('PaperClone', 'BETWEEN_TOPIC')

# class Collection(StructuredNode):
#     collectionID = StringProperty(unique_index=True)
#     collectionName = StringProperty()
#     graphLevel = IntegerProperty()
#     parent_of = RelationshipTo('Collection', 'PARENT_OF')


# class CollectionRelationships:
#     parent_of = RelationshipTo("Collection", "PARENT_OF")
#     member_of = RelationshipTo("Collection", "MEMBER_OF")

# class PaperRelationships:
#     member_of = RelationshipTo("Collection", "MEMBER_OF")
