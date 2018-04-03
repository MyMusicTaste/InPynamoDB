"""
InPynamoDB Indexes
"""
from pynamodb.compat import getmembers_issubclass
from pynamodb.connection.util import pythonic
from pynamodb.constants import ATTR_NAME, ATTR_TYPE, KEY_TYPE, ATTR_TYPE_MAP, KEY_SCHEMA, ATTR_DEFINITIONS, \
    META_CLASS_NAME
from pynamodb.indexes import Index as _Index, IndexMeta
from pynamodb.types import HASH, RANGE

from inpynamodb.attributes import Attribute


class Index(_Index, metaclass=IndexMeta):
    """
    Base class for secondary indexes
    """
    Meta = None

    def __init__(self):
        if self.Meta is None:
            raise ValueError("Indexes require a Meta class for settings")
        if not hasattr(self.Meta, "projection"):
            raise ValueError("No projection defined, define a projection for this class")

    @classmethod
    async def count(cls,
                    hash_key,
                    range_key_condition=None,
                    filter_condition=None,
                    consistent_read=False,
                    **filters):
        """
        Count on an index
        """

        return await cls.Meta.model.count(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=cls.Meta.index_name,
            consistent_read=consistent_read,
            **filters
        )

    @classmethod
    async def query(cls,
                    hash_key,
                    range_key_condition=None,
                    filter_condition=None,
                    scan_index_forward=None,
                    consistent_read=False,
                    limit=None,
                    last_evaluated_key=None,
                    attributes_to_get=None,
                    **filters):
        """
        Queries an index
        """
        return await cls.Meta.model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=cls.Meta.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            **filters
        )


class GlobalSecondaryIndex(Index):
    """
    A global secondary index
    """
    pass


class LocalSecondaryIndex(Index):
    """
    A local secondary index
    """
    pass
