from pynamodb.indexes import Index as PynamoDBIndex


class Index(PynamoDBIndex):
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
    async def query(self,
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
        return await self.Meta.model.query(
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=self.Meta.index_name,
            scan_index_forward=scan_index_forward,
            consistent_read=consistent_read,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            attributes_to_get=attributes_to_get,
            **filters
        )

    @classmethod
    async def scan(cls,
                   filter_condition=None,
                   segment=None,
                   total_segments=None,
                   limit=None,
                   last_evaluated_key=None,
                   page_size=None,
                   consistent_read=None,
                   **filters):
        """
        Scans an index
        """
        return await cls.Meta.model.scan(
            filter_condition=filter_condition,
            segment=segment,
            total_segments=total_segments,
            limit=limit,
            last_evaluated_key=last_evaluated_key,
            page_size=page_size,
            consistent_read=consistent_read,
            index_name=cls.Meta.index_name,
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
