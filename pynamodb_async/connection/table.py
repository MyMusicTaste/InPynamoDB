from pynamodb_async.connection.base import AsyncConnection
from pynamodb.connection.table import TableConnection as PynamoDBTableConnection


class TableConnection(PynamoDBTableConnection):
    def __init__(self, table_name, region=None, host=None, session_cls=None, request_timeout_seconds=None,
                 max_retry_attempts=None, base_backoff_ms=None):
        super(TableConnection, self).__init__(table_name, region, host, session_cls, request_timeout_seconds,
                                              max_retry_attempts, base_backoff_ms)

        self.connection = AsyncConnection(region=region, host=host, session_cls=session_cls,
                                          request_timeout_seconds=request_timeout_seconds,
                                          max_retry_attempts=max_retry_attempts,
                                          base_backoff_ms=base_backoff_ms)

    async def put_item(self, hash_key,
                       range_key=None,
                       attributes=None,
                       condition=None,
                       expected=None,
                       conditional_operator=None,
                       return_values=None,
                       return_consumed_capacity=None,
                       return_item_collection_metrics=None):
        """
        Performs the PutItem operation and returns the result
        """
        return await self.connection.put_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            attributes=attributes,
            condition=condition,
            expected=expected,
            conditional_operator=conditional_operator,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    async def batch_write_item(self,
                               put_items=None,
                               delete_items=None,
                               return_consumed_capacity=None,
                               return_item_collection_metrics=None):
        """
        Performs the batch_write_item operation
        """
        return await self.connection.batch_write_item(
            self.table_name,
            put_items=put_items,
            delete_items=delete_items,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    async def update_item(self,
                          hash_key,
                          range_key=None,
                          actions=None,
                          attribute_updates=None,
                          condition=None,
                          expected=None,
                          conditional_operator=None,
                          return_consumed_capacity=None,
                          return_item_collection_metrics=None,
                          return_values=None
                          ):
        """
        Performs the UpdateItem operation
        """
        return await self.connection.update_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            actions=actions,
            attribute_updates=attribute_updates,
            condition=condition,
            expected=expected,
            conditional_operator=conditional_operator,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics,
            return_values=return_values)

    async def scan(self,
                   filter_condition=None,
                   attributes_to_get=None,
                   limit=None,
                   conditional_operator=None,
                   scan_filter=None,
                   return_consumed_capacity=None,
                   segment=None,
                   total_segments=None,
                   exclusive_start_key=None,
                   consistent_read=None):
        """
        Performs the scan operation
        """
        return await self.connection.scan(
            self.table_name,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            limit=limit,
            conditional_operator=conditional_operator,
            scan_filter=scan_filter,
            return_consumed_capacity=return_consumed_capacity,
            segment=segment,
            total_segments=total_segments,
            exclusive_start_key=exclusive_start_key,
            consistent_read=consistent_read)

    async def delete_item(self, hash_key,
                          range_key=None,
                          condition=None,
                          expected=None,
                          conditional_operator=None,
                          return_values=None,
                          return_consumed_capacity=None,
                          return_item_collection_metrics=None):
        """
        Performs the DeleteItem operation and returns the result
        """
        return await self.connection.delete_item(
            self.table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            expected=expected,
            conditional_operator=conditional_operator,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    async def batch_write_item(self,
                               put_items=None,
                               delete_items=None,
                               return_consumed_capacity=None,
                               return_item_collection_metrics=None):
        """
        Performs the batch_write_item operation
        """
        return await self.connection.batch_write_item(
            self.table_name,
            put_items=put_items,
            delete_items=delete_items,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics)

    async def query(self,
                    hash_key,
                    range_key_condition=None,
                    filter_condition=None,
                    attributes_to_get=None,
                    consistent_read=False,
                    exclusive_start_key=None,
                    index_name=None,
                    key_conditions=None,
                    query_filters=None,
                    limit=None,
                    return_consumed_capacity=None,
                    scan_index_forward=None,
                    conditional_operator=None,
                    select=None
                    ):
        """
        Performs the Query operation and returns the result
        """
        return await self.connection.query(
            self.table_name,
            hash_key,
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            attributes_to_get=attributes_to_get,
            consistent_read=consistent_read,
            exclusive_start_key=exclusive_start_key,
            index_name=index_name,
            key_conditions=key_conditions,
            query_filters=query_filters,
            limit=limit,
            return_consumed_capacity=return_consumed_capacity,
            scan_index_forward=scan_index_forward,
            conditional_operator=conditional_operator,
            select=select)

    async def delete_table(self):
        """
        Performs the DeleteTable operation and returns the result
        """
        return await self.connection.delete_table(self.table_name)

    async def describe_table(self):
        """
        Performs the DescribeTable operation and returns the result
        """
        return await self.connection.describe_table(self.table_name)

    async def create_table(self,
                           attribute_definitions=None,
                           key_schema=None,
                           read_capacity_units=None,
                           write_capacity_units=None,
                           global_secondary_indexes=None,
                           local_secondary_indexes=None,
                           stream_specification=None):
        """
        Performs the CreateTable operation and returns the result
        """
        return await self.connection.create_table(
            self.table_name,
            attribute_definitions=attribute_definitions,
            key_schema=key_schema,
            read_capacity_units=read_capacity_units,
            write_capacity_units=write_capacity_units,
            global_secondary_indexes=global_secondary_indexes,
            local_secondary_indexes=local_secondary_indexes,
            stream_specification=stream_specification
        )
