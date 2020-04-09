"""
Lowest level connection
"""
import logging

from pynamodb.connection.util import pythonic
from pynamodb.exceptions import TableError, TableDoesNotExist, DeleteError, UpdateError, PutError, TransactWriteError, \
    TransactGetError, GetError, ScanError, QueryError

import logging
import uuid

import aiobotocore
import botocore.client

from async_property import async_property
from botocore.exceptions import BotoCoreError, ClientError
from pynamodb.connection.base import Connection, MetaTable
from pynamodb.constants import SERVICE_NAME, DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, UPDATE_TIME_TO_LIVE, \
    DELETE_TABLE, CREATE_TABLE, RETURN_CONSUMED_CAPACITY, TOTAL, TABLE_NAME, CONSUMED_CAPACITY, CAPACITY_UNITS, \
    TABLE_KEY, DEFAULT_BILLING_MODE, BILLING_MODE, PROVISIONED_THROUGHPUT, WRITE_CAPACITY_UNITS, READ_CAPACITY_UNITS, \
    ATTR_NAME, ATTR_TYPE, ATTR_DEFINITIONS, AVAILABLE_BILLING_MODES, PAY_PER_REQUEST_BILLING_MODE, INDEX_NAME, \
    KEY_SCHEMA, KEY_TYPE, PROJECTION, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, STREAM_SPECIFICATION, \
    STREAM_ENABLED, STREAM_VIEW_TYPE, TIME_TO_LIVE_SPECIFICATION, ENABLED, UPDATE, GLOBAL_SECONDARY_INDEX_UPDATES, \
    EXCLUSIVE_START_TABLE_NAME, LIMIT, ITEM, KEY, PROJECTION_EXPRESSION, CONDITION_EXPRESSION, CONSISTENT_READ, \
    UPDATE_EXPRESSION, EXPRESSION_ATTRIBUTE_NAMES, EXPRESSION_ATTRIBUTE_VALUES, DELETE_ITEM, UPDATE_ITEM, PUT_ITEM, \
    TRANSACT_CONDITION_CHECK, TRANSACT_DELETE, TRANSACT_PUT, TRANSACT_UPDATE, TRANSACT_ITEMS, TRANSACT_WRITE_ITEMS, \
    TRANSACT_GET, TRANSACT_GET_ITEMS, REQUEST_ITEMS, PUT_REQUEST, DELETE_REQUEST, BATCH_WRITE_ITEM, KEYS, \
    BATCH_GET_ITEM, GET_ITEM, FILTER_EXPRESSION, SEGMENT, TOTAL_SEGMENTS, SCAN, KEY_CONDITION_EXPRESSION, SELECT_VALUES, \
    SELECT, SCAN_INDEX_FORWARD, QUERY
from pynamodb.expressions.operand import Path
from pynamodb.expressions.projection import create_projection_expression
from pynamodb.expressions.update import Update

BOTOCORE_EXCEPTIONS = (BotoCoreError, ClientError)

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class AsyncConnection(Connection):
    """
    A higher level abstraction over botocore
    """
    def __repr__(self):
        return "AsyncConnection"

    @async_property
    async def session(self):
        """
        Returns a valid aiobotocore session
        """
        if getattr(self, '_session', None) is None:
            self._session = aiobotocore.get_session()
        return self._session

    @async_property
    async def client(self):
        """
        Returns a aiobotocore dynamodb client
        """
        if not self._client or (self._client._request_signer and not self._client._request_signer._credentials):
            config = botocore.client.Config(
                connect_timeout=self._connect_timeout_seconds,
                read_timeout=self._read_timeout_seconds,
                max_pool_connections=self._max_pool_connections)
            self._client = (await self.session).create_client(
                SERVICE_NAME, self.region, endpoint_url=self.host, config=config
            )
        return self._client

    async def dispatch(self, operation_name, operation_kwargs):
        """
        Dispatches `operation_name` with arguments `operation_kwargs`

        Raises TableDoesNotExist if the specified table does not exist
        """
        if operation_name not in [DESCRIBE_TABLE, LIST_TABLES, UPDATE_TABLE, UPDATE_TIME_TO_LIVE, DELETE_TABLE, CREATE_TABLE]:
            if RETURN_CONSUMED_CAPACITY not in operation_kwargs:
                operation_kwargs.update(self.get_consumed_capacity_map(TOTAL))
        self._log_debug(operation_name, operation_kwargs)

        table_name = operation_kwargs.get(TABLE_NAME)
        req_uuid = uuid.uuid4()

        self.send_pre_boto_callback(operation_name, req_uuid, table_name)
        data = await (await self.client)._make_api_call(operation_name, operation_kwargs)
        self.send_post_boto_callback(operation_name, req_uuid, table_name)

        if data and CONSUMED_CAPACITY in data:
            capacity = data.get(CONSUMED_CAPACITY)
            if isinstance(capacity, dict) and CAPACITY_UNITS in capacity:
                capacity = capacity.get(CAPACITY_UNITS)
            log.debug("%s %s consumed %s units",  data.get(TABLE_NAME, ''), operation_name, capacity)
        return data

    async def get_meta_table(self, table_name, refresh=False):
        """
        Returns a MetaTable
        """
        if table_name not in self._tables or refresh:
            operation_kwargs = {
                TABLE_NAME: table_name
            }
            try:
                data = await self.dispatch(DESCRIBE_TABLE, operation_kwargs)
                self._tables[table_name] = MetaTable(data.get(TABLE_KEY))
            except BotoCoreError as e:
                raise TableError("Unable to describe table: {}".format(e))
            except ClientError as e:
                if 'ResourceNotFound' in e.response['Error']['Code']:
                    raise TableDoesNotExist(e.response['Error']['Message'])
                else:
                    raise
        return self._tables[table_name]

    async def create_table(self,
                           table_name,
                           attribute_definitions=None,
                           key_schema=None,
                           read_capacity_units=None,
                           write_capacity_units=None,
                           global_secondary_indexes=None,
                           local_secondary_indexes=None,
                           stream_specification=None,
                           billing_mode=DEFAULT_BILLING_MODE):
        """
        Performs the CreateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            BILLING_MODE: billing_mode,
            PROVISIONED_THROUGHPUT: {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units,
            }
        }
        attrs_list = []
        if attribute_definitions is None:
            raise ValueError("attribute_definitions argument is required")
        for attr in attribute_definitions:
            attrs_list.append({
                ATTR_NAME: attr.get(pythonic(ATTR_NAME)),
                ATTR_TYPE: attr.get(pythonic(ATTR_TYPE))
            })
        operation_kwargs[ATTR_DEFINITIONS] = attrs_list

        if billing_mode not in AVAILABLE_BILLING_MODES:
            raise ValueError("incorrect value for billing_mode, available modes: {}".format(AVAILABLE_BILLING_MODES))
        if billing_mode == PAY_PER_REQUEST_BILLING_MODE:
            del operation_kwargs[PROVISIONED_THROUGHPUT]

        if global_secondary_indexes:
            global_secondary_indexes_list = []
            for index in global_secondary_indexes:
                index_kwargs = {
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                    PROVISIONED_THROUGHPUT: index.get(pythonic(PROVISIONED_THROUGHPUT))
                }
                if billing_mode == PAY_PER_REQUEST_BILLING_MODE:
                    del index_kwargs[PROVISIONED_THROUGHPUT]
                global_secondary_indexes_list.append(index_kwargs)
            operation_kwargs[GLOBAL_SECONDARY_INDEXES] = global_secondary_indexes_list

        if key_schema is None:
            raise ValueError("key_schema is required")
        key_schema_list = []
        for item in key_schema:
            key_schema_list.append({
                ATTR_NAME: item.get(pythonic(ATTR_NAME)),
                KEY_TYPE: str(item.get(pythonic(KEY_TYPE))).upper()
            })
        operation_kwargs[KEY_SCHEMA] = sorted(key_schema_list, key=lambda x: x.get(KEY_TYPE))

        local_secondary_indexes_list = []
        if local_secondary_indexes:
            for index in local_secondary_indexes:
                local_secondary_indexes_list.append({
                    INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                    KEY_SCHEMA: sorted(index.get(pythonic(KEY_SCHEMA)), key=lambda x: x.get(KEY_TYPE)),
                    PROJECTION: index.get(pythonic(PROJECTION)),
                })
            operation_kwargs[LOCAL_SECONDARY_INDEXES] = local_secondary_indexes_list

        if stream_specification:
            operation_kwargs[STREAM_SPECIFICATION] = {
                STREAM_ENABLED: stream_specification[pythonic(STREAM_ENABLED)],
                STREAM_VIEW_TYPE: stream_specification[pythonic(STREAM_VIEW_TYPE)]
            }

        try:
            data = await self.dispatch(CREATE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to create table: {}".format(e)) from e
        return data

    async def update_time_to_live(self, table_name, ttl_attribute_name):
        """
        Performs the UpdateTimeToLive operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name,
            TIME_TO_LIVE_SPECIFICATION: {
                ATTR_NAME: ttl_attribute_name,
                ENABLED: True,
            }
        }
        try:
            return await self.dispatch(UPDATE_TIME_TO_LIVE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to update TTL on table: {}".format(e)) from e

    async def delete_table(self, table_name):
        """
        Performs the DeleteTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name
        }
        try:
            data = await self.dispatch(DELETE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to delete table: {}".format(e)) from e
        return data

    async def update_table(self,
                           table_name,
                           read_capacity_units=None,
                           write_capacity_units=None,
                           global_secondary_index_updates=None):
        """
        Performs the UpdateTable operation
        """
        operation_kwargs = {
            TABLE_NAME: table_name
        }
        if read_capacity_units and not write_capacity_units or write_capacity_units and not read_capacity_units:
            raise ValueError("read_capacity_units and write_capacity_units are required together")
        if read_capacity_units and write_capacity_units:
            operation_kwargs[PROVISIONED_THROUGHPUT] = {
                READ_CAPACITY_UNITS: read_capacity_units,
                WRITE_CAPACITY_UNITS: write_capacity_units
            }
        if global_secondary_index_updates:
            global_secondary_indexes_list = []
            for index in global_secondary_index_updates:
                global_secondary_indexes_list.append({
                    UPDATE: {
                        INDEX_NAME: index.get(pythonic(INDEX_NAME)),
                        PROVISIONED_THROUGHPUT: {
                            READ_CAPACITY_UNITS: index.get(pythonic(READ_CAPACITY_UNITS)),
                            WRITE_CAPACITY_UNITS: index.get(pythonic(WRITE_CAPACITY_UNITS))
                        }
                    }
                })
            operation_kwargs[GLOBAL_SECONDARY_INDEX_UPDATES] = global_secondary_indexes_list
        try:
            return await self.dispatch(UPDATE_TABLE, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Failed to update table: {}".format(e)) from e

    async def list_tables(self, exclusive_start_table_name=None, limit=None):
        """
        Performs the ListTables operation
        """
        operation_kwargs = {}
        if exclusive_start_table_name:
            operation_kwargs.update({
                EXCLUSIVE_START_TABLE_NAME: exclusive_start_table_name
            })
        if limit is not None:
            operation_kwargs.update({
                LIMIT: limit
            })
        try:
            return await self.dispatch(LIST_TABLES, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TableError("Unable to list tables: {}".format(e)) from e

    async def describe_table(self, table_name):
        """
        Performs the DescribeTable operation
        """
        try:
            tbl = await self.get_meta_table(table_name, refresh=True)
            if tbl:
                return tbl.data
        except ValueError:
            pass
        raise TableDoesNotExist(table_name)

    async def get_item_attribute_map(self, table_name, attributes, item_key=ITEM, pythonic_key=True):
        """
        Builds up a dynamodb compatible AttributeValue map
        """
        tbl = await self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_item_attribute_map(
            attributes,
            item_key=item_key,
            pythonic_key=pythonic_key)

    async def get_attribute_type(self, table_name, attribute_name, value=None):
        """
        Returns the proper attribute type for a given attribute name
        :param value: The attribute value an be supplied just in case the type is already included
        """
        tbl = await self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_attribute_type(attribute_name, value=value)

    async def get_identifier_map(self, table_name, hash_key, range_key=None, key=KEY):
        """
        Builds the identifier map that is common to several operations
        """
        tbl = await self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_identifier_map(hash_key, range_key=range_key, key=key)

    async def get_exclusive_start_key_map(self, table_name, exclusive_start_key):
        """
        Builds the exclusive start key attribute map
        """
        tbl = await self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table {}".format(table_name))
        return tbl.get_exclusive_start_key_map(exclusive_start_key)

    async def get_operation_kwargs(self,
                                   table_name,
                                   hash_key,
                                   range_key=None,
                                   key=KEY,
                                   attributes=None,
                                   attributes_to_get=None,
                                   actions=None,
                                   condition=None,
                                   consistent_read=None,
                                   return_values=None,
                                   return_consumed_capacity=None,
                                   return_item_collection_metrics=None,
                                   return_values_on_condition_failure=None):
        self._check_condition('condition', condition)

        operation_kwargs = {}
        name_placeholders = {}
        expression_attribute_values = {}

        operation_kwargs[TABLE_NAME] = table_name
        operation_kwargs.update(await self.get_identifier_map(table_name, hash_key, range_key, key=key))
        if attributes and operation_kwargs.get(ITEM) is not None:
            attrs = await self.get_item_attribute_map(table_name, attributes)
            operation_kwargs[ITEM].update(attrs[ITEM])
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if condition is not None:
            condition_expression = condition.serialize(name_placeholders, expression_attribute_values)
            operation_kwargs[CONDITION_EXPRESSION] = condition_expression
        if consistent_read is not None:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if return_values is not None:
            operation_kwargs.update(self.get_return_values_map(return_values))
        if return_values_on_condition_failure is not None:
            operation_kwargs.update(self.get_return_values_on_condition_failure_map(return_values_on_condition_failure))
        if return_consumed_capacity is not None:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics is not None:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        if actions is not None:
            update_expression = Update(*actions)
            operation_kwargs[UPDATE_EXPRESSION] = update_expression.serialize(
                name_placeholders,
                expression_attribute_values
            )
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values
        return operation_kwargs

    async def delete_item(self,
                          table_name,
                          hash_key,
                          range_key=None,
                          condition=None,
                          return_values=None,
                          return_consumed_capacity=None,
                          return_item_collection_metrics=None):
        """
        Performs the DeleteItem operation and returns the result
        """
        operation_kwargs = await self.get_operation_kwargs(
            table_name,
            hash_key,
            range_key=range_key,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return await self.dispatch(DELETE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise DeleteError("Failed to delete item: {}".format(e)) from e

    async def update_item(self,
                          table_name,
                          hash_key,
                          range_key=None,
                          actions=None,
                          condition=None,
                          return_consumed_capacity=None,
                          return_item_collection_metrics=None,
                          return_values=None):
        """
        Performs the UpdateItem operation
        """
        if not actions:
            raise ValueError("'actions' cannot be empty")

        operation_kwargs = await self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            actions=actions,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return await self.dispatch(UPDATE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise UpdateError("Failed to update item: {}".format(e)) from e

    async def put_item(self,
                       table_name,
                       hash_key,
                       range_key=None,
                       attributes=None,
                       condition=None,
                       return_values=None,
                       return_consumed_capacity=None,
                       return_item_collection_metrics=None):
        """
        Performs the PutItem operation and returns the result
        """
        operation_kwargs = await self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            key=ITEM,
            attributes=attributes,
            condition=condition,
            return_values=return_values,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        try:
            return await self.dispatch(PUT_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to put item: {}".format(e)) from e

    async def transact_write_items(self,
                                   condition_check_items,
                                   delete_items,
                                   put_items,
                                   update_items,
                                   client_request_token=None,
                                   return_consumed_capacity=None,
                                   return_item_collection_metrics=None):
        """
        Performs the TransactWrite operation and returns the result
        """
        transact_items = []
        transact_items.extend(
            {TRANSACT_CONDITION_CHECK: item} for item in condition_check_items
        )
        transact_items.extend(
            {TRANSACT_DELETE: item} for item in delete_items
        )
        transact_items.extend(
            {TRANSACT_PUT: item} for item in put_items
        )
        transact_items.extend(
            {TRANSACT_UPDATE: item} for item in update_items
        )

        operation_kwargs = self._get_transact_operation_kwargs(
            client_request_token=client_request_token,
            return_consumed_capacity=return_consumed_capacity,
            return_item_collection_metrics=return_item_collection_metrics
        )
        operation_kwargs[TRANSACT_ITEMS] = transact_items

        try:
            return await self.dispatch(TRANSACT_WRITE_ITEMS, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TransactWriteError("Failed to write transaction items", e) from e

    async def transact_get_items(self, get_items, return_consumed_capacity=None):
        """
        Performs the TransactGet operation and returns the result
        """
        operation_kwargs = self._get_transact_operation_kwargs(return_consumed_capacity=return_consumed_capacity)
        operation_kwargs[TRANSACT_ITEMS] = [
            {TRANSACT_GET: item} for item in get_items
        ]

        try:
            return await self.dispatch(TRANSACT_GET_ITEMS, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise TransactGetError("Failed to get transaction items", e) from e

    async def batch_write_item(self,
                               table_name,
                               put_items=None,
                               delete_items=None,
                               return_consumed_capacity=None,
                               return_item_collection_metrics=None):
        """
        Performs the batch_write_item operation
        """
        if put_items is None and delete_items is None:
            raise ValueError("Either put_items or delete_items must be specified")
        operation_kwargs = {
            REQUEST_ITEMS: {
                table_name: []
            }
        }
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if return_item_collection_metrics:
            operation_kwargs.update(self.get_item_collection_map(return_item_collection_metrics))
        put_items_list = []
        if put_items:
            for item in put_items:
                put_items_list.append({
                    PUT_REQUEST: await self.get_item_attribute_map(table_name, item, pythonic_key=False)
                })
        delete_items_list = []
        if delete_items:
            for item in delete_items:
                delete_items_list.append({
                    DELETE_REQUEST: await self.get_item_attribute_map(
                        table_name, item, item_key=KEY, pythonic_key=False
                    )
                })
        operation_kwargs[REQUEST_ITEMS][table_name] = delete_items_list + put_items_list
        try:
            return await self.dispatch(BATCH_WRITE_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise PutError("Failed to batch write items: {}".format(e)) from e

    async def batch_get_item(self,
                             table_name,
                             keys,
                             consistent_read=None,
                             return_consumed_capacity=None,
                             attributes_to_get=None):
        """
        Performs the batch get item operation
        """
        operation_kwargs = {
            REQUEST_ITEMS: {
                table_name: {}
            }
        }

        args_map = {}
        name_placeholders = {}
        if consistent_read:
            args_map[CONSISTENT_READ] = consistent_read
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            args_map[PROJECTION_EXPRESSION] = projection_expression
        if name_placeholders:
            args_map[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        operation_kwargs[REQUEST_ITEMS][table_name].update(args_map)

        keys_map = {KEYS: []}
        for key in keys:
            keys_map[KEYS].append(
                (await self.get_item_attribute_map(table_name, key))[ITEM]
            )
        operation_kwargs[REQUEST_ITEMS][table_name].update(keys_map)
        try:
            return await self.dispatch(BATCH_GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to batch get items: {}".format(e)) from e

    async def get_item(self,
                       table_name,
                       hash_key,
                       range_key=None,
                       consistent_read=False,
                       attributes_to_get=None):
        """
        Performs the GetItem operation and returns the result
        """
        operation_kwargs = await self.get_operation_kwargs(
            table_name=table_name,
            hash_key=hash_key,
            range_key=range_key,
            consistent_read=consistent_read,
            attributes_to_get=attributes_to_get
        )
        try:
            return await self.dispatch(GET_ITEM, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise GetError("Failed to get item: {}".format(e)) from e

    async def scan(self,
                   table_name,
                   filter_condition=None,
                   attributes_to_get=None,
                   limit=None,
                   return_consumed_capacity=None,
                   exclusive_start_key=None,
                   segment=None,
                   total_segments=None,
                   consistent_read=None,
                   index_name=None):
        """
        Performs the scan operation
        """
        self._check_condition('filter_condition', filter_condition)

        operation_kwargs = {TABLE_NAME: table_name}
        name_placeholders = {}
        expression_attribute_values = {}

        if filter_condition is not None:
            filter_expression = filter_condition.serialize(name_placeholders, expression_attribute_values)
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get is not None:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if exclusive_start_key:
            operation_kwargs.update(await self.get_exclusive_start_key_map(table_name, exclusive_start_key))
        if segment is not None:
            operation_kwargs[SEGMENT] = segment
        if total_segments:
            operation_kwargs[TOTAL_SEGMENTS] = total_segments
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = consistent_read
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return await self.dispatch(SCAN, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise ScanError("Failed to scan table: {}".format(e)) from e

    async def query(self,
                    table_name,
                    hash_key,
                    range_key_condition=None,
                    filter_condition=None,
                    attributes_to_get=None,
                    consistent_read=False,
                    exclusive_start_key=None,
                    index_name=None,
                    limit=None,
                    return_consumed_capacity=None,
                    scan_index_forward=None,
                    select=None):
        """
        Performs the Query operation and returns the result
        """
        self._check_condition('range_key_condition', range_key_condition)
        self._check_condition('filter_condition', filter_condition)

        operation_kwargs = {TABLE_NAME: table_name}
        name_placeholders = {}
        expression_attribute_values = {}

        tbl = await self.get_meta_table(table_name)
        if tbl is None:
            raise TableError("No such table: {}".format(table_name))
        if index_name:
            if not tbl.has_index_name(index_name):
                raise ValueError("Table {} has no index: {}".format(table_name, index_name))
            hash_keyname = tbl.get_index_hash_keyname(index_name)
            if not hash_keyname:
                raise ValueError("No hash key attribute for index: {}".format(index_name))
            range_keyname = tbl.get_index_range_keyname(index_name)
        else:
            hash_keyname = tbl.hash_keyname
            range_keyname = tbl.range_keyname

        hash_condition_value = {
            await self.get_attribute_type(table_name, hash_keyname, hash_key): self.parse_attribute(hash_key)
        }
        key_condition = getattr(Path([hash_keyname]), '__eq__')(hash_condition_value)

        if range_key_condition is not None:
            if range_key_condition.is_valid_range_key_condition(range_keyname):
                key_condition = key_condition & range_key_condition
            elif filter_condition is None:
                # Try to gracefully handle the case where a user passed in a filter as a range key condition
                (filter_condition, range_key_condition) = (range_key_condition, None)
            else:
                raise ValueError("{} is not a valid range key condition".format(range_key_condition))

        operation_kwargs[KEY_CONDITION_EXPRESSION] = key_condition.serialize(
            name_placeholders, expression_attribute_values)
        if filter_condition is not None:
            filter_expression = filter_condition.serialize(name_placeholders, expression_attribute_values)
            # FilterExpression does not allow key attributes. Check for hash and range key name placeholders
            hash_key_placeholder = name_placeholders.get(hash_keyname)
            range_key_placeholder = range_keyname and name_placeholders.get(range_keyname)
            if (
                hash_key_placeholder in filter_expression or
                (range_key_placeholder and range_key_placeholder in filter_expression)
            ):
                raise ValueError("'filter_condition' cannot contain key attributes")
            operation_kwargs[FILTER_EXPRESSION] = filter_expression
        if attributes_to_get:
            projection_expression = create_projection_expression(attributes_to_get, name_placeholders)
            operation_kwargs[PROJECTION_EXPRESSION] = projection_expression
        if consistent_read:
            operation_kwargs[CONSISTENT_READ] = True
        if exclusive_start_key:
            operation_kwargs.update(await self.get_exclusive_start_key_map(table_name, exclusive_start_key))
        if index_name:
            operation_kwargs[INDEX_NAME] = index_name
        if limit is not None:
            operation_kwargs[LIMIT] = limit
        if return_consumed_capacity:
            operation_kwargs.update(self.get_consumed_capacity_map(return_consumed_capacity))
        if select:
            if select.upper() not in SELECT_VALUES:
                raise ValueError("{} must be one of {}".format(SELECT, SELECT_VALUES))
            operation_kwargs[SELECT] = str(select).upper()
        if scan_index_forward is not None:
            operation_kwargs[SCAN_INDEX_FORWARD] = scan_index_forward
        if name_placeholders:
            operation_kwargs[EXPRESSION_ATTRIBUTE_NAMES] = self._reverse_dict(name_placeholders)
        if expression_attribute_values:
            operation_kwargs[EXPRESSION_ATTRIBUTE_VALUES] = expression_attribute_values

        try:
            return await  self.dispatch(QUERY, operation_kwargs)
        except BOTOCORE_EXCEPTIONS as e:
            raise QueryError("Failed to query items: {}".format(e)) from e
