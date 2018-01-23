import asyncio
import copy
import inspect
import json
import logging
import warnings

import six
import time
from pynamodb.attributes import AttributeContainerMeta, Attribute
from pynamodb.connection.base import MetaTable
from pynamodb.connection.util import pythonic
from pynamodb.exceptions import TableDoesNotExist, DoesNotExist, TableError
from pynamodb.indexes import Index
from pynamodb.models import Model as PynamoDBModel, DefaultMeta
from pynamodb.types import RANGE, HASH

from inpynamodb.pagination import ResultIterator
from pynamodb.compat import NullHandler

from inpynamodb.connection.table import TableConnection
from inpynamodb.constants import PUT_FILTER_OPERATOR_MAP, READ_CAPACITY_UNITS, WRITE_CAPACITY_UNITS, \
    STREAM_VIEW_TYPE, STREAM_SPECIFICATION, STREAM_ENABLED, GLOBAL_SECONDARY_INDEXES, LOCAL_SECONDARY_INDEXES, \
    ATTR_DEFINITIONS, ATTR_NAME, QUERY_OPERATOR_MAP, QUERY_FILTER_OPERATOR_MAP, META_CLASS_NAME, REGION, HOST, \
    RETURN_VALUES, ALL_NEW, ATTR_UPDATES, RANGE_KEY, UPDATE_FILTER_OPERATOR_MAP, ACTION, VALUE, ATTRIBUTES, \
    ATTR_TYPE_MAP, SCAN_OPERATOR_MAP, DELETE_FILTER_OPERATOR_MAP, ITEM_COUNT, COUNT, BATCH_WRITE_PAGE_LIMIT, PUT, \
    DELETE, UNPROCESSED_ITEMS, PUT_REQUEST, ITEM, DELETE_REQUEST, KEY, BATCH_GET_PAGE_LIMIT, RESPONSES, \
    UNPROCESSED_KEYS, KEYS, TABLE_STATUS, ACTIVE
from inpynamodb.settings import get_settings_value

log = logging.getLogger(__name__)
log.addHandler(NullHandler())


class InvalidUsageException(Exception):
    pass


class ModelContextManager(object):
    """
    A class for managing batch operations

    """

    def __init__(self, model, auto_commit=True):
        self.model = model
        self.auto_commit = auto_commit
        self.max_operations = BATCH_WRITE_PAGE_LIMIT
        self.pending_operations = []


class BatchWrite(ModelContextManager):
    """
    A class for batch writes
    """
    async def save(self, put_item):
        """
        This adds `put_item` to the list of pending writes to be performed.
        Additionally, the a BatchWriteItem will be performed if the length of items
        reaches 25.

        :param put_item: Should be an instance of a `Model` to be written
        """
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                await self.commit()
        self.pending_operations.append({"action": PUT, "item": put_item})

    async def delete(self, del_item):
        """
        This adds `del_item` to the list of pending deletes to be performed.
        If the list of items reaches 25, a BatchWriteItem will be called.

        :param del_item: Should be an instance of a `Model` to be deleted
        """
        if len(self.pending_operations) == self.max_operations:
            if not self.auto_commit:
                raise ValueError("DynamoDB allows a maximum of 25 batch operations")
            else:
                await self.commit()
        self.pending_operations.append({"action": DELETE, "item": del_item})

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        This ensures that all pending operations are committed when
        the context is exited
        """
        return await self.commit()

    async def commit(self):
        """
        Writes all of the changes that are pending
        """
        log.debug("%s committing batch operation", self.model)
        put_items = []
        delete_items = []
        attrs_name = pythonic(ATTRIBUTES)
        for item in self.pending_operations:
            if item['action'] == PUT:
                put_items.append(item['item']._serialize(attr_map=True)[attrs_name])
            elif item['action'] == DELETE:
                delete_items.append(item['item']._get_keys())
        self.pending_operations = []
        if not len(put_items) and not len(delete_items):
            return
        data = await self.model._get_connection().batch_write_item(
            put_items=put_items,
            delete_items=delete_items
        )
        if data is None:
            return
        unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)
        while unprocessed_items:
            put_items = []
            delete_items = []
            for item in unprocessed_items:
                if PUT_REQUEST in item:
                    put_items.append(item.get(PUT_REQUEST).get(ITEM))
                elif DELETE_REQUEST in item:
                    delete_items.append(item.get(DELETE_REQUEST).get(KEY))
            log.info("Resending %s unprocessed keys for batch operation", len(unprocessed_items))
            data = await self.model._get_connection().batch_write_item(
                put_items=put_items,
                delete_items=delete_items
            )
            unprocessed_items = data.get(UNPROCESSED_ITEMS, {}).get(self.model.Meta.table_name)


class MetaModel(AttributeContainerMeta):
    """
       Model meta class

       This class is just here so that index queries have nice syntax.
       Model.index.query()
       """

    def __init__(cls, name, bases, attrs):
        super(MetaModel, cls).__init__(name, bases, attrs)
        if isinstance(attrs, dict):
            for attr_name, attr_obj in attrs.items():
                if attr_name == META_CLASS_NAME:
                    if not hasattr(attr_obj, REGION):
                        setattr(attr_obj, REGION, get_settings_value('region'))
                    if not hasattr(attr_obj, HOST):
                        setattr(attr_obj, HOST, get_settings_value('host'))
                    if not hasattr(attr_obj, 'session_cls'):
                        setattr(attr_obj, 'session_cls', get_settings_value('session_cls'))
                    if not hasattr(attr_obj, 'request_timeout_seconds'):
                        setattr(attr_obj, 'request_timeout_seconds', get_settings_value('request_timeout_seconds'))
                    if not hasattr(attr_obj, 'base_backoff_ms'):
                        setattr(attr_obj, 'base_backoff_ms', get_settings_value('base_backoff_ms'))
                    if not hasattr(attr_obj, 'max_retry_attempts'):
                        setattr(attr_obj, 'max_retry_attempts', get_settings_value('max_retry_attempts'))
                elif issubclass(attr_obj.__class__, (Index,)):
                    attr_obj.Meta.model = cls
                    if not hasattr(attr_obj.Meta, "index_name"):
                        attr_obj.Meta.index_name = attr_name
                elif issubclass(attr_obj.__class__, (Attribute,)):
                    if attr_obj.attr_name is None:
                        attr_obj.attr_name = attr_name

            if META_CLASS_NAME not in attrs:
                setattr(cls, META_CLASS_NAME, DefaultMeta)

            # create a custom Model.DoesNotExist derived from pynamodb.exceptions.DoesNotExist,
            # so that "except Model.DoesNotExist:" would not catch other models' exceptions
            if 'DoesNotExist' not in attrs:
                exception_attrs = {'__module__': attrs.get('__module__')}
                if hasattr(cls, '__qualname__'):  # On Python 3, Model.DoesNotExist
                    exception_attrs['__qualname__'] = '{}.{}'.format(cls.__qualname__, 'DoesNotExist')
                cls.DoesNotExist = type('DoesNotExist', (DoesNotExist,), exception_attrs)


class Model(PynamoDBModel):
    def __init__(self, **attributes):
        if 'cls' not in inspect.stack()[1][0].f_locals or \
                inspect.stack()[1][0].f_locals['cls'].__class__.__name__ != 'MetaModel' or \
                inspect.stack()[1][3] != "create":
            raise InvalidUsageException("You should declare a model with create() factory method.")

        super().__init__(**attributes)

    def __repr__(self):
        """
        Temporary method since __repr__ does not support async
        """
        if self.Meta.table_name:
            serialized = self._serialize(null_check=False)

            if serialized.get(RANGE):
                msg = "{0}<{1}, {2}>".format(self.Meta.table_name, serialized.get(HASH), serialized.get(RANGE))
            else:
                msg = "{0}<{1}".format(self.Meta.table_name, serialized.get(HASH))

            return six.u(msg)

    @classmethod
    async def create(cls, hash_key=None, range_key=None, **attributes):
        if hash_key is not None:
            attributes[cls._dynamo_to_python_attr((await cls._get_meta_data()).hash_keyname)] = hash_key
        if range_key is not None:
            range_keyname = (await cls._get_meta_data()).range_keyname
            if range_keyname is None:
                raise ValueError(
                    "This table has no range key, but a range key value was provided: {0}".format(range_key)
                )
            attributes[cls._dynamo_to_python_attr(range_keyname)] = range_key

        return cls(**attributes)

    @classmethod
    async def delete_table(cls):
        """
        Delete the table for this model
        """
        return await cls._get_connection().delete_table()

    @classmethod
    async def describe_table(cls):
        """
        Returns the result of a DescribeTable operation on this model's table
        """
        return await cls._get_connection().describe_table()

    @classmethod
    async def create_table(cls, wait=False, read_capacity_units=None, write_capacity_units=None):
        """
        :param wait: If set, then this call will block until the table is ready for use
        :param read_capacity_units: Sets the read capacity units for this table
        :param write_capacity_units: Sets the write capacity units for this table
        """
        if not await cls.exists():
            schema = cls._get_schema()
            if hasattr(cls.Meta, pythonic(READ_CAPACITY_UNITS)):
                schema[pythonic(READ_CAPACITY_UNITS)] = cls.Meta.read_capacity_units
            if hasattr(cls.Meta, pythonic(WRITE_CAPACITY_UNITS)):
                schema[pythonic(WRITE_CAPACITY_UNITS)] = cls.Meta.write_capacity_units
            if hasattr(cls.Meta, pythonic(STREAM_VIEW_TYPE)):
                schema[pythonic(STREAM_SPECIFICATION)] = {
                    pythonic(STREAM_ENABLED): True,
                    pythonic(STREAM_VIEW_TYPE): cls.Meta.stream_view_type
                }
            if read_capacity_units is not None:
                schema[pythonic(READ_CAPACITY_UNITS)] = read_capacity_units
            if write_capacity_units is not None:
                schema[pythonic(WRITE_CAPACITY_UNITS)] = write_capacity_units
            index_data = cls._get_indexes()
            schema[pythonic(GLOBAL_SECONDARY_INDEXES)] = index_data.get(pythonic(GLOBAL_SECONDARY_INDEXES))
            schema[pythonic(LOCAL_SECONDARY_INDEXES)] = index_data.get(pythonic(LOCAL_SECONDARY_INDEXES))
            index_attrs = index_data.get(pythonic(ATTR_DEFINITIONS))
            attr_keys = [attr.get(pythonic(ATTR_NAME)) for attr in schema.get(pythonic(ATTR_DEFINITIONS))]
            for attr in index_attrs:
                attr_name = attr.get(pythonic(ATTR_NAME))
                if attr_name not in attr_keys:
                    schema[pythonic(ATTR_DEFINITIONS)].append(attr)
                    attr_keys.append(attr_name)
            cls._get_connection().create_table(
                **schema
            )

            if wait:
                while True:
                    status = await cls._get_connection().describe_table()
                    if status:
                        data = status.get(TABLE_STATUS)
                        if data == ACTIVE:
                            return
                        else:
                            await asyncio.sleep(2)
                    else:
                        raise TableError("No TableStatus returned for table")

    @classmethod
    async def loads(cls, data):
        content = json.loads(data)
        async with cls.batch_write() as batch:
            for item_data in content:
                item = cls._from_data(item_data)
                await batch.save(item)

    async def update_item(self, attribute, value=None, action=None, condition=None, conditional_operator=None,
                          **expected_values):
        """
        Updates an item using the UpdateItem operation.

        This should be used for updating a single attribute of an item.

        :param attribute: The name of the attribute to be updated
        :param value: The new value for the attribute.
        :param action: The action to take if this item already exists.
            See: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html#DDB-UpdateItem-request-AttributeUpdate
        """
        warnings.warn("`Model.update_item` is deprecated in favour of `Model.update` now")

        self._conditional_operator_check(conditional_operator)
        args, save_kwargs = self._get_save_args(null_check=False)
        attribute_cls = None
        for attr_name, attr_cls in self._get_attributes().items():
            if attr_name == attribute:
                attribute_cls = attr_cls
                break
        if not attribute_cls:
            raise ValueError("Attribute {0} specified does not exist".format(attr_name))
        if save_kwargs.get(pythonic(RANGE_KEY)):
            kwargs = {pythonic(RANGE_KEY): save_kwargs.get(pythonic(RANGE_KEY))}
        else:
            kwargs = {}
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, UPDATE_FILTER_OPERATOR_MAP))
        kwargs[pythonic(ATTR_UPDATES)] = {
            attribute_cls.attr_name: {
                ACTION: action.upper() if action else None,
            }
        }
        if value is not None:
            kwargs[pythonic(ATTR_UPDATES)][attribute_cls.attr_name][VALUE] = {
                ATTR_TYPE_MAP[attribute_cls.attr_type]: attribute_cls.serialize(value)
            }
        kwargs[pythonic(RETURN_VALUES)] = ALL_NEW
        kwargs.update(conditional_operator=conditional_operator)
        kwargs.update(condition=condition)
        data = await self._get_connection().update_item(
            *args,
            **kwargs
        )

        for name, value in data.get(ATTRIBUTES).items():
            attr_name = self._dynamo_to_python_attr(name)
            attr = self._get_attributes().get(attr_name)
            if attr:
                setattr(self, attr_name, attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type])))
        return data

    async def update(self, attributes=None, actions=None, condition=None, conditional_operator=None, **expected_values):
        """
        Updates an item using the UpdateItem operation.

        :param attributes: A dictionary of attributes to update in the following format
                            {
                                attr_name: {'value': 10, 'action': 'ADD'},
                                next_attr: {'value': True, 'action': 'PUT'},
                            }
        """
        if attributes is not None and not isinstance(attributes, dict):
            raise TypeError("the value of `attributes` is expected to be a dictionary")
        if actions is not None and not isinstance(actions, list):
            raise TypeError("the value of `actions` is expected to be a list")

        self._conditional_operator_check(conditional_operator)
        args, save_kwargs = self._get_save_args(null_check=False)
        kwargs = {
            pythonic(RETURN_VALUES): ALL_NEW,
            'conditional_operator': conditional_operator,
        }

        if attributes:
            kwargs[pythonic(ATTR_UPDATES)] = {}

        if pythonic(RANGE_KEY) in save_kwargs:
            kwargs[pythonic(RANGE_KEY)] = save_kwargs[pythonic(RANGE_KEY)]

        if expected_values:
            kwargs['expected'] = self._build_expected_values(expected_values, UPDATE_FILTER_OPERATOR_MAP)

        attrs = self._get_attributes()
        attributes = attributes or {}
        for attr, params in attributes.items():
            attribute_cls = attrs[attr]
            action = params['action'] and params['action'].upper()
            attr_values = {ACTION: action}
            if 'value' in params:
                attr_values[VALUE] = self._serialize_value(attribute_cls, params['value'])

            kwargs[pythonic(ATTR_UPDATES)][attribute_cls.attr_name] = attr_values

        kwargs.update(condition=condition)
        kwargs.update(actions=actions)
        data = await self._get_connection().update_item(*args, **kwargs)
        for name, value in data[ATTRIBUTES].items():
            attr_name = self._dynamo_to_python_attr(name)
            attr = self._get_attributes().get(attr_name)
            if attr:
                setattr(self, attr_name, attr.deserialize(value.get(ATTR_TYPE_MAP[attr.attr_type])))
        return data

    @classmethod
    async def from_raw_data(cls, data):
        """
        Returns an instance of this class
        from the raw data

        :param data: A serialized DynamoDB object
        """
        mutable_data = copy.copy(data)
        if mutable_data is None:
            raise ValueError("Received no mutable_data to construct object")
        meta_data = await cls._get_meta_data()
        hash_keyname = meta_data.hash_keyname
        range_keyname = meta_data.range_keyname
        hash_key_type = meta_data.get_attribute_type(hash_keyname)
        hash_key = mutable_data.pop(hash_keyname).get(hash_key_type)

        hash_key_attr = cls._get_attributes().get(cls._dynamo_to_python_attr(hash_keyname))

        hash_key = hash_key_attr.deserialize(hash_key)
        args = (hash_key,)
        kwargs = {}
        if range_keyname:
            range_key_attr = cls._get_attributes().get(cls._dynamo_to_python_attr(range_keyname))
            range_key_type = meta_data.get_attribute_type(range_keyname)
            range_key = mutable_data.pop(range_keyname).get(range_key_type)
            kwargs['range_key'] = range_key_attr.deserialize(range_key)
        for name, value in mutable_data.items():
            attr_name = cls._dynamo_to_python_attr(name)
            attr = cls._get_attributes().get(attr_name, None)
            if attr:
                kwargs[attr_name] = attr.deserialize(attr.get_value(value))
        return await cls.create(*args, **kwargs)

    @classmethod
    async def count(cls,
                    hash_key=None,
                    range_key_condition=None,
                    filter_condition=None,
                    consistent_read=False,
                    index_name=None,
                    limit=None,
                    **filters):
        """
        Provides a filtered count

        :param hash_key: The hash key to query. Can be None.
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param filters: A dictionary of filters to be used in the query. Requires a hash_key to be passed.
        """
        if hash_key is None:
            if filters:
                raise ValueError('A hash_key must be given to use filters')
            return (await cls.describe_table()).get(ITEM_COUNT)

        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
            key_attribute_classes = cls._index_classes[index_name]._get_attributes()
            non_key_attribute_classes = cls._get_attributes()
        else:
            hash_key = await cls._serialize_keys(hash_key)[0]
            non_key_attribute_classes = dict(cls._get_attributes())
            key_attribute_classes = dict(cls._get_attributes())
            for name, attr in cls._get_attributes().items():
                if attr.is_range_key or attr.is_hash_key:
                    key_attribute_classes[name] = attr
                else:
                    non_key_attribute_classes[name] = attr
        key_conditions, query_filters = cls._build_filters(
            QUERY_OPERATOR_MAP,
            non_key_operator_map=QUERY_FILTER_OPERATOR_MAP,
            key_attribute_classes=key_attribute_classes,
            non_key_attribute_classes=non_key_attribute_classes,
            filters=filters)

        query_args = (hash_key,)
        query_kwargs = dict(
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            consistent_read=consistent_read,
            key_conditions=key_conditions,
            query_filters=query_filters,
            limit=limit,
            select=COUNT
        )

        result_iterator = ResultIterator(
            cls._get_connection().query,
            query_args,
            query_kwargs,
            limit=limit
        )

        # iterate through results
        list(i async for i in result_iterator)

        return result_iterator.total_count

    @classmethod
    async def query(cls,
                    hash_key,
                    range_key_condition=None,
                    filter_condition=None,
                    consistent_read=False,
                    index_name=None,
                    scan_index_forward=None,
                    conditional_operator=None,
                    limit=None,
                    last_evaluated_key=None,
                    attributes_to_get=None,
                    page_size=None,
                    **filters):
        """
        Provides a high level query API

        :param hash_key: The hash key to query
        :param range_key_condition: Condition for range key
        :param filter_condition: Condition used to restrict the query results
        :param consistent_read: If True, a consistent read is performed
        :param index_name: If set, then this index is used
        :param limit: Used to limit the number of results returned
        :param scan_index_forward: If set, then used to specify the same parameter to the DynamoDB API.
            Controls descending or ascending results
        :param conditional_operator:
        :param last_evaluated_key: If set, provides the starting point for query.
        :param attributes_to_get: If set, only returns these elements
        :param page_size: Page size of the query to DynamoDB
        :param filters: A dictionary of filters to be used in the query
        """
        cls._conditional_operator_check(conditional_operator)
        cls._get_indexes()
        if index_name:
            hash_key = cls._index_classes[index_name]._hash_key_attribute().serialize(hash_key)
            key_attribute_classes = cls._index_classes[index_name]._get_attributes()
            non_key_attribute_classes = cls._get_attributes()
        else:
            hash_key = (await cls._serialize_keys(hash_key))[0]
            non_key_attribute_classes = {}
            key_attribute_classes = {}
            for name, attr in cls._get_attributes().items():
                if attr.is_range_key or attr.is_hash_key:
                    key_attribute_classes[name] = attr
                else:
                    non_key_attribute_classes[name] = attr

        if page_size is None:
            page_size = limit

        key_conditions, query_filters = cls._build_filters(
            QUERY_OPERATOR_MAP,
            non_key_operator_map=QUERY_FILTER_OPERATOR_MAP,
            key_attribute_classes=key_attribute_classes,
            non_key_attribute_classes=non_key_attribute_classes,
            filters=filters)

        query_args = (hash_key,)
        query_kwargs = dict(
            range_key_condition=range_key_condition,
            filter_condition=filter_condition,
            index_name=index_name,
            exclusive_start_key=last_evaluated_key,
            consistent_read=consistent_read,
            scan_index_forward=scan_index_forward,
            limit=page_size,
            key_conditions=key_conditions,
            attributes_to_get=attributes_to_get,
            query_filters=query_filters,
            conditional_operator=conditional_operator
        )

        iterator = ResultIterator(
            cls._get_connection().query,
            query_args,
            query_kwargs,
            map_fn=cls.from_raw_data,
            limit=limit
        )

        return iterator

    @classmethod
    def scan(cls,
             filter_condition=None,
             segment=None,
             total_segments=None,
             limit=None,
             conditional_operator=None,
             last_evaluated_key=None,
             page_size=None,
             consistent_read=None,
             **filters):
        """
        Iterates through all items in the table

        :param filter_condition: Condition used to restrict the scan results
        :param segment: If set, then scans the segment
        :param total_segments: If set, then specifies total segments
        :param limit: Used to limit the number of results returned
        :param conditional_operator:
        :param last_evaluated_key: If set, provides the starting point for scan.
        :param page_size: Page size of the scan to DynamoDB
        :param filters: A list of item filters
        :param consistent_read: If True, a consistent read is performed
        """
        cls._conditional_operator_check(conditional_operator)
        key_filter, scan_filter = cls._build_filters(
            SCAN_OPERATOR_MAP,
            non_key_operator_map=SCAN_OPERATOR_MAP,
            key_attribute_classes=cls._get_attributes(),
            filters=filters
        )
        key_filter.update(scan_filter)

        if page_size is None:
            page_size = limit

        scan_args = ()
        scan_kwargs = dict(
            filter_condition=filter_condition,
            exclusive_start_key=last_evaluated_key,
            segment=segment,
            limit=page_size,
            scan_filter=key_filter,
            total_segments=total_segments,
            conditional_operator=conditional_operator,
            consistent_read=consistent_read
        )

        return ResultIterator(
            cls._get_connection().scan,
            scan_args,
            scan_kwargs,
            map_fn=cls.from_raw_data,
            limit=limit
        )

    @classmethod
    async def batch_get(cls, items, consistent_Read=None, attributes_to_get=None):
        """
        BatchGetItem for this model

        :param items: Should be a list of hash keys to retrieve, or a list of
            tuples if range keys are used.
        """
        items = list(items)
        hash_keyname = (await cls._get_meta_data()).hash_keyname
        range_keyname = (await cls._get_meta_data()).range_keyname
        keys_to_get = []
        while items:
            if len(keys_to_get) == BATCH_GET_PAGE_LIMIT:
                while keys_to_get:
                    page, unprocessed_keys = cls._batch_get_page(
                        keys_to_get,
                        consistent_read=consistent_Read,
                        attributes_to_get=attributes_to_get
                    )
                    for batch_item in page:
                        yield await cls.from_raw_data(batch_item)
                    if unprocessed_keys:
                        keys_to_get = unprocessed_keys
                    else:
                        keys_to_get = []
            item = items.pop()
            if range_keyname:
                hash_key, range_key = await cls._serialize_keys(item[0], item[1])
                keys_to_get.append({
                    hash_keyname: hash_key,
                    range_keyname: range_key
                })
            else:
                hash_key = await cls._serialize_keys(item)[0]
                keys_to_get.append({
                    hash_keyname: hash_key
                })

        while keys_to_get:
            page, unprocessed_keys = cls._batch_get_page(
                keys_to_get,
                consistent_read=consistent_Read,
                attributes_to_get=attributes_to_get
            )
            for batch_item in page:
                yield cls.from_raw_data(batch_item)
            if unprocessed_keys:
                keys_to_get = unprocessed_keys
            else:
                keys_to_get = []


    @classmethod
    def batch_write(cls, auto_commit=True):
        """
        Returns a context manager for a batch operation'

        :param auto_commit: Commits writes automatically if `True`
        """
        return BatchWrite(cls, auto_commit=auto_commit)

    async def delete(self, condition=None, conditional_operator=None, **expected_values):
        """
        Deletes this object from dynamodb
        """
        self._conditional_operator_check(conditional_operator)
        args, kwargs = self._get_save_args(attributes=False, null_check=False)
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, DELETE_FILTER_OPERATOR_MAP))
        kwargs.update(conditional_operator=conditional_operator)
        kwargs.update(condition=condition)
        return await self._get_connection().delete_item(*args, **kwargs)

    @classmethod
    async def exists(cls):
        """
        Returns True if this table exists, False otherwise
        """
        try:
            await cls._get_connection().describe_table()
            return True
        except TableDoesNotExist:
            return False

    async def save(self, condition=None, conditional_operator=None, **expected_values):
        """
        Save this object to dynamodb
        """
        self._conditional_operator_check(conditional_operator)
        args, kwargs = self._get_save_args()
        if len(expected_values):
            kwargs.update(expected=self._build_expected_values(expected_values, PUT_FILTER_OPERATOR_MAP))
        kwargs.update(conditional_operator=conditional_operator)
        kwargs.update(condition=condition)

        return await self._get_connection().put_item(*args, **kwargs)

    @classmethod
    async def _range_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls._get_attributes()
        range_keyname = (await cls._get_meta_data()).range_keyname
        if range_keyname:
            attr = attributes[cls._dynamo_to_python_attr(range_keyname)]
        else:
            attr = None
        return attr

    @classmethod
    async def _hash_key_attribute(cls):
        """
        Returns the attribute class for the hash key
        """
        attributes = cls._get_attributes()
        hash_keyname = (await cls._get_meta_data()).hash_keyname
        return attributes[cls._dynamo_to_python_attr(hash_keyname)]

    @classmethod
    async def _batch_get_page(cls, keys_to_get, consistent_read, attributes_to_get):
        """
        Returns a single page from BatchGetItem
        Also returns any unprocessed items

        :param keys_to_get: A list of keys
        :param consistent_read: Whether or not this needs to be consistent
        :param attributes_to_get: A list of attributes to return
        """
        log.debug("Fetching a BatchGetItem page")
        data = await cls._get_connection().batch_get_item(
            keys_to_get, consistent_read=consistent_read, attributes_to_get=attributes_to_get
        )
        item_data = data.get(RESPONSES).get(cls.Meta.table_name)
        unprocessed_items = data.get(UNPROCESSED_KEYS).get(cls.Meta.table_name, {}).get(KEYS, None)
        return item_data, unprocessed_items

    @classmethod
    async def _get_meta_data(cls):
        """
        A helper object that contains meta data about this table
        """
        if cls._meta_table is None:
            cls._meta_table = MetaTable(await cls._get_connection().describe_table())
        return cls._meta_table

    @classmethod
    def _get_connection(cls):
        """
        Returns a (cached) connection
        """
        if not hasattr(cls, "Meta") or cls.Meta.table_name is None:
            raise AttributeError(
                """As of v1.0 PynamoDB Models require a `Meta` class.
                See https://pynamodb.readthedocs.io/en/latest/release_notes.html"""
            )
        if cls._connection is None:
            cls._connection = TableConnection(cls.Meta.table_name,
                                              region=cls.Meta.region,
                                              host=cls.Meta.host,
                                              session_cls=cls.Meta.session_cls,
                                              request_timeout_seconds=cls.Meta.request_timeout_seconds,
                                              max_retry_attempts=cls.Meta.max_retry_attempts,
                                              base_backoff_ms=cls.Meta.base_backoff_ms)
        return cls._connection

    @classmethod
    async def _serialize_keys(cls, hash_key, range_key=None):
        """
        Serializes the hash and range keys

        :param hash_key: The hash key value
        :param range_key: The range key value
        """
        hash_key = (await cls._hash_key_attribute()).serialize(hash_key)
        if range_key is not None:
            range_key = (await cls._range_key_attribute()).serialize(range_key)
        return hash_key, range_key
