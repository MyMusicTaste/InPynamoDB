"""
Test model API
"""
import base64
import copy
import json
import random
from datetime import datetime

import pytest as pytest
import six
from aiobotocore import AioSession
from asynctest import patch, TestCase, CoroutineMock, fail_on, MagicMock
from botocore.exceptions import ClientError
from pynamodb.connection.util import pythonic
from pynamodb.types import RANGE

from inpynamodb.attributes import MapAttribute, UnicodeAttribute, UTCDateTimeAttribute, NumberSetAttribute, UnicodeSetAttribute, \
    BinarySetAttribute, BooleanAttribute, NumberAttribute, BinaryAttribute, ListAttribute
from inpynamodb.indexes import AllProjection, IncludeProjection, KeysOnlyProjection, Index

from inpynamodb.constants import ITEM, STRING_SHORT, ATTRIBUTES, REQUEST_ITEMS, KEYS, UNPROCESSED_KEYS, RESPONSES, \
    BINARY_SHORT, DEFAULT_ENCODING, UNPROCESSED_ITEMS, ALL, KEYS_ONLY, INCLUDE, MAP_SHORT, LIST_SHORT, NUMBER_SHORT, \
    EXCLUSIVE_START_KEY, CAMEL_COUNT, ITEMS, LAST_EVALUATED_KEY
from inpynamodb.indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from inpynamodb.models import Model, ResultSet
from inpynamodb.tests.pynamodb_tests.data import MODEL_TABLE_DATA, SIMPLE_MODEL_TABLE_DATA, \
    CUSTOM_ATTR_NAME_INDEX_TABLE_DATA, GET_MODEL_ITEM_DATA, COMPLEX_TABLE_DATA, COMPLEX_ITEM_DATA, CAR_MODEL_TABLE_DATA, \
    SIMPLE_BATCH_GET_ITEMS, BATCH_GET_ITEMS, INDEX_TABLE_DATA, LOCAL_INDEX_TABLE_DATA, BINARY_ATTR_DATA, \
    SERIALIZED_TABLE_DATA, COMPLEX_MODEL_SERIALIZED_TABLE_DATA, COMPLEX_MODEL_ITEM_DATA, \
    OFFICE_EMPLOYEE_MODEL_TABLE_DATA, GROCERY_LIST_MODEL_TABLE_DATA, OFFICE_MODEL_TABLE_DATA, \
    CAR_MODEL_WITH_NULL_ITEM_DATA, INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA, FULL_CAR_MODEL_ITEM_DATA, \
    GET_OFFICE_EMPLOYEE_ITEM_DATA, GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL, GET_GROCERY_LIST_ITEM_DATA, \
    GET_OFFICE_ITEM_DATA, COMPLEX_MODEL_TABLE_DATA, BOOLEAN_CONVERSION_MODEL_TABLE_DATA, \
    BOOLEAN_CONVERSION_MODEL_TABLE_DATA_OLD_STYLE, BOOLEAN_CONVERSION_MODEL_OLD_STYLE_FALSE_ITEM_DATA, \
    BOOLEAN_CONVERSION_MODEL_OLD_STYLE_TRUE_ITEM_DATA, BOOLEAN_CONVERSION_MODEL_NEW_STYLE_FALSE_ITEM_DATA, \
    BOOLEAN_CONVERSION_MODEL_NEW_STYLE_TRUE_ITEM_DATA, TREE_MODEL_ITEM_DATA, TREE_MODEL_TABLE_DATA, \
    EXPLICIT_RAW_MAP_MODEL_TABLE_DATA, EXPLICIT_RAW_MAP_MODEL_ITEM_DATA, \
    EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_TABLE_DATA, EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_ITEM_DATA, \
    DOG_TABLE_DATA
from inpynamodb.tests.pynamodb_tests.deep_eq import deep_eq

PATCH_METHOD = 'aiobotocore.client.AioBaseClient._make_api_call'


class GamePlayerOpponentIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GamePlayerOpponentIndex"
        host = "http://localhost:8000"
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'
        projection = AllProjection()

    player_id = UnicodeAttribute(hash_key=True)
    winner_id = UnicodeAttribute(range_key=True)


class GameOpponentTimeIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameOpponentTimeIndex"
        host = "http://localhost:8000"
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'
        projection = AllProjection()

    winner_id = UnicodeAttribute(hash_key=True)
    created_time = UnicodeAttribute(range_key=True)


class GameModel(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameModel"
        host = "http://localhost:8000"
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    player_id = UnicodeAttribute(hash_key=True)
    created_time = UTCDateTimeAttribute(range_key=True)
    winner_id = UnicodeAttribute()
    loser_id = UnicodeAttribute(null=True)

    player_opponent_index = GamePlayerOpponentIndex()
    opponent_time_index = GameOpponentTimeIndex()


class OldStyleModel(Model):
    _table_name = 'IndexedModel'
    user_name = UnicodeAttribute(hash_key=True)


class EmailIndex(GlobalSecondaryIndex):
    """
    A global secondary index for email addresses
    """

    class Meta:
        index_name = 'custom_idx_name'
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()

    email = UnicodeAttribute(hash_key=True)
    alt_numbers = NumberSetAttribute(range_key=True, attr_name='numbers')


class LocalEmailIndex(LocalSecondaryIndex):
    """
    A global secondary index for email addresses
    """

    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()

    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class NonKeyAttrIndex(LocalSecondaryIndex):
    class Meta:
        index_name = "non_key_idx"
        read_capacity_units = 2
        write_capacity_units = 1
        projection = IncludeProjection(non_attr_keys=['numbers'])

    email = UnicodeAttribute(hash_key=True)
    numbers = NumberSetAttribute(range_key=True)


class IndexedModel(Model):
    """
    A model with an index
    """

    class Meta:
        table_name = 'IndexedModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    email_index = EmailIndex()
    include_index = NonKeyAttrIndex()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


class LocalIndexedModel(Model):
    """
    A model with an index
    """

    class Meta:
        table_name = 'LocalIndexedModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    email_index = LocalEmailIndex()
    numbers = NumberSetAttribute()
    aliases = UnicodeSetAttribute()
    icons = BinarySetAttribute()


class SimpleUserModel(Model):
    """
    A hash key only model
    """

    class Meta:
        table_name = 'SimpleModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute()
    numbers = NumberSetAttribute()
    custom_aliases = UnicodeSetAttribute(attr_name='aliases')
    icons = BinarySetAttribute()
    views = NumberAttribute(null=True)
    is_active = BooleanAttribute(null=True)
    signature = UnicodeAttribute(null=True)


class CustomAttrIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()

    overidden_uid = UnicodeAttribute(hash_key=True, attr_name='user_id')


class CustomAttrNameModel(Model):
    """
    A testing model
    """

    class Meta:
        table_name = 'CustomAttrModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    overidden_user_name = UnicodeAttribute(hash_key=True, attr_name='user_name')
    overidden_user_id = UnicodeAttribute(range_key=True, attr_name='user_id')
    overidden_attr = UnicodeAttribute(attr_name='foo_attr', null=True)
    uid_index = CustomAttrIndex()


class UserModel(Model):
    """
    A testing model
    """

    class Meta:
        table_name = 'UserModel'
        read_capacity_units = 25
        write_capacity_units = 25
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    custom_user_name = UnicodeAttribute(hash_key=True, attr_name='user_name')
    user_id = UnicodeAttribute(range_key=True)
    picture = BinaryAttribute(null=True)
    zip_code = NumberAttribute(null=True)
    email = UnicodeAttribute(default='needs_email')
    callable_field = NumberAttribute(default=lambda: 42)


class HostSpecificModel(Model):
    """
    A testing model
    """

    class Meta:
        host = 'http://localhost'
        table_name = 'RegionSpecificModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class RegionSpecificModel(Model):
    """
    A testing model
    """

    class Meta:
        region = 'us-west-1'
        table_name = 'RegionSpecificModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class ComplexKeyModel(Model):
    """
    This model has a key that must be serialized/deserialized properly
    """

    class Meta:
        table_name = 'ComplexKey'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    name = UnicodeAttribute(hash_key=True)
    date_created = UTCDateTimeAttribute(default=datetime.utcnow)


class Location(MapAttribute):
    lat = NumberAttribute(attr_name='latitude')
    lng = NumberAttribute(attr_name='longitude')
    name = UnicodeAttribute()


class Person(MapAttribute):
    fname = UnicodeAttribute(attr_name='firstName')
    lname = UnicodeAttribute(null=True)
    age = NumberAttribute(null=True)
    is_male = BooleanAttribute(attr_name='is_dude')

    def foo(self):
        return 1


class ComplexModel(Model):
    class Meta:
        table_name = 'ComplexModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    person = Person(attr_name='weird_person')
    key = NumberAttribute(hash_key=True)


class OfficeEmployee(Model):
    class Meta:
        table_name = 'OfficeEmployeeModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    office_employee_id = NumberAttribute(hash_key=True)
    person = Person()
    office_location = Location()

    def foo(self):
        return 1


class CarInfoMap(MapAttribute):
    make = UnicodeAttribute(null=False)
    model = UnicodeAttribute(null=True)


class CarModel(Model):
    class Meta:
        table_name = 'CarModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    car_id = NumberAttribute(null=False)
    car_info = CarInfoMap(null=False)


class CarModelWithNull(Model):
    class Meta:
        table_name = 'CarModelWithNull'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    car_id = NumberAttribute(null=False)
    car_color = UnicodeAttribute(null=True)
    car_info = CarInfoMap(null=True)


class OfficeEmployeeMap(MapAttribute):
    office_employee_id = NumberAttribute()
    person = Person()
    office_location = Location()

    def cool_function(self):
        return 1


class GroceryList(Model):
    class Meta:
        table_name = 'GroceryListModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    store_name = UnicodeAttribute(hash_key=True)
    groceries = ListAttribute()


class Office(Model):
    class Meta:
        table_name = 'OfficeModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    office_id = NumberAttribute(hash_key=True)
    address = Location()
    employees = ListAttribute(of=OfficeEmployeeMap)


class BooleanConversionModel(Model):
    class Meta:
        table_name = 'BooleanConversionTable'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    user_name = UnicodeAttribute(hash_key=True)
    is_human = BooleanAttribute()


class TreeLeaf2(MapAttribute):
    value = NumberAttribute()


class TreeLeaf1(MapAttribute):
    value = NumberAttribute()
    left = TreeLeaf2()
    right = TreeLeaf2()


class TreeLeaf(MapAttribute):
    value = NumberAttribute()
    left = TreeLeaf1()
    right = TreeLeaf1()


class TreeModel(Model):
    class Meta:
        table_name = 'TreeModelTable'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    tree_key = UnicodeAttribute(hash_key=True)
    left = TreeLeaf()
    right = TreeLeaf()


class ExplicitRawMapModel(Model):
    class Meta:
        table_name = 'ExplicitRawMapModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    map_id = NumberAttribute(hash_key=True, default=123)
    map_attr = MapAttribute()


class MapAttrSubClassWithRawMapAttr(MapAttribute):
    num_field = NumberAttribute()
    str_field = UnicodeAttribute()
    map_field = MapAttribute()


class ExplicitRawMapAsMemberOfSubClass(Model):
    class Meta:
        table_name = 'ExplicitRawMapAsMemberOfSubClass'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    map_id = NumberAttribute(hash_key=True)
    sub_attr = MapAttrSubClassWithRawMapAttr()


class OverriddenSession(AioSession):
    """
    A overridden session for test
    """

    def __init__(self):
        super(OverriddenSession, self).__init__()


class OverriddenSessionModel(Model):
    """
    A testing model
    """

    class Meta:
        table_name = 'OverriddenSessionModel'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'
        request_timeout_seconds = 9999
        max_retry_attempts = 200
        base_backoff_ms = 4120
        session_cls = OverriddenSession

    random_user_name = UnicodeAttribute(hash_key=True, attr_name='random_name_1')
    random_attr = UnicodeAttribute(attr_name='random_attr_1', null=True)


class Animal(Model):
    name = UnicodeAttribute(hash_key=True)


class Dog(Animal):
    class Meta:
        table_name = 'Dog'
        aws_access_key_id = 'foo'
        aws_secret_access_key = 'bar'

    breed = UnicodeAttribute()


class ModelTestCase(TestCase):
    """
    Tests for the models API
    """

    @staticmethod
    async def init_table_meta(model_clz, table_data):
        with patch(PATCH_METHOD) as req:
            req.return_value = table_data
            await model_clz._get_meta_data()

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionaries
        """
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item.items() == d1_item.items():
                    found = True
            if not found:
                raise AssertionError("Values not equal: {0} {1}".format(d1_item, list2))
        if len(list1) != len(list2):
            raise AssertionError("Values not equal: {0} {1}".format(list1, list2))

    @pytest.mark.asyncio
    async def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        scope_args = {'count': 0}

        def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': UserModel.Meta.table_name}:
                if scope_args['count'] == 0:
                    return {}
                else:
                    return MODEL_TABLE_DATA
            else:
                return {}

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            await UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        # Test for default region
        self.assertEqual(UserModel.Meta.region, 'us-east-1')
        self.assertEqual(UserModel.Meta.request_timeout_seconds, 60)
        self.assertEqual(UserModel.Meta.max_retry_attempts, 3)
        self.assertEqual(UserModel.Meta.base_backoff_ms, 25)
        self.assertTrue(isinstance(UserModel.Meta.session_cls, AioSession))

        self.assertEqual(UserModel._connection.connection._request_timeout_seconds, 60)
        self.assertEqual(UserModel._connection.connection._max_retry_attempts_exception, 3)
        self.assertEqual(UserModel._connection.connection._base_backoff_ms, 25)

        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await UserModel.create_table(read_capacity_units=2, write_capacity_units=2)
            # The default region is us-east-1
            self.assertEqual(UserModel._connection.connection.region, 'us-east-1')

        # A table with a specified region
        self.assertEqual(RegionSpecificModel.Meta.region, 'us-west-1')
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await RegionSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(RegionSpecificModel._connection.connection.region, 'us-west-1')

        # A table with a specified host
        self.assertEqual(HostSpecificModel.Meta.host, 'http://localhost')
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await HostSpecificModel.create_table(read_capacity_units=2, write_capacity_units=2)
            self.assertEqual(HostSpecificModel._connection.connection.host, 'http://localhost')

        # A table with a specified capacity
        self.assertEqual(UserModel.Meta.read_capacity_units, 25)
        self.assertEqual(UserModel.Meta.write_capacity_units, 25)

        UserModel._connection = None

        def fake_wait(*obj, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                data = copy.deepcopy(MODEL_TABLE_DATA)
                data['Table']['TableStatus'] = 'Creating'
                scope_args['count'] += 1
                return data
            else:
                return MODEL_TABLE_DATA

        mock_wait = CoroutineMock()
        mock_wait.side_effect = fake_wait

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=mock_wait) as req:
            await UserModel.create_table(wait=True)
            params = {
                'AttributeDefinitions': [
                    {
                        'AttributeName': 'user_name',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'user_id',
                        'AttributeType': 'S'
                    }
                ],
                'KeySchema': [
                    {
                        'AttributeName': 'user_name',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'user_id',
                        'KeyType': 'RANGE'
                    }
                ],
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 25, 'WriteCapacityUnits': 25
                },
                'TableName': 'UserModel'
            }
            actual = req.call_args_list[1][0][1]
            self.assertEquals(sorted(actual.keys()), sorted(params.keys()))
            self.assertEquals(actual['TableName'], params['TableName'])
            self.assertEquals(actual['ProvisionedThroughput'], params['ProvisionedThroughput'])
            self.assert_dict_lists_equal(sorted(actual['KeySchema'], key=lambda x: x['AttributeName']),
                                         sorted(params['KeySchema'], key=lambda x: x['AttributeName']))
            # These come in random order
            self.assert_dict_lists_equal(sorted(actual['AttributeDefinitions'], key=lambda x: x['AttributeName']),
                                         sorted(params['AttributeDefinitions'], key=lambda x: x['AttributeName']))

    @pytest.mark.asyncio
    async def test_model_attrs(self):
        """
        Model()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            item = await UserModel.initialize('foo', 'bar')
            self.assertEqual(item.email, 'needs_email')
            self.assertEqual(item.callable_field, 42)
            self.assertEqual(
                repr(item), '{0}<{1}, {2}>'.format(UserModel.Meta.table_name, item.custom_user_name, item.user_id)
            )
            self.assertEqual(repr(await UserModel._get_meta_data()), 'MetaTable<{0}>'.format('Thread'))

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            item = await SimpleUserModel.initialize('foo')
            self.assertEqual(repr(item), '{0}<{1}>'.format(SimpleUserModel.Meta.table_name, item.user_name))
            await self.assertAsyncRaises(ValueError, item.save())

        await self.assertAsyncRaises(ValueError, UserModel.from_raw_data(None))

        with patch(PATCH_METHOD) as req:
            req.return_value = CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            item = await CustomAttrNameModel.initialize('foo', 'bar', overidden_attr='test')
            self.assertEqual(item.overidden_attr, 'test')
            self.assertTrue(not hasattr(item, 'foo_attr'))

    @fail_on(unused_loop=False)
    def test_overidden_defaults(self):
        """
        Custom attribute names
        """
        schema = CustomAttrNameModel._get_schema()
        correct_schema = {
            'KeySchema': [
                {'key_type': 'HASH', 'attribute_name': 'user_name'},
                {'key_type': 'RANGE', 'attribute_name': 'user_id'}
            ],
            'AttributeDefinitions': [
                {'attribute_type': 'S', 'attribute_name': 'user_name'},
                {'attribute_type': 'S', 'attribute_name': 'user_id'}
            ]
        }
        self.assert_dict_lists_equal(correct_schema['KeySchema'], schema['key_schema'])
        self.assert_dict_lists_equal(correct_schema['AttributeDefinitions'], schema['attribute_definitions'])

    @pytest.mark.asyncio
    async def test_overridden_attr_name(self):
        user = await UserModel.initialize(custom_user_name="bob")
        self.assertEqual(user.custom_user_name, "bob")
        self.assertRaises(AttributeError, getattr, user, "user_name")

        await self.assertAsyncRaises(ValueError, UserModel.initialize(user_name="bob"))

        await self.assertAsyncRaises(ValueError, CustomAttrNameModel.query("bob", foo_attr="bar"))

    @pytest.mark.asyncio
    async def test_refresh(self):
        """
        Model.refresh
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            item = await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await self.assertAsyncRaises(item.DoesNotExist, item.refresh())

        with patch(PATCH_METHOD) as req:
            req.return_value = GET_MODEL_ITEM_DATA
            item.picture = b'to-be-removed'
            await item.refresh()
            self.assertEqual(
                item.custom_user_name,
                GET_MODEL_ITEM_DATA.get(ITEM).get('user_name').get(STRING_SHORT))
            self.assertIsNone(item.picture)

    @pytest.mark.asyncio
    async def test_complex_key(self):
        """
        Model with complex key
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = COMPLEX_TABLE_DATA
            item = await ComplexKeyModel.initialize('test')

        with patch(PATCH_METHOD) as req:
            req.return_value = COMPLEX_ITEM_DATA
            await item.refresh()

    @pytest.mark.asyncio
    async def test_delete(self):
        """
        Model.delete
        """
        UserModel._meta_table = None
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            item = await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete()
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete(UserModel.user_id == 'bar')
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete(user_id='bar')
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete(UserModel.user_id == 'bar')
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete(user_id='bar')
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            self.assertEqual(args, params)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete((UserModel.user_id == 'bar') & UserModel.email.contains('@'))
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '(#0 = :0 AND contains (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'email'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    },
                    ':1': {
                        'S': '@'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = None
            await item.delete(user_id='bar', email__contains='@', conditional_operator='AND')
            params = {
                'Key': {
                    'user_id': {
                        'S': 'bar'
                    },
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '(contains (#0, :0) AND #1 = :1)',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': '@'
                    },
                    ':1': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            args = req.call_args[0][1]
            deep_eq(args, params, _assert=True)

    @pytest.mark.asyncio
    async def test_delete_doesnt_do_validation_on_null_attributes(self):
        """
        Model.delete
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = CAR_MODEL_TABLE_DATA
            await (await CarModel.initialize('foo')).delete()

        with patch(PATCH_METHOD) as req:
            req.return_value = CAR_MODEL_TABLE_DATA
            async with CarModel.batch_write() as batch:
                car = await CarModel.initialize('foo')
                await batch.delete(car)

    @pytest.mark.asyncio
    async def test_update(self):
        """
        Model.update
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            item = await SimpleUserModel.initialize('foo', is_active=True, email='foo@example.com', signature='foo')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save()

        await self.assertAsyncRaises(TypeError, item.update(["not", "a", "dict"]))

        await self.assertAsyncRaises(TypeError, item.update(actions={'not': 'a list'}))

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "email": {
                        "S": "foo@example.com",
                    },
                    "is_active": {
                        "NULL": None,
                    },
                    "aliases": {
                        "SS": set(["bob"]),
                    }
                }
            }
            await item.update(actions=[
                SimpleUserModel.email.set('foo@example.com'),
                SimpleUserModel.views.remove(),
                SimpleUserModel.is_active.set(None),
                SimpleUserModel.signature.set(None),
                SimpleUserModel.custom_aliases.set(['bob'])
            ])

            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'SET #0 = :0, #1 = :1, #2 = :2, #3 = :3 REMOVE #4',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'is_active',
                    '#2': 'signature',
                    '#3': 'aliases',
                    '#4': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo@example.com',
                    },
                    ':1': {
                        'NULL': True
                    },
                    ':2': {
                        'NULL': True
                    },
                    ':3': {
                        'SS': {'bob'}
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

            assert item.views is None
            self.assertEquals({'bob'}, item.custom_aliases)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "email": {
                        "S": "foo@example.com",
                    },
                    "is_active": {
                        "NULL": None,
                    },
                    "aliases": {
                        "SS": {"bob"},
                    }
                }
            }
            await item.update({
                'email': {'value': 'foo@example.com', 'action': 'put'},
                'views': {'action': 'delete'},
                'is_active': {'value': None, 'action': 'put'},
                'signature': {'value': None, 'action': 'put'},
                'custom_aliases': {'value': {'bob'}, 'action': 'put'},
            })

            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'SET #0 = :0, #1 = :1, #2 = :2, #3 = :3 REMOVE #4',
                'ExpressionAttributeNames': {
                    '#0': 'aliases',
                    '#1': 'email',
                    '#2': 'is_active',
                    '#3': 'signature',
                    '#4': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'SS': set(['bob'])
                    },
                    ':1': {
                        'S': 'foo@example.com',
                    },
                    ':2': {
                        'NULL': True
                    },
                    ':3': {
                        'NULL': True
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

            assert item.views is None
            self.assertEquals({'bob'}, item.custom_aliases)

        # Reproduces https://github.com/pynamodb/PynamoDB/issues/132
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "aliases": {
                        "SS": {"alias1", "alias3"}
                    }
                }
            }
            await item.update({
                'custom_aliases': {'value': {'alias2'}, 'action': 'delete'},
            })

            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'DELETE #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'SS': {'alias2'}
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

            assert item.views is None
            self.assertEquals({'alias1', 'alias3'}, item.custom_aliases)

    @pytest.mark.asyncio
    async def test_update_item(self):
        """
        Model.update_item
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            item = await SimpleUserModel.initialize('foo', email='bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save()

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await self.assertAsyncRaises(ValueError, item.update_item('views', 10))

        await self.assertAsyncRaises(ValueError, item.update_item('nonexistent', 5))
        await self.assertAsyncRaises(ValueError, item.update_item('views', 10, action='add', nonexistent__not_contains='-'))

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', condition=(
                (SimpleUserModel.user_name == 'foo') & ~SimpleUserModel.email.contains('@')
            ))
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '(#0 = :0 AND (NOT contains (#1, :1)))',
                'UpdateExpression': 'ADD #2 :2',
                'ExpressionAttributeNames': {
                    '#0': 'user_name',
                    '#1': 'email',
                    '#2': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo'
                    },
                    ':1': {
                        'S': '@'
                    },
                    ':2': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', user_name='foo', email__not_contains='@')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '((NOT contains (#1, :1)) AND #2 = :2)',
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views',
                    '#1': 'email',
                    '#2': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    },
                    ':1': {
                        'S': '@'
                    },
                    ':2': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', condition=SimpleUserModel.user_name.does_not_exist())
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'UpdateExpression': 'ADD #1 :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name',
                    '#1': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', user_name__exists=False)
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': 'attribute_not_exists (#1)',
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        # Reproduces https://github.com/pynamodb/PynamoDB/issues/59
        with patch(PATCH_METHOD) as req:
            user = await UserModel.initialize("test_hash", "test_range")
            req.return_value = {
                ATTRIBUTES: {}
            }
            await user.update_item('zip_code', 10, action='add')
            args = req.call_args[0][1]

            params = {
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    }
                },
                'TableName': 'UserModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_id': {'S': u'test_range'},
                    'user_name': {'S': u'test_hash'}
                },
                'ReturnConsumedCapacity': 'TOTAL'}
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            # Reproduces https://github.com/pynamodb/PynamoDB/issues/34
            item.email = None
            await item.update_item('views', 10, action='add')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                }
            }
            item.email = None
            await item.update_item('views', action='delete')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'REMOVE #0',
                'ExpressionAttributeNames': {
                    '#0': 'views'
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', condition=SimpleUserModel.numbers == [1, 2])
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 = :0',
                'UpdateExpression': 'ADD #1 :1',
                'ExpressionAttributeNames': {
                    '#0': 'numbers',
                    '#1': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'NS': ['1', '2']
                    },
                    ':1': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', numbers__eq=[1, 2])
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#1 = :1',
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views',
                    '#1': 'numbers'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    },
                    ':1': {
                        'NS': ['1', '2']
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        # Reproduces https://github.com/pynamodb/PynamoDB/issues/102
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', condition=SimpleUserModel.email.is_in('1@pynamo.db','2@pynamo.db'))
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#0 IN (:0, :1)',
                'UpdateExpression': 'ADD #1 :2',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'views'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': '1@pynamo.db'
                    },
                    ':1': {
                        'S': '2@pynamo.db'
                    },
                    ':2': {
                        'N': '10'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        # Reproduces https://github.com/pynamodb/PynamoDB/issues/102
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "views": {
                        "N": "10"
                    }
                }
            }
            await item.update_item('views', 10, action='add', email__in=['1@pynamo.db','2@pynamo.db'])
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'ConditionExpression': '#1 IN (:1, :2)',
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'views',
                    '#1': 'email'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'N': '10'
                    },
                    ':1': {
                        'S': '1@pynamo.db'
                    },
                    ':2': {
                        'S': '2@pynamo.db'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "aliases": {
                        "SS": {"lita"}
                    }
                }
            }
            await item.update_item('custom_aliases', {'lita'}, action='add')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'ADD #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'SS': {'lita'}
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)
            self.assertEqual({"lita"}, item.custom_aliases)

        with patch(PATCH_METHOD) as req:
            await item.update_item('is_active', True, action='put')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'SET #0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'is_active'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'BOOL': True
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)

        # Reproduces https://github.com/pynamodb/PynamoDB/issues/132
        with patch(PATCH_METHOD) as req:
            req.return_value = {
                ATTRIBUTES: {
                    "aliases": {
                        "SS": {"alias1", "alias3"}
                    }
                }
            }
            await item.update_item('custom_aliases', {'alias2'}, action='delete')
            args = req.call_args[0][1]
            params = {
                'TableName': 'SimpleModel',
                'ReturnValues': 'ALL_NEW',
                'Key': {
                    'user_name': {
                        'S': 'foo'
                    }
                },
                'UpdateExpression': 'DELETE #0 :0',
                'ExpressionAttributeNames': {
                    '#0': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'SS': {'alias2'}
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL'
            }
            deep_eq(args, params, _assert=True)
            self.assertEqual({"alias1", "alias3"}, item.custom_aliases)

    @pytest.mark.asyncio
    async def test_save(self):
        """
        Model.save
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            item = await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save()
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }

            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(UserModel.email.does_not_exist())
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'email'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(email__exists=False)
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': 'attribute_not_exists (#0)',
                'ExpressionAttributeNames': {
                    '#0': 'email'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(UserModel.email.does_not_exist() & UserModel.zip_code.exists())
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '(attribute_not_exists (#0) AND attribute_exists (#1))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'zip_code'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(email__exists=False, zip_code__null=False)
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '(attribute_not_exists (#0) AND attribute_exists (#1))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'zip_code'
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(
                (UserModel.custom_user_name == 'bar') | UserModel.zip_code.does_not_exist() | UserModel.email.contains('@')
            )
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '((#0 = :0 OR attribute_not_exists (#1)) OR contains (#2, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'user_name',
                    '#1': 'zip_code',
                    '#2': 'email'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'bar'
                    },
                    ':1': {
                        'S': '@'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(custom_user_name='bar', zip_code__null=True, email__contains='@', conditional_operator='OR')
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '((contains (#0, :0) OR #1 = :1) OR attribute_not_exists (#2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name',
                    '#2': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': '@'
                    },
                    ':1': {
                        'S': 'bar'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(UserModel.custom_user_name == 'foo')
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await item.save(custom_user_name='foo')
            args = req.call_args[0][1]
            params = {
                'Item': {
                    'callable_field': {
                        'N': '42'
                    },
                    'email': {
                        'S': u'needs_email'
                    },
                    'user_id': {
                        'S': u'bar'
                    },
                    'user_name': {
                        'S': u'foo'
                    },
                },
                'ConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'foo'
                    }
                },
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': 'UserModel'
            }
            deep_eq(args, params, _assert=True)

    @pytest.mark.asyncio
    async def test_filter_count(self):
        """
        Model.count(**filters)
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 10}
            res = await UserModel.count('foo')
            self.assertEqual(res, 10)
            args = req.call_args[0][1]
            params = {
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    }
                },
                'TableName': 'UserModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }
            deep_eq(args, params, _assert=True)

    @pytest.mark.asyncio
    async def test_count(self):
        """
        Model.count()
        """

        def fake_dynamodb(*args, **kwargs):
            return MODEL_TABLE_DATA

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            res = await UserModel.count()
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
            params = {'TableName': 'UserModel'}
            self.assertEqual(args, params)

    @pytest.mark.asyncio
    async def test_count_no_hash_key(self):
        with self.assertRaises(ValueError):
            await UserModel.count(zip_code__le='94117')

    @pytest.mark.asyncio
    async def test_index_count(self):
        """
        Model.index.count()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 42}
            res = await CustomAttrNameModel.uid_index.count(
                'foo',
                CustomAttrNameModel.overidden_user_name.startswith('bar'),
                limit=2)
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'Limit': 2,
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }
            deep_eq(args, params, _assert=True)

    @pytest.mark.asyncio
    async def test_index_multipage_count(self):
        with patch(PATCH_METHOD) as req:
            last_evaluated_key = {
                'user_name': {'S': u'user'},
                'user_id': {'S': '1234'},
            }
            req.side_effect = [
                {'Count': 1000, 'LastEvaluatedKey': last_evaluated_key},
                {'Count': 42}
            ]
            res = await CustomAttrNameModel.uid_index.count('foo')
            self.assertEqual(res, 1042)

            args_one = req.call_args_list[0][0][1]
            params_one = {
                'KeyConditionExpression': '#0 = :0',
                'ExpressionAttributeNames': {
                    '#0': 'user_id'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    }
                },
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }

            args_two = req.call_args_list[1][0][1]
            params_two = copy.deepcopy(params_one)
            params_two['ExclusiveStartKey'] = last_evaluated_key

            deep_eq(args_one, params_one, _assert=True)
            deep_eq(args_two, params_two, _assert=True)

    @pytest.mark.asyncio
    async def test_query_limit_greater_than_available_items_single_page(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(5):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)

            req.return_value = {'Count': len(items), 'Items': items}
            results = [i async for i in (await UserModel.query('foo', limit=25))]
            self.assertEqual(len(results), 5)
            self.assertEquals(req.mock_calls[0][1][1]['Limit'], 25)

    @pytest.mark.asyncio
    async def test_query_limit_identical_to_available_items_single_page(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(5):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)

            req.return_value = {'Count': len(items), 'Items': items}
            results = [i async for i in (await UserModel.query('foo', limit=5))]
            self.assertEqual(len(results), 5)
            self.assertEquals(req.mock_calls[0][1][1]['Limit'], 5)

    @pytest.mark.asyncio
    async def test_batch_get(self):
        """
        Model.batch_get
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            await SimpleUserModel.initialize('foo')

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{0}'.format(x) for x in range(10)]
            async for item in SimpleUserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-9'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-0'}}
                        ]
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{0}'.format(x) for x in range(10)]
            async for item in SimpleUserModel.batch_get(item_keys, attributes_to_get=['numbers']):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-9'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-0'}}
                        ],
                        'ProjectionExpression': '#0',
                        'ExpressionAttributeNames': {
                            '#0': 'numbers'
                        }
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_BATCH_GET_ITEMS
            item_keys = ['hash-{0}'.format(x) for x in range(10)]
            async for item in SimpleUserModel.batch_get(item_keys, consistent_read=True):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'RequestItems': {
                    'SimpleModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-9'}},
                            {'user_name': {'S': 'hash-8'}},
                            {'user_name': {'S': 'hash-7'}},
                            {'user_name': {'S': 'hash-6'}},
                            {'user_name': {'S': 'hash-5'}},
                            {'user_name': {'S': 'hash-4'}},
                            {'user_name': {'S': 'hash-3'}},
                            {'user_name': {'S': 'hash-2'}},
                            {'user_name': {'S': 'hash-1'}},
                            {'user_name': {'S': 'hash-0'}}
                        ],
                        'ConsistentRead': True
                    }
                }
            }
            self.assertEqual(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            await UserModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            item_keys = [('hash-{0}'.format(x), '{0}'.format(x)) for x in range(10)]
            item_keys_copy = list(item_keys)
            req.return_value = BATCH_GET_ITEMS
            async for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)
            self.assertEqual(item_keys, item_keys_copy)
            params = {
                'RequestItems': {
                    'UserModel': {
                        'Keys': [
                            {'user_name': {'S': 'hash-0'}, 'user_id': {'S': '0'}},
                            {'user_name': {'S': 'hash-1'}, 'user_id': {'S': '1'}},
                            {'user_name': {'S': 'hash-2'}, 'user_id': {'S': '2'}},
                            {'user_name': {'S': 'hash-3'}, 'user_id': {'S': '3'}},
                            {'user_name': {'S': 'hash-4'}, 'user_id': {'S': '4'}},
                            {'user_name': {'S': 'hash-5'}, 'user_id': {'S': '5'}},
                            {'user_name': {'S': 'hash-6'}, 'user_id': {'S': '6'}},
                            {'user_name': {'S': 'hash-7'}, 'user_id': {'S': '7'}},
                            {'user_name': {'S': 'hash-8'}, 'user_id': {'S': '8'}},
                            {'user_name': {'S': 'hash-9'}, 'user_id': {'S': '9'}}
                        ]
                    }
                }
            }
            args = req.call_args[0][1]
            self.assertTrue('RequestItems' in params)
            self.assertTrue('UserModel' in params['RequestItems'])
            self.assertTrue('Keys' in params['RequestItems']['UserModel'])
            self.assert_dict_lists_equal(
                params['RequestItems']['UserModel']['Keys'],
                args['RequestItems']['UserModel']['Keys'],
            )

        def fake_batch_get(*batch_args):
            kwargs = batch_args[1]
            if REQUEST_ITEMS in kwargs:
                batch_item = kwargs.get(REQUEST_ITEMS).get(UserModel.Meta.table_name).get(KEYS)[0]
                batch_items = kwargs.get(REQUEST_ITEMS).get(UserModel.Meta.table_name).get(KEYS)[1:]
                response = {
                    UNPROCESSED_KEYS: {
                        UserModel.Meta.table_name: {
                            KEYS: batch_items
                        }
                    },
                    RESPONSES: {
                        UserModel.Meta.table_name: [batch_item]
                    }
                }
                return response
            return {}

        batch_get_mock = CoroutineMock()
        batch_get_mock.side_effect = fake_batch_get

        with patch(PATCH_METHOD, new=batch_get_mock) as req:
            item_keys = [('hash-{0}'.format(x), '{0}'.format(x)) for x in range(200)]
            async for item in UserModel.batch_get(item_keys):
                self.assertIsNotNone(item)

    @pytest.mark.asyncio
    async def test_batch_write(self):
        """
        Model.batch_write
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {}

            async with UserModel.batch_write(auto_commit=False) as batch:
                pass

            async with UserModel.batch_write() as batch:
                self.assertIsNone(await batch.commit())

            with self.assertRaises(ValueError):
                async with UserModel.batch_write(auto_commit=False) as batch:
                    items = [await UserModel.initialize('hash-{0}'.format(x), '{0}'.format(x)) for x in range(26)]
                    for item in items:
                        await batch.delete(item)
                    await self.assertAsyncRaises(ValueError, batch.save(await UserModel.initialize('asdf', '1234')))

            async with UserModel.batch_write(auto_commit=False) as batch:
                items = [await UserModel.initialize('hash-{0}'.format(x), '{0}'.format(x)) for x in range(25)]
                for item in items:
                    await batch.delete(item)
                await self.assertAsyncRaises(ValueError, batch.save(await UserModel.initialize('asdf', '1234')))

            async with UserModel.batch_write(auto_commit=False) as batch:
                items = [await UserModel.initialize('hash-{0}'.format(x), '{0}'.format(x)) for x in range(25)]
                for item in items:
                    await batch.save(item)
                await self.assertAsyncRaises(ValueError, batch.save(await UserModel.initialize('asdf', '1234')))

            async with UserModel.batch_write() as batch:
                items = [await UserModel.initialize('hash-{0}'.format(x), '{0}'.format(x)) for x in range(30)]
                for item in items:
                    await batch.delete(item)

            async with UserModel.batch_write() as batch:
                items = [await UserModel.initialize('hash-{0}'.format(x), '{0}'.format(x)) for x in range(30)]
                for item in items:
                    await batch.save(item)

    @pytest.mark.asyncio
    async def test_batch_write_with_unprocessed(self):
        picture_blob = b'FFD8FFD8'

        items = []
        for idx in range(10):
            items.append(await UserModel.initialize(
                'daniel',
                '{0}'.format(idx),
                picture=picture_blob,
            ))

        unprocessed_items = []
        for idx in range(5, 10):
            unprocessed_items.append({
                'PutRequest': {
                    'Item': {
                        'custom_username': {STRING_SHORT: 'daniel'},
                        'user_id': {STRING_SHORT: '{0}'.format(idx)},
                        'picture': {BINARY_SHORT: base64.b64encode(picture_blob).decode(DEFAULT_ENCODING)}
                    }
                }
            })

        with patch(PATCH_METHOD) as req:
            req.side_effect = [
                {
                    UNPROCESSED_ITEMS: {
                        UserModel.Meta.table_name: unprocessed_items[:2],
                    }
                },
                {
                    UNPROCESSED_ITEMS: {
                        UserModel.Meta.table_name: unprocessed_items[2:],
                    }
                },
                {}
            ]

            async with UserModel.batch_write() as batch:
                for item in items:
                    await batch.save(item)

            self.assertEqual(len(req.mock_calls), 3)

    @pytest.mark.asyncio
    async def test_index_queries(self):
        """
        Model.Index.Query
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            await CustomAttrNameModel._get_meta_data()

        with patch(PATCH_METHOD) as req:
            req.return_value = INDEX_TABLE_DATA
            await IndexedModel._get_connection().describe_table()

        with patch(PATCH_METHOD) as req:
            req.return_value = LOCAL_INDEX_TABLE_DATA
            await LocalIndexedModel._get_meta_data()

        self.assertEqual(IndexedModel.include_index.Meta.index_name, "non_key_idx")

        queried = []
        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in await IndexedModel.email_index.query('foo', user_id__between=['id-1', 'id-3']):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in await IndexedModel.email_index.query('foo', user_name__startswith='foo'):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            with self.assertRaises(ValueError):
                for item in await IndexedModel.email_index.query('foo', name='foo'):
                    queried.append(item._serialize().get(RANGE))

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            async for item in await IndexedModel.email_index.query('foo', IndexedModel.user_name.startswith('bar'), limit=2):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'custom_idx_name',
                'TableName': 'IndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

        # Note this test is incorrect as 'user_name' is not the range key for email_index.
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            for item in await IndexedModel.email_index.query('foo', limit=2, user_name__begins_with='bar'):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'custom_idx_name',
                'TableName': 'IndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            async for item in await LocalIndexedModel.email_index.query(
                    'foo',
                    LocalIndexedModel.user_name.startswith('bar') & LocalIndexedModel.aliases.contains(1),
                    limit=1):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': '(begins_with (#1, :1) AND contains (#2, :2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name',
                    '#2': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    },
                    ':2': {
                        'S': '1'
                    }
                },
                'IndexName': 'email_index',
                'TableName': 'LocalIndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 1
            }
            self.assertEqual(req.call_args[0][1], params)

        # Note this test is incorrect as 'user_name' is not the range key for email_index.
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            for item in await LocalIndexedModel.email_index.query(
                    'foo',
                    limit=1,
                    user_name__begins_with='bar',
                    aliases__contains=1):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'FilterExpression': 'contains (#2, :2)',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_name',
                    '#2': 'aliases'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    },
                    ':2': {
                        'S': '1'
                    }
                },
                'IndexName': 'email_index',
                'TableName': 'LocalIndexedModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 1
            }
            self.assertEqual(req.call_args[0][1], params)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            for item in await CustomAttrNameModel.uid_index.query(
                    'foo',
                    CustomAttrNameModel.overidden_user_name.startswith('bar'),
                    limit=2):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '#0 = :0',
                'FilterExpression': 'begins_with (#1, :1)',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

        # Note: this test is incorrect since uid_index has no range key
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_name'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            queried = []

            async for item in await CustomAttrNameModel.uid_index.query('foo', limit=2, overidden_user_name__begins_with='bar'):
                queried.append(item._serialize())

            params = {
                'KeyConditionExpression': '(#0 = :0 AND begins_with (#1, :1))',
                'ExpressionAttributeNames': {
                    '#0': 'user_id',
                    '#1': 'user_name'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': u'foo'
                    },
                    ':1': {
                        'S': u'bar'
                    }
                },
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Limit': 2
            }
            self.assertEqual(req.call_args[0][1], params)

    @pytest.mark.asyncio
    async def test_multiple_indices_share_non_key_attribute(self):
        """
        Models.Model
        """
        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            return {}

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            await IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'AttributeDefinitions': [
                    {'AttributeName': 'email', 'AttributeType': 'S'},
                    {'AttributeName': 'numbers', 'AttributeType': 'NS'},
                    {'AttributeName': 'user_name', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[0][1]
            self.assert_dict_lists_equal(args['AttributeDefinitions'], params['AttributeDefinitions'])

        scope_args['count'] = 0

        with patch(PATCH_METHOD, new=fake_db) as req:
            await GameModel.create_table()
            params = {
                'KeySchema': [
                    {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                    {'KeyType': 'RANGE', 'AttributeName': 'created_time'}
                ],
                'LocalSecondaryIndexes': [
                    {
                        'KeySchema': [
                            {'KeyType': 'HASH', 'AttributeName': 'player_id'},
                            {'KeyType': 'RANGE', 'AttributeName': 'winner_id'}
                        ],
                        'IndexName': 'player_opponent_index',
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ],
                'TableName': 'GameModel',
                'ProvisionedThroughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1},
                'GlobalSecondaryIndexes': [
                    {
                        'ProvisionedThroughput': {'WriteCapacityUnits': 1, 'ReadCapacityUnits': 1},
                        'KeySchema': [
                            {'KeyType': 'HASH', 'AttributeName': 'winner_id'},
                            {'KeyType': 'RANGE', 'AttributeName': 'created_time'}
                        ],
                        'IndexName': 'opponent_time_index',
                        'Projection': {'ProjectionType': 'ALL'}
                    }
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'created_time', 'AttributeType': 'S'},
                    {'AttributeName': 'player_id', 'AttributeType': 'S'},
                    {'AttributeName': 'winner_id', 'AttributeType': 'S'}
                ]
            }
            args = req.call_args[0][1]
            for key in ['KeySchema', 'AttributeDefinitions', 'LocalSecondaryIndexes', 'GlobalSecondaryIndexes']:
                self.assert_dict_lists_equal(args[key], params[key])

    @pytest.mark.asyncio
    async def test_global_index(self):
        """
        Models.GlobalSecondaryIndex
        """
        self.assertIsNotNone(IndexedModel.email_index._hash_key_attribute())
        self.assertEqual(IndexedModel.email_index.Meta.projection.projection_type, AllProjection.projection_type)
        with patch(PATCH_METHOD) as req:
            req.return_value = INDEX_TABLE_DATA
            with self.assertRaises(ValueError):
                await IndexedModel.initialize('foo', 'bar')
            await IndexedModel._get_meta_data()

        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            else:
                return {}

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            await IndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'AttributeDefinitions': [
                    {'attribute_name': 'email', 'attribute_type': 'S'},
                    {'attribute_name': 'numbers', 'attribute_type': 'NS'}
                ],
                'KeySchema': [
                    {'AttributeName': 'numbers', 'KeyType': 'RANGE'},
                    {'AttributeName': 'email', 'KeyType': 'HASH'}
                ]
            }
            schema = IndexedModel.email_index._get_schema()
            args = req.call_args[0][1]
            self.assertEqual(
                args['GlobalSecondaryIndexes'][0]['ProvisionedThroughput'],
                {
                    'ReadCapacityUnits': 2,
                    'WriteCapacityUnits': 1
                }
            )
            self.assert_dict_lists_equal(schema['key_schema'], params['KeySchema'])
            self.assert_dict_lists_equal(schema['attribute_definitions'], params['AttributeDefinitions'])

    @pytest.mark.asyncio
    async def test_local_index(self):
        """
        Models.LocalSecondaryIndex
        """
        with self.assertRaises(ValueError):
            with patch(PATCH_METHOD) as req:
                req.return_value = LOCAL_INDEX_TABLE_DATA
                # This table has no range key
                await LocalIndexedModel.initialize('foo', 'bar')

        with patch(PATCH_METHOD) as req:
            req.return_value = LOCAL_INDEX_TABLE_DATA
            await LocalIndexedModel.initialize('foo')

        schema = IndexedModel._get_indexes()

        expected = {
            'local_secondary_indexes': [
                {
                    'KeySchema': [
                        {'KeyType': 'HASH', 'AttributeName': 'email'},
                        {'KeyType': 'RANGE', 'AttributeName': 'numbers'}
                    ],
                    'IndexName': 'include_index',
                    'projection': {
                        'ProjectionType': 'INCLUDE',
                        'NonKeyAttributes': ['numbers']
                    }
                }
            ],
            'global_secondary_indexes': [
                {
                    'KeySchema': [
                        {'KeyType': 'HASH', 'AttributeName': 'email'},
                        {'KeyType': 'RANGE', 'AttributeName': 'numbers'}
                    ],
                    'IndexName': 'email_index',
                    'projection': {'ProjectionType': 'ALL'},
                    'provisioned_throughput': {
                        'WriteCapacityUnits': 1,
                        'ReadCapacityUnits': 2
                    }
                }
            ],
            'attribute_definitions': [
                {'attribute_type': 'S', 'attribute_name': 'email'},
                {'attribute_type': 'NS', 'attribute_name': 'numbers'},
                {'attribute_type': 'S', 'attribute_name': 'email'},
                {'attribute_type': 'NS', 'attribute_name': 'numbers'}
            ]
        }
        self.assert_dict_lists_equal(
            schema['attribute_definitions'],
            expected['attribute_definitions']
        )
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['ProjectionType'], 'INCLUDE')
        self.assertEqual(schema['local_secondary_indexes'][0]['projection']['NonKeyAttributes'], ['numbers'])

        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            else:
                return {}

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            await LocalIndexedModel.create_table(read_capacity_units=2, write_capacity_units=2)
            params = {
                'AttributeDefinitions': [
                    {
                        'attribute_name': 'email', 'attribute_type': 'S'
                    },
                    {
                        'attribute_name': 'numbers',
                        'attribute_type': 'NS'
                    }
                ],
                'KeySchema': [
                    {
                        'AttributeName': 'email', 'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'numbers', 'KeyType': 'RANGE'
                    }
                ]
            }
            schema = LocalIndexedModel.email_index._get_schema()
            args = req.call_args[0][1]
            self.assert_dict_lists_equal(schema['attribute_definitions'], params['AttributeDefinitions'])
            self.assert_dict_lists_equal(schema['key_schema'], params['KeySchema'])
            self.assertTrue('ProvisionedThroughput' not in args['LocalSecondaryIndexes'][0])

    @fail_on(unused_loop=False)
    def test_projections(self):
        """
        Models.Projection
        """
        projection = AllProjection()
        self.assertEqual(projection.projection_type, ALL)

        projection = KeysOnlyProjection()
        self.assertEqual(projection.projection_type, KEYS_ONLY)

        projection = IncludeProjection(non_attr_keys=['foo', 'bar'])
        self.assertEqual(projection.projection_type, INCLUDE)
        self.assertEqual(projection.non_key_attributes, ['foo', 'bar'])

        self.assertRaises(ValueError, IncludeProjection, None)

        with self.assertRaises(ValueError):
            class BadIndex(Index):
                pass

            BadIndex()

        with self.assertRaises(ValueError):
            class BadIndex(Index):
                class Meta:
                    pass

                pass

            BadIndex()

    @pytest.mark.asyncio
    async def test_old_style_model_exception(self):
        """
        Display warning for pre v1.0 Models
        """
        with self.assertRaises(AttributeError):
            await OldStyleModel._get_meta_data()

        with self.assertRaises(AttributeError):
            await OldStyleModel.exists()

    async def test_dumps(self):
        """
        Model.dumps
        """
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                item['email'] = {STRING_SHORT: 'email-{0}'.format(random.randint(0, 65536))}
                item['picture'] = {BINARY_SHORT: BINARY_ATTR_DATA}
                items.append(item)
            req.return_value = {'Count': len(items), 'ScannedCount': len(items), 'Items': items}
            content = await UserModel.dumps()
            serialized_items = json.loads(content)
            for original, new_item in zip(items, serialized_items):
                self.assertEqual(new_item[0], original['user_name'][STRING_SHORT])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['zip_code']['N'], original['zip_code']['N'])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['email']['S'], original['email']['S'])
                self.assertEqual(new_item[1][pythonic(ATTRIBUTES)]['picture']['B'], original['picture']['B'])

    @pytest.mark.asyncio
    async def test_loads(self):
        """
        Model.loads
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await UserModel.loads(json.dumps(SERIALIZED_TABLE_DATA))

        args = {
            'UserModel': [
                {
                    'PutRequest': {
                        'Item': {
                            'user_id': {'S': u'id-0'},
                            'callable_field': {'N': '42'},
                            'user_name': {'S': u'foo'},
                            'email': {'S': u'email-7980'},
                            'picture': {
                                "B": "aGVsbG8sIHdvcmxk"
                            },
                            'zip_code': {'N': '88030'}
                        }
                    }
                },
                {
                    'PutRequest': {
                        'Item': {
                            'user_id': {'S': u'id-1'},
                            'callable_field': {'N': '42'},
                            'user_name': {'S': u'foo'},
                            'email': {'S': u'email-19770'},
                            'picture': {
                                "B": "aGVsbG8sIHdvcmxk"
                            },
                            'zip_code': {'N': '88030'}
                        }
                    }
                }
            ]
        }

        self.assert_dict_lists_equal(req.call_args[0][1]['RequestItems']['UserModel'], args['UserModel'])

    @pytest.mark.asyncio
    async def test_loads_complex_model(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            await ComplexModel.loads(json.dumps(COMPLEX_MODEL_SERIALIZED_TABLE_DATA))

        args = {
            'ComplexModel': [
                {
                    'PutRequest': COMPLEX_MODEL_ITEM_DATA
                }
            ]
        }
        self.assert_dict_lists_equal(req.call_args[0][1]['RequestItems']['ComplexModel'], args['ComplexModel'])

    async def _get_office_employee(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )

        loc = Location(
            lat=37.77461,
            lng=-122.3957216,
            name='Lyft HQ'
        )

        return await OfficeEmployee.initialize(
            hash_key=None,
            range_key=None,
            office_employee_id=123,
            person=justin,
            office_location=loc
        )

    async def _get_grocery_list(self):
        return await GroceryList.initialize(store_name='Haight Street Market',
                                            groceries=['bread', 1, 'butter', 6, 'milk', 1])

    async def _get_complex_thing(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )
        return await ComplexModel.initialize(person=justin, key=123)

    async def _get_office(self):
        justin = Person(
            fname='Justin',
            lname='Phillips',
            age=31,
            is_male=True
        )
        lei = Person(
            fname='Lei',
            lname='Ding',
            age=32,
            is_male=True
        )
        garrett = Person(
            fname='Garrett',
            lname='Heel',
            age=30,
            is_male=True
        )
        tanya = Person(
            fname='Tanya',
            lname='Ashkenazi',
            age=30,
            is_male=False
        )
        loc = Location(
            lat=37.77461,
            lng=-122.3957216,
            name='Lyft HQ'
        )
        emp1 = OfficeEmployeeMap(
            office_employee_id=123,
            person=justin,
            office_location=loc
        )
        emp2 = OfficeEmployeeMap(
            office_employee_id=124,
            person=lei,
            office_location=loc
        )
        emp3 = OfficeEmployeeMap(
            office_employee_id=125,
            person=garrett,
            office_location=loc
        )
        emp4 = OfficeEmployeeMap(
            office_employee_id=126,
            person=tanya,
            office_location=loc
        )
        return await Office.initialize(
            office_id=3,
            address=loc,
            employees=[emp1, emp2, emp3, emp4]
        )

    @pytest.mark.asyncio
    async def test_rate_limited_scan(self):
        """
        Model.rate_limited_scan
        """
        with patch('inpynamodb.connection.AsyncConnection.rate_limited_scan') as req:
            items = []

            item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
            item['user_id'] = {STRING_SHORT: '11232'}
            items.append(item)

            req.return_value = items
            self.assertEqual(1, len(UserModel.rate_limited_scan(
                segment=1,
                total_segments=12,
                limit=16,
                conditional_operator='AND',
                last_evaluated_key='XXX',
                page_size=11,
                timeout_seconds=21,
                read_capacity_to_consume_per_second=33,
                allow_rate_limited_scan_without_consumed_capacity=False,
                max_sleep_between_retry=4,
                max_consecutive_exceptions=22,
                attributes_to_get=['X1', 'X2'],
                consistent_read=True,
                index_name='index'
            )))

            self.assertEqual('UserModel', req.call_args[0][0])
            params = {
                'filter_condition': None,
                'segment': 1,
                'total_segments': 12,
                'limit': 16,
                'conditional_operator': 'AND',
                'exclusive_start_key': 'XXX',
                'page_size': 11,
                'timeout_seconds': 21,
                'scan_filter': {},
                'attributes_to_get': ['X1', 'X2'],
                'read_capacity_to_consume_per_second': 33,
                'allow_rate_limited_scan_without_consumed_capacity': False,
                'max_sleep_between_retry': 4,
                'max_consecutive_exceptions': 22,
                'consistent_read': True,
                'index_name': 'index'
            }
            self.assertEqual(params, req.call_args[1])

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Items': items}
            scanned_items = []

            for item in await UserModel.rate_limited_scan(limit=5):
                scanned_items.append(item._serialize().get(RANGE))
            self.assertListEqual(
                [item.get('user_id').get(STRING_SHORT) for item in items[:5]],
                scanned_items
            )

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Items': items, 'ConsumedCapacity': {'TableName': 'UserModel', 'CapacityUnits': 10}}
            scan_result = await UserModel.rate_limited_scan(
                user_id__contains='tux',
                zip_code__null=False,
                email__null=True,
                read_capacity_to_consume_per_second=13
            )

            for item in scan_result:
                self.assertIsNotNone(item)
            params = {
                'Limit': 13,
                'ReturnConsumedCapacity': 'TOTAL',
                'FilterExpression': '((attribute_not_exists (#0) AND contains (#1, :0)) AND attribute_exists (#2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_id',
                    '#2': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'tux'
                    }
                },
                'TableName': 'UserModel'
            }
            self.assertEquals(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            for item in await UserModel.scan(
                    user_id__contains='tux',
                    zip_code__null=False,
                    conditional_operator='OR',
                    email__null=True,
                    page_size=12):
                self.assertIsNotNone(item)
            params = {
                'Limit': 12,
                'ReturnConsumedCapacity': 'TOTAL',
                'FilterExpression': '((attribute_not_exists (#0) OR contains (#1, :0)) OR attribute_exists (#2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_id',
                    '#2': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'tux'
                    },
                },
                'TableName': 'UserModel'
            }

            self.assertEquals(params, req.call_args[0][1])

        async def _multiple_scan():
            return list(await UserModel.scan(user_id__contains='tux', user_id__beginswith='penguin'))

        # you cannot scan with multiple conditions against the same key
        self.assertAsyncRaises(
            ValueError,
            _multiple_scan()
        )

    @pytest.mark.asyncio
    async def test_scan(self):
        """
        Model.scan
        """
        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            scanned_items = []
            async for item in await UserModel.scan():
                scanned_items.append(item._serialize().get(RANGE))
            self.assertListEqual(
                [item.get('user_id').get(STRING_SHORT) for item in items],
                scanned_items
            )

        def fake_scan(*args):
            kwargs = args[1]
            start_key = kwargs.get(EXCLUSIVE_START_KEY, None)
            if start_key:
                item_idx = 0
                for scan_item in BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name):
                    item_idx += 1
                    if scan_item == start_key:
                        break
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[item_idx:item_idx + 1]
            else:
                scan_items = BATCH_GET_ITEMS.get(RESPONSES).get(UserModel.Meta.table_name)[:1]
            data = {
                CAMEL_COUNT: len(scan_items),
                ITEMS: scan_items,
                LAST_EVALUATED_KEY: scan_items[-1] if len(scan_items) else None
            }
            return data

        mock_scan = CoroutineMock()
        mock_scan.side_effect = fake_scan

        with patch(PATCH_METHOD, new=mock_scan) as req:
            async for item in await UserModel.scan():
                self.assertIsNotNone(item)

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            async for item in await UserModel.scan(user_id__contains='tux', zip_code__null=False, email__null=True):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'FilterExpression': '((attribute_not_exists (#0) AND contains (#1, :0)) AND attribute_exists (#2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_id',
                    '#2': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'tux'
                    }
                },
                'TableName': 'UserModel'
            }
            self.assertEquals(params, req.call_args[0][1])

        with patch(PATCH_METHOD) as req:
            items = []
            for idx in range(10):
                item = copy.copy(GET_MODEL_ITEM_DATA.get(ITEM))
                item['user_id'] = {STRING_SHORT: 'id-{0}'.format(idx)}
                items.append(item)
            req.return_value = {'Count': len(items), 'Items': items}
            async for item in await UserModel.scan(
                    user_id__contains='tux',
                    zip_code__null=False,
                    conditional_operator='OR',
                    email__null=True):
                self.assertIsNotNone(item)
            params = {
                'ReturnConsumedCapacity': 'TOTAL',
                'FilterExpression': '((attribute_not_exists (#0) OR contains (#1, :0)) OR attribute_exists (#2))',
                'ExpressionAttributeNames': {
                    '#0': 'email',
                    '#1': 'user_id',
                    '#2': 'zip_code'
                },
                'ExpressionAttributeValues': {
                    ':0': {
                        'S': 'tux'
                    }
                },
                'TableName': 'UserModel'
            }
            self.assertEquals(params, req.call_args[0][1])

        async def _multiple_scan():
            return list(await UserModel.scan(user_id__contains='tux', user_id__beginswith='penguin'))

        # you cannot scan with multiple conditions against the same key
        self.assertAsyncRaises(
            ValueError,
            _multiple_scan()
        )

    @pytest.mark.asyncio
    async def test_model_with_maps(self):
        office_employee = await self._get_office_employee()
        with patch(PATCH_METHOD) as req:
            req.return_value = OFFICE_EMPLOYEE_MODEL_TABLE_DATA
            await office_employee.save()

    @pytest.mark.asyncio
    async def test_model_with_list(self):
        grocery_list = await self._get_grocery_list()
        with patch(PATCH_METHOD) as req:
            req.return_value = GROCERY_LIST_MODEL_TABLE_DATA
            await grocery_list.save()

    @pytest.mark.asyncio
    async def test_model_with_list_of_map(self):
        item = await self._get_office()
        with patch(PATCH_METHOD) as req:
            req.return_value = OFFICE_MODEL_TABLE_DATA
            await item.save()

    @pytest.mark.asyncio
    async def test_model_with_nulls_validates(self):
        car_info = CarInfoMap(make='Dodge')
        item = await CarModel.initialize(car_id=123, car_info=car_info)
        with patch(PATCH_METHOD) as req:
            req.return_value = CAR_MODEL_WITH_NULL_ITEM_DATA
            await item.save()

    @pytest.mark.asyncio
    async def test_model_with_invalid_data_does_not_validate(self):
        car_info = CarInfoMap(model='Envoy')
        item = await CarModel.initialize(car_id=123, car_info=car_info)
        with patch(PATCH_METHOD) as req:
            req.return_value = INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA
            with self.assertRaises(ValueError):
                await item.save()

    @pytest.mark.asyncio
    async def test_model_works_like_model(self):
        office_employee = await self._get_office_employee()
        self.assertTrue(office_employee.person)
        self.assertEquals('Justin', office_employee.person.fname)
        self.assertEquals('Phillips', office_employee.person.lname)
        self.assertEquals(31, office_employee.person.age)
        self.assertEquals(True, office_employee.person.is_male)

    @pytest.mark.asyncio
    async def test_list_works_like_list(self):
        grocery_list = await self._get_grocery_list()
        self.assertTrue(grocery_list.groceries)
        self.assertEquals('butter', grocery_list.groceries[2])

    @pytest.mark.asyncio
    async def test_complex_model_is_complex(self):
        complex_thing = await self._get_complex_thing()
        self.assertTrue(complex_thing.person)
        self.assertEquals(complex_thing.person.fname, 'Justin')
        self.assertEquals(complex_thing.key, 123)

    @pytest.mark.asyncio
    async def test_list_of_map_works_like_list_of_map(self):
        office = await self._get_office()
        self.assertTrue(office.employees[1].person.is_male)
        self.assertFalse(office.employees[3].person.is_male)
        self.assertEquals(office.employees[2].person.fname, 'Garrett')
        self.assertEquals(office.employees[0].person.lname, 'Phillips')

    @pytest.mark.asyncio
    async def test_invalid_map_model_raises(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       FULL_CAR_MODEL_ITEM_DATA, 'car_id', 'N', '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            with self.assertRaises(ValueError):
                await (await CarModel.initialize(car_id=2)).save()
            try:
                await (await CarModel.initialize(car_id=2)).save()
            except ValueError as e:
                assert str(e) == "Attribute 'car_info' cannot be None"

    @pytest.mark.asyncio
    async def test_model_with_maps_retrieve_from_db(self):
        fake_db = self.database_mocker(OfficeEmployee, OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
                                       GET_OFFICE_EMPLOYEE_ITEM_DATA, 'office_employee_id', 'N', '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA
            item = await OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA.get(ITEM).get('person').get(
                    MAP_SHORT).get('firstName').get(STRING_SHORT))

    @pytest.mark.asyncio
    async def test_model_with_maps_with_nulls_retrieve_from_db(self):
        fake_db = self.database_mocker(OfficeEmployee, OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
                                       GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL, 'office_employee_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL
            item = await OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA_WITH_NULL.get(ITEM).get('person').get(
                    MAP_SHORT).get('firstName').get(STRING_SHORT))
            self.assertIsNone(item.person.age)
            self.assertIsNone(item.person.is_male)

    @pytest.mark.asyncio
    async def test_model_with_maps_with_pythonic_attributes(self):
        fake_db = self.database_mocker(
            OfficeEmployee,
            OFFICE_EMPLOYEE_MODEL_TABLE_DATA,
            GET_OFFICE_EMPLOYEE_ITEM_DATA,
            'office_employee_id',
            'N',
            '123'
        )

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_EMPLOYEE_ITEM_DATA
            item = await OfficeEmployee.get(123)
            self.assertEqual(
                item.person.fname,
                GET_OFFICE_EMPLOYEE_ITEM_DATA
                    .get(ITEM)
                    .get('person')
                    .get(MAP_SHORT)
                    .get('firstName')
                    .get(STRING_SHORT)
            )
        assert item.person.is_male
        with pytest.raises(AttributeError):
            item.person.is_dude

    @pytest.mark.asyncio
    async def test_model_with_list_retrieve_from_db(self):
        fake_db = self.database_mocker(GroceryList, GROCERY_LIST_MODEL_TABLE_DATA,
                                       GET_GROCERY_LIST_ITEM_DATA, 'store_name', 'S',
                                 'Haight Street Market')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_GROCERY_LIST_ITEM_DATA
            item = await GroceryList.get('Haight Street Market')
            self.assertEquals(item.store_name, GET_GROCERY_LIST_ITEM_DATA.get(ITEM).get('store_name').get(STRING_SHORT))
            self.assertEquals(
                item.groceries[2],
                GET_GROCERY_LIST_ITEM_DATA.get(ITEM).get('groceries').get(
                    LIST_SHORT)[2].get(STRING_SHORT))
            self.assertEquals(item.store_name, 'Haight Street Market')

    @pytest.mark.asyncio
    async def test_model_with_list_of_map_retrieve_from_db(self):
        fake_db = self.database_mocker(Office, OFFICE_MODEL_TABLE_DATA,
                                       GET_OFFICE_ITEM_DATA, 'office_id', 'N', '6161')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = GET_OFFICE_ITEM_DATA
            item = await Office.get(6161)
            self.assertEquals(item.office_id,
                              int(GET_OFFICE_ITEM_DATA.get(ITEM).get('office_id').get(NUMBER_SHORT)))
            self.assertEquals(item.office_id, 6161)
            self.assertEquals(
                item.employees[2].person.fname,
                GET_OFFICE_ITEM_DATA.get(ITEM).get('employees').get(
                    LIST_SHORT)[2].get(MAP_SHORT).get('person').get(MAP_SHORT).get('firstName').get(STRING_SHORT))

    @pytest.mark.asyncio
    async def test_complex_model_retrieve_from_db(self):
        fake_db = self.database_mocker(ComplexModel, COMPLEX_MODEL_TABLE_DATA,
                                       COMPLEX_MODEL_ITEM_DATA, 'key', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = COMPLEX_MODEL_ITEM_DATA
            item = await ComplexModel.get(123)
            self.assertEquals(item.key,
                              int(COMPLEX_MODEL_ITEM_DATA.get(ITEM).get(
                                  'key').get(NUMBER_SHORT)))
            self.assertEquals(item.key, 123)
            self.assertEquals(
                item.person.fname,
                COMPLEX_MODEL_ITEM_DATA.get(ITEM).get('weird_person').get(
                    MAP_SHORT).get('firstName').get(STRING_SHORT))

    def database_mocker(self, model, table_data, item_data,
                        primary_key_name, primary_key_dynamo_type, primary_key_id):
        def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': model.Meta.table_name}:
                return table_data
            elif kwargs == {
                'ReturnConsumedCapacity': 'TOTAL',
                'TableName': model.Meta.table_name,
                'Key': {
                    primary_key_name: {primary_key_dynamo_type: primary_key_id},
                },
                'ConsistentRead': False}:
                return item_data
            return table_data
        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb
        return fake_db

    @pytest.mark.asyncio
    async def test_car_model_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       FULL_CAR_MODEL_ITEM_DATA, 'car_id', 'N', '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = FULL_CAR_MODEL_ITEM_DATA
            item = await CarModel.get(123)
            self.assertEquals(item.car_id,
                              int(FULL_CAR_MODEL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER_SHORT)))
            self.assertEquals(item.car_info.make, 'Volkswagen')
            self.assertEquals(item.car_info.model, 'Beetle')

    @pytest.mark.asyncio
    async def test_car_model_with_null_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       CAR_MODEL_WITH_NULL_ITEM_DATA, 'car_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = CAR_MODEL_WITH_NULL_ITEM_DATA
            item = await CarModel.get(123)
            self.assertEquals(item.car_id,
                              int(CAR_MODEL_WITH_NULL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER_SHORT)))
            self.assertEquals(item.car_info.make, 'Dodge')
            self.assertIsNone(item.car_info.model)

    @pytest.mark.asyncio
    async def test_invalid_car_model_with_null_retrieve_from_db(self):
        fake_db = self.database_mocker(CarModel, CAR_MODEL_TABLE_DATA,
                                       INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA, 'car_id', 'N',
                                 '123')

        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA
            item = await CarModel.get(123)
            self.assertEquals(item.car_id,
                              int(INVALID_CAR_MODEL_WITH_NULL_ITEM_DATA.get(ITEM).get(
                                  'car_id').get(NUMBER_SHORT)))
            self.assertIsNone(item.car_info.make)

    @pytest.mark.asyncio
    async def test_new_style_boolean_serializes_as_bool(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_TABLE_DATA
            item = await BooleanConversionModel.initialize(user_name='justin', is_human=True)
            item.save()

    @pytest.mark.asyncio
    async def test_old_style_boolean_serializes_as_bool(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_TABLE_DATA_OLD_STYLE
            item = await BooleanConversionModel.initialize(user_name='justin', is_human=True)
            item.save()

    @pytest.mark.asyncio
    async def test_deserializing_old_style_bool_false_works(self):
        fake_db = self.database_mocker(BooleanConversionModel, BOOLEAN_CONVERSION_MODEL_TABLE_DATA,
                                       BOOLEAN_CONVERSION_MODEL_OLD_STYLE_FALSE_ITEM_DATA,
                                 'user_name', 'S',
                                 'alf')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_OLD_STYLE_FALSE_ITEM_DATA
            item = await BooleanConversionModel.get('alf')
            self.assertFalse(item.is_human)

    @pytest.mark.asyncio
    async def test_deserializing_old_style_bool_true_works(self):
        fake_db = self.database_mocker(BooleanConversionModel,
                                       BOOLEAN_CONVERSION_MODEL_TABLE_DATA,
                                       BOOLEAN_CONVERSION_MODEL_OLD_STYLE_TRUE_ITEM_DATA,
                                 'user_name', 'S',
                                 'justin')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_OLD_STYLE_TRUE_ITEM_DATA
            item = await BooleanConversionModel.get('justin')
            self.assertTrue(item.is_human)

    @pytest.mark.asyncio
    async def test_deserializing_new_style_bool_false_works(self):
        fake_db = self.database_mocker(BooleanConversionModel,
                                       BOOLEAN_CONVERSION_MODEL_TABLE_DATA,
                                       BOOLEAN_CONVERSION_MODEL_NEW_STYLE_FALSE_ITEM_DATA,
                                 'user_name', 'S',
                                 'alf')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_NEW_STYLE_FALSE_ITEM_DATA
            item = await BooleanConversionModel.get('alf')
            self.assertFalse(item.is_human)

    @pytest.mark.asyncio
    async def test_deserializing_new_style_bool_true_works(self):
        fake_db = self.database_mocker(BooleanConversionModel,
                                       BOOLEAN_CONVERSION_MODEL_TABLE_DATA,
                                       BOOLEAN_CONVERSION_MODEL_NEW_STYLE_TRUE_ITEM_DATA,
                                 'user_name', 'S',
                                 'justin')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = BOOLEAN_CONVERSION_MODEL_NEW_STYLE_TRUE_ITEM_DATA
            item = await BooleanConversionModel.get('justin')
            self.assertTrue(item.is_human)

    @pytest.mark.asyncio
    async def test_deserializing_map_four_layers_deep_works(self):
        fake_db = self.database_mocker(TreeModel,
                                       TREE_MODEL_TABLE_DATA,
                                       TREE_MODEL_ITEM_DATA,
                                 'tree_key', 'S',
                                 '123')
        with patch(PATCH_METHOD, new=fake_db) as req:
            req.return_value = TREE_MODEL_ITEM_DATA
            item = await TreeModel.get('123')
            self.assertEquals(item.left.left.left.value, 3)

    @pytest.mark.asyncio
    async def test_conditional_operator_map_attribute(self):
        with patch(PATCH_METHOD) as req:
            req.return_value = {}
            item = await self._get_complex_thing()
            with self.assertRaises(NotImplementedError):
                await item.save(key=123, conditional_operator='OR')
            with self.assertRaises(NotImplementedError):
                await item.delete(key=123, conditional_operator='OR')
            with self.assertRaises(NotImplementedError):
                await item.update_item(123, conditional_operator='OR')
            with self.assertRaises(NotImplementedError):
                list(await ComplexModel.query(123, limit=20, conditional_operator='OR'))
            with self.assertRaises(NotImplementedError):
                list(await ComplexModel.scan(conditional_operator='OR'))

    @pytest.mark.asyncio
    async def test_result_set_init(self):
        results = []
        operations = 1
        arguments = 'args'
        rs = ResultSet(results=results, operation=operations, arguments=arguments)
        self.assertEquals(rs.results, results)
        self.assertEquals(rs.operation, operations)
        self.assertEquals(rs.arguments, arguments)

    @pytest.mark.asyncio
    async def test_result_set_iter(self):
        results = [1, 2, 3]
        operations = 1
        arguments = 'args'
        rs = ResultSet(results=results, operation=operations, arguments=arguments)
        for k in rs:
            self.assertTrue(k in results)

    @pytest.mark.asyncio
    async def test_explicit_raw_map_serialize_pass(self):
        map_native = {'foo': 'bar'}
        map_serialized = {'M': {'foo': {'S': 'bar'}}}
        instance = await ExplicitRawMapModel.initialize(map_attr=map_native)
        serialized = instance._serialize()
        self.assertEqual(serialized['attributes']['map_attr'], map_serialized)

    @pytest.mark.asyncio
    async def test_raw_map_serialize_fun_one(self):
        map_native = {
            'foo': 'bar', 'num': 12345678909876543211234234324234, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1,2,3],
            'mapy': {'baz': 'bongo'}
        }
        expected = {'M': {'foo': {'S': u'bar'},
               'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '3'}]},
               'num': {'N': '12345678909876543211234234324234'}, 'other_b_type': {'BOOL': False},
               'floaty': {'N': '1.2'}, 'mapy': {'M': {'baz': {'S': u'bongo'}}},
               'bool_type': {'BOOL': True}}}

        instance = await ExplicitRawMapModel.initialize(map_attr=map_native)
        serialized = instance._serialize()
        actual = serialized['attributes']['map_attr']
        self.assertEqual(expected, actual)

    @pytest.mark.asyncio
    async def test_raw_map_deserializes(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 12345678909876543211234234324234],
            'mapy': {'baz': 'bongo'}
        }
        map_serialized = {
            'M': {
                'foo': {'S': 'bar'},
                'num': {'N': '1'},
                'bool_type': {'BOOL': True},
                'other_b_type': {'BOOL': False},
                'floaty': {'N': '1.2'},
                'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '12345678909876543211234234324234'}]},
                'mapy': {'M': {'baz': {'S': 'bongo'}}}
            }
        }
        instance = await ExplicitRawMapModel.initialize()
        instance._deserialize({'map_attr': map_serialized})
        actual = instance.map_attr
        for k, v in six.iteritems(map_native):
            self.assertEqual(v, actual[k])

    @pytest.mark.asyncio
    async def test_raw_map_from_raw_data_works(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 12345678909876543211234234324234],
            'mapy': {'baz': 'bongo'}
        }
        fake_db = self.database_mocker(ExplicitRawMapModel,
                                       EXPLICIT_RAW_MAP_MODEL_TABLE_DATA,
                                       EXPLICIT_RAW_MAP_MODEL_ITEM_DATA,
                                       'map_id', 'N',
                                       '123')
        with patch(PATCH_METHOD, new=fake_db):
            item = await ExplicitRawMapModel.get(123)
            actual = item.map_attr
            self.assertEqual(map_native.get('listy')[2], actual['listy'][2])
            for k, v in six.iteritems(map_native):
                self.assertEqual(v, actual[k])

    @pytest.mark.asyncio
    async def test_raw_map_as_sub_map_serialize_pass(self):
        map_native = {'a': 'dict', 'lives': [123, 456], 'here': True}
        map_serialized = {
            'M': {
                'a': {'S': 'dict'},
                'lives': {'L': [{'N': '123'}, {'N': '456'}]},
                'here': {'BOOL': True}
            }
        }
        instance = await ExplicitRawMapAsMemberOfSubClass.initialize(
            map_id=123,
            sub_attr=MapAttrSubClassWithRawMapAttr(
                num_field=37, str_field='hi',
                map_field=map_native
            )
        )
        serialized = instance._serialize()
        self.assertEqual(serialized['attributes']['sub_attr']['M']['map_field'], map_serialized)

    async def _get_raw_map_as_sub_map_test_data(self):
        map_native = {
            'foo': 'bar', 'num': 1, 'bool_type': True,
            'other_b_type': False, 'floaty': 1.2, 'listy': [1, 2, 3],
            'mapy': {'baz': 'bongo'}
        }
        map_serialized = {
            'M': {
                'foo': {'S': 'bar'},
                'num': {'N': '1'},
                'bool_type': {'BOOL': True},
                'other_b_type': {'BOOL': False},
                'floaty': {'N': '1.2'},
                'listy': {'L': [{'N': '1'}, {'N': '2'}, {'N': '3'}]},
                'mapy': {'M': {'baz': {'S': 'bongo'}}}
            }
        }

        sub_attr = MapAttrSubClassWithRawMapAttr(
            num_field=37, str_field='hi', map_field=map_native
        )

        instance = await ExplicitRawMapAsMemberOfSubClass.initialize(
            map_id=123,
            sub_attr=sub_attr
        )
        return map_native, map_serialized, sub_attr, instance

    @pytest.mark.asyncio
    async def test_raw_map_as_sub_map(self):
        map_native, map_serialized, sub_attr, instance = await self._get_raw_map_as_sub_map_test_data()
        actual = instance.sub_attr
        self.assertEqual(sub_attr, actual)
        self.assertEqual(actual.map_field['floaty'], map_native.get('floaty'))
        self.assertEqual(actual.map_field['mapy']['baz'], map_native.get('mapy').get('baz'))

    @pytest.mark.asyncio
    async def test_raw_map_as_sub_map_deserialize(self):
        map_native, map_serialized, _, _ = await self._get_raw_map_as_sub_map_test_data()

        actual = MapAttrSubClassWithRawMapAttr().deserialize({
            "map_field": map_serialized
        })

        for k, v in six.iteritems(map_native):
            self.assertEqual(actual.map_field[k], v)

    @pytest.mark.asyncio
    async def test_raw_map_as_sub_map_from_raw_data_works(self):
        map_native, map_serialized, sub_attr, instance = await self._get_raw_map_as_sub_map_test_data()
        fake_db = self.database_mocker(ExplicitRawMapAsMemberOfSubClass,
                                       EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_TABLE_DATA,
                                       EXPLICIT_RAW_MAP_MODEL_AS_SUB_MAP_IN_TYPED_MAP_ITEM_DATA,
                                       'map_id', 'N',
                                       '123')
        with patch(PATCH_METHOD, new=fake_db):
            item = await ExplicitRawMapAsMemberOfSubClass.get(123)
            actual = item.sub_attr
            self.assertEqual(sub_attr.map_field['floaty'],
                             map_native.get('floaty'))
            self.assertEqual(actual.map_field['mapy']['baz'],
                             map_native.get('mapy').get('baz'))

    @pytest.mark.asyncio
    async def test_model_subclass_attributes_inherited_on_create(self):
        scope_args = {'count': 0}

        def fake_dynamodb(*args, **kwargs):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                raise ClientError({'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Not Found'}},
                                  "DescribeTable")
            return {}

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            await Dog.create_table(read_capacity_units=2, write_capacity_units=2)

            actual = req.call_args_list[1][0][1]

            self.assertEquals(actual['TableName'], DOG_TABLE_DATA['Table']['TableName'])
            self.assert_dict_lists_equal(actual['KeySchema'], DOG_TABLE_DATA['Table']['KeySchema'])
            self.assert_dict_lists_equal(actual['AttributeDefinitions'],
                                         DOG_TABLE_DATA['Table']['AttributeDefinitions'])


class ModelInitTestCase(TestCase):

    @pytest.mark.asyncio
    async def test_raw_map_attribute_with_dict_init(self):
        attribute = {
            'foo': 123,
            'bar': 'baz'
        }
        actual = await ExplicitRawMapModel.initialize(map_id=3, map_attr=attribute)
        self.assertEquals(actual.map_attr['foo'], attribute['foo'])

    @pytest.mark.asyncio
    async def test_raw_map_attribute_with_initialized_instance_init(self):
        attribute = {
            'foo': 123,
            'bar': 'baz'
        }
        initialized_instance = MapAttribute(**attribute)
        actual = await ExplicitRawMapModel.initialize(map_id=3, map_attr=initialized_instance)
        self.assertEquals(actual.map_attr['foo'], initialized_instance['foo'])
        self.assertEquals(actual.map_attr['foo'], attribute['foo'])

    @pytest.mark.asyncio
    async def test_subclassed_map_attribute_with_dict_init(self):
        attribute = {
            'make': 'Volkswagen',
            'model': 'Super Beetle'
        }
        expected_model = CarInfoMap(**attribute)
        actual = await CarModel.initialize(car_id=1, car_info=attribute)
        self.assertEquals(expected_model.make, actual.car_info.make)
        self.assertEquals(expected_model.model, actual.car_info.model)

    @pytest.mark.asyncio
    async def test_subclassed_map_attribute_with_initialized_instance_init(self):
        attribute = {
            'make': 'Volkswagen',
            'model': 'Super Beetle'
        }
        expected_model = CarInfoMap(**attribute)
        actual = await CarModel.initialize(car_id=1, car_info=expected_model)
        self.assertEquals(expected_model.make, actual.car_info.make)
        self.assertEquals(expected_model.model, actual.car_info.model)

    def _get_bin_tree(self, multiplier=1):
        return {
            'value': 5 * multiplier,
            'left': {
                'value': 2 * multiplier,
                'left': {
                    'value': 1 * multiplier
                },
                'right': {
                    'value': 3 * multiplier
                }
            },
            'right': {
                'value': 7 * multiplier,
                'left': {
                    'value': 6 * multiplier
                },
                'right': {
                    'value': 8 * multiplier
                }
            }
        }

    @pytest.mark.asyncio
    async def test_subclassed_map_attribute_with_map_attributes_member_with_dict_init(self):
        left = self._get_bin_tree()
        right = self._get_bin_tree(multiplier=2)
        actual = await TreeModel.initialize(tree_key='key', left=left, right=right)
        self.assertEquals(actual.left.left.right.value, 3)
        self.assertEquals(actual.left.left.value, 2)
        self.assertEquals(actual.right.right.left.value, 12)
        self.assertEquals(actual.right.right.value, 14)

    @pytest.mark.asyncio
    async def test_subclassed_map_attribute_with_map_attribute_member_with_initialized_instance_init(self):
        left = self._get_bin_tree()
        right = self._get_bin_tree(multiplier=2)
        left_instance = TreeLeaf(**left)
        right_instance = TreeLeaf(**right)
        actual = await TreeModel.initialize(tree_key='key', left=left_instance, right=right_instance)
        self.assertEquals(actual.left.left.right.value, left_instance.left.right.value)
        self.assertEquals(actual.left.left.value, left_instance.left.value)
        self.assertEquals(actual.right.right.left.value, right_instance.right.left.value)
        self.assertEquals(actual.right.right.value, right_instance.right.value)
