"""
Test model API
"""
import base64
import copy
from datetime import datetime

import pytest as pytest
from aiobotocore import AioSession
from asynctest import patch, TestCase, CoroutineMock, fail_on, MagicMock
from botocore.exceptions import ClientError
from pynamodb.types import RANGE

from inpynamodb.attributes import MapAttribute, UnicodeAttribute, UTCDateTimeAttribute, NumberSetAttribute, UnicodeSetAttribute, \
    BinarySetAttribute, BooleanAttribute, NumberAttribute, BinaryAttribute, ListAttribute
from inpynamodb.indexes import AllProjection, IncludeProjection

from inpynamodb.constants import ITEM, STRING_SHORT, ATTRIBUTES, REQUEST_ITEMS, KEYS, UNPROCESSED_KEYS, RESPONSES, \
    BINARY_SHORT, DEFAULT_ENCODING, UNPROCESSED_ITEMS
from inpynamodb.indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from inpynamodb.models import Model
from inpynamodb.tests.pynamodb_tests.data import MODEL_TABLE_DATA, SIMPLE_MODEL_TABLE_DATA, \
    CUSTOM_ATTR_NAME_INDEX_TABLE_DATA, GET_MODEL_ITEM_DATA, COMPLEX_TABLE_DATA, COMPLEX_ITEM_DATA, CAR_MODEL_TABLE_DATA, \
    SIMPLE_BATCH_GET_ITEMS, BATCH_GET_ITEMS, INDEX_TABLE_DATA, LOCAL_INDEX_TABLE_DATA
from inpynamodb.tests.pynamodb_tests.deep_eq import deep_eq

PATCH_METHOD = 'aiobotocore.client.AioBaseClient._make_api_call'


class GamePlayerOpponentIndex(LocalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GamePlayerOpponentIndex"
        host = "http://localhost:8000"
        projection = AllProjection()

    player_id = UnicodeAttribute(hash_key=True)
    winner_id = UnicodeAttribute(range_key=True)


class GameOpponentTimeIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameOpponentTimeIndex"
        host = "http://localhost:8000"
        projection = AllProjection()

    winner_id = UnicodeAttribute(hash_key=True)
    created_time = UnicodeAttribute(range_key=True)


class GameModel(Model):
    class Meta:
        read_capacity_units = 1
        write_capacity_units = 1
        table_name = "GameModel"
        host = "http://localhost:8000"

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

    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class RegionSpecificModel(Model):
    """
    A testing model
    """

    class Meta:
        region = 'us-west-1'
        table_name = 'RegionSpecificModel'

    user_name = UnicodeAttribute(hash_key=True)
    user_id = UnicodeAttribute(range_key=True)


class ComplexKeyModel(Model):
    """
    This model has a key that must be serialized/deserialized properly
    """

    class Meta:
        table_name = 'ComplexKey'

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

    person = Person(attr_name='weird_person')
    key = NumberAttribute(hash_key=True)


class OfficeEmployee(Model):
    class Meta:
        table_name = 'OfficeEmployeeModel'

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

    car_id = NumberAttribute(null=False)
    car_info = CarInfoMap(null=False)


class CarModelWithNull(Model):
    class Meta:
        table_name = 'CarModelWithNull'

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

    store_name = UnicodeAttribute(hash_key=True)
    groceries = ListAttribute()


class Office(Model):
    class Meta:
        table_name = 'OfficeModel'

    office_id = NumberAttribute(hash_key=True)
    address = Location()
    employees = ListAttribute(of=OfficeEmployeeMap)


class BooleanConversionModel(Model):
    class Meta:
        table_name = 'BooleanConversionTable'

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

    tree_key = UnicodeAttribute(hash_key=True)
    left = TreeLeaf()
    right = TreeLeaf()


class ExplicitRawMapModel(Model):
    class Meta:
        table_name = 'ExplicitRawMapModel'

    map_id = NumberAttribute(hash_key=True, default=123)
    map_attr = MapAttribute()


class MapAttrSubClassWithRawMapAttr(MapAttribute):
    num_field = NumberAttribute()
    str_field = UnicodeAttribute()
    map_field = MapAttribute()


class ExplicitRawMapAsMemberOfSubClass(Model):
    class Meta:
        table_name = 'ExplicitRawMapAsMemberOfSubClass'

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
    async def test_overidden_session(self):
        """
        Custom session
        """
        fake_db = CoroutineMock()

        with patch(PATCH_METHOD, new=fake_db):
            with patch("inpynamodb.connection.TableConnection.describe_table") as req:
                req.return_value = None
                await OverriddenSessionModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        self.assertEqual(OverriddenSessionModel.Meta.request_timeout_seconds, 9999)
        self.assertEqual(OverriddenSessionModel.Meta.max_retry_attempts, 200)
        self.assertEqual(OverriddenSessionModel.Meta.base_backoff_ms, 4120)
        self.assertTrue(OverriddenSessionModel.Meta.session_cls is OverriddenSession)

        self.assertEqual(OverriddenSessionModel._connection.connection._request_timeout_seconds, 9999)
        self.assertEqual(OverriddenSessionModel._connection.connection._max_retry_attempts_exception, 200)
        self.assertEqual(OverriddenSessionModel._connection.connection._base_backoff_ms, 4120)
        self.assertTrue(type(OverriddenSessionModel._connection.connection.requests_session) is OverriddenSession)

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

    # @pytest.mark.asyncio
    # async def test_index_multipage_count(self):
    #     with patch(PATCH_METHOD) as req:
    #         last_evaluated_key = {
    #             'user_name': {'S': u'user'},
    #             'user_id': {'S': '1234'},
    #         }
    #         req.side_effect = [
    #             {'Count': 1000, 'LastEvaluatedKey': last_evaluated_key},
    #             {'Count': 42}
    #         ]
    #         res = await CustomAttrNameModel.uid_index.count('foo')
    #         self.assertEqual(res, 1042)
    #
    #         args_one = req.call_args_list[0][0][1]
    #         params_one = {
    #             'KeyConditionExpression': '#0 = :0',
    #             'ExpressionAttributeNames': {
    #                 '#0': 'user_id'
    #             },
    #             'ExpressionAttributeValues': {
    #                 ':0': {
    #                     'S': u'foo'
    #                 }
    #             },
    #             'IndexName': 'uid_index',
    #             'TableName': 'CustomAttrModel',
    #             'ReturnConsumedCapacity': 'TOTAL',
    #             'Select': 'COUNT'
    #         }
    #
    #         args_two = req.call_args_list[1][0][1]
    #         params_two = copy.deepcopy(params_one)
    #         params_two['ExclusiveStartKey'] = last_evaluated_key
    #
    #         deep_eq(args_one, params_one, _assert=True)
    #         deep_eq(args_two, params_two, _assert=True)

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

            for item in await LocalIndexedModel.email_index.query(
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

            for item in await CustomAttrNameModel.uid_index.query('foo', limit=2, overidden_user_name__begins_with='bar'):
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
