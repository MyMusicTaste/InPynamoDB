"""
Test model API
"""
from datetime import datetime

import aiobotocore
import aiohttp
import copy

import pytest
from botocore.client import ClientError
from asynctest import MagicMock, patch, TestCase, CoroutineMock
from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute, NumberSetAttribute, BinarySetAttribute, \
    UnicodeSetAttribute, NumberAttribute, BooleanAttribute, BinaryAttribute, MapAttribute, ListAttribute
from pynamodb.constants import ITEM, STRING_SHORT, ATTRIBUTES
from pynamodb.exceptions import TableError
from pynamodb.indexes import AllProjection, IncludeProjection
from pynamodb.tests.data import MODEL_TABLE_DATA, SIMPLE_MODEL_TABLE_DATA, CUSTOM_ATTR_NAME_INDEX_TABLE_DATA, \
    GET_MODEL_ITEM_DATA, COMPLEX_TABLE_DATA, COMPLEX_ITEM_DATA, CAR_MODEL_TABLE_DATA
from pynamodb.tests.deep_eq import deep_eq

from inpynamodb.indexes import LocalSecondaryIndex, GlobalSecondaryIndex
from inpynamodb.models import Model


PATCH_METHOD = 'inpynamodb.connection.AsyncConnection._make_api_call'


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

    overidden_user_id = UnicodeAttribute(hash_key=True, attr_name='user_id')


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


class OverriddenSession(aiohttp.ClientSession):
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
        aws_access_key_id = 'access_key_id'
        aws_secret_access_key = 'secret_access_key'
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
    def init_table_meta(model_clz, table_data):
        with patch(PATCH_METHOD) as req:
            req.return_value = table_data
            model_clz._get_meta_data()

    def assert_dict_lists_equal(self, list1, list2):
        """
        Compares two lists of dictionaries
        This function allows both the lists and dictionaries to have any order
        """
        if len(list1) != len(list2):
            raise AssertionError("Values not equal: {0} {1}".format(list1, list2))
        for d1_item in list1:
            found = False
            for d2_item in list2:
                if d2_item == d1_item:
                    found = True
            if not found:
                raise AssertionError("Values not equal: {0} {1}".format(list1, list2))

    async def test_create_model(self):
        """
        Model.create_table
        """
        self.maxDiff = None
        scope_args = {'count': 0}

        async def fake_dynamodb(*args):
            kwargs = args[1]
            if kwargs == {'TableName': UserModel.Meta.table_name}:
                if scope_args['count'] == 0:
                    return {}
                else:
                    return MODEL_TABLE_DATA
            else:
                return {}

        fake_db = MagicMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db):
            with patch("inpynamodb.connection.TableConnection.describe_table") as req:
                req.return_value = None
                with pytest.raises(TableError):
                    await UserModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        with patch(PATCH_METHOD, new=fake_db) as req:
            await UserModel.create_table(read_capacity_units=2, write_capacity_units=2)

        # Test for default region
        self.assertEqual(UserModel.Meta.region, 'us-east-1')
        self.assertEqual(UserModel.Meta.request_timeout_seconds, 60)
        self.assertEqual(UserModel.Meta.max_retry_attempts, 3)
        self.assertEqual(UserModel.Meta.base_backoff_ms, 25)
        self.assertTrue(UserModel.Meta.session_cls is aiobotocore.AioSession)

        self.assertEqual(UserModel._connection.connection._request_timeout_seconds, 60)
        self.assertEqual(UserModel._connection.connection._max_retry_attempts_exception, 3)
        self.assertEqual(UserModel._connection.connection._base_backoff_ms, 25)

        self.assertTrue(type(UserModel._connection.connection.requests_session) is aiobotocore.AioSession)

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

        async def fake_wait(*obj, **kwargs):
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

        async def bad_server(*args):
            if scope_args['count'] == 0:
                scope_args['count'] += 1
                return {}
            elif scope_args['count'] == 1 or scope_args['count'] == 2:
                return {}

        bad_mock_server = CoroutineMock()
        bad_mock_server.side_effect = bad_server

        scope_args = {'count': 0}
        with patch(PATCH_METHOD, new=bad_mock_server) as req:
            with pytest.raises(TableError):
                await UserModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

    async def test_model_attrs(self):
        """
        Model()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            async with UserModel('foo', 'bar', save_on_exit=False) as item:
                self.assertEqual(item.email, 'needs_email')
                self.assertEqual(item.callable_field, 42)
                self.assertEqual(
                    repr(item), '{0}<{1}, {2}>'.format(UserModel.Meta.table_name, item.custom_user_name, item.user_id)
                )
                self.assertEqual(repr(await UserModel._get_meta_data()), 'MetaTable<{0}>'.format('Thread'))

        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            async with SimpleUserModel('foo', save_on_exit=False) as item:
                self.assertEqual(repr(item), f'{SimpleUserModel.Meta.table_name}<{item.user_name}>')
                with pytest.raises(ValueError):
                    await item.save()
        with pytest.raises(ValueError):
            await UserModel.from_raw_data(None)

        with patch(PATCH_METHOD) as req:
            req.return_value = CUSTOM_ATTR_NAME_INDEX_TABLE_DATA
            async with CustomAttrNameModel('foo', 'bar', overidden_attr='test') as item:
                self.assertEqual(item.overidden_attr, 'test')
                self.assertTrue(not hasattr(item, 'foo_attr'))

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

    async def test_overidden_session(self):
        """
        Custom session
        """
        fake_db = CoroutineMock()

        with patch(PATCH_METHOD, new=fake_db):
            with patch("inpynamodb.connection.TableConnection.describe_table") as req:
                req.return_value = None
                with pytest.raises(TableError):
                    await OverriddenSessionModel.create_table(read_capacity_units=2, write_capacity_units=2, wait=True)

        self.assertEqual(OverriddenSessionModel.Meta.request_timeout_seconds, 9999)
        self.assertEqual(OverriddenSessionModel.Meta.max_retry_attempts, 200)
        self.assertEqual(OverriddenSessionModel.Meta.base_backoff_ms, 4120)
        self.assertEqual(OverriddenSessionModel.Meta.aws_access_key_id, 'access_key_id')
        self.assertEqual(OverriddenSessionModel.Meta.aws_secret_access_key, 'secret_access_key')
        self.assertTrue(OverriddenSessionModel.Meta.session_cls is OverriddenSession)

        self.assertEqual(OverriddenSessionModel._connection.connection._request_timeout_seconds, 9999)
        self.assertEqual(OverriddenSessionModel._connection.connection._max_retry_attempts_exception, 200)
        self.assertEqual(OverriddenSessionModel._connection.connection._base_backoff_ms, 4120)
        self.assertTrue(type(OverriddenSessionModel._connection.connection.requests_session) is OverriddenSession)

    async def test_overridden_attr_name(self):
        async with UserModel(custom_user_name="bob") as user:
            self.assertEqual(user.custom_user_name, "bob")
            self.assertRaises(AttributeError, getattr, user, "user_name")

            with pytest.raises(ValueError):
                async with UserModel(user_name="bob"):
                    pass

            with pytest.raises(ValueError):
                await CustomAttrNameModel.query("bob", foo_attr="bar")

    async def test_refresh(self):
        """
        Model.refresh
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            async with UserModel('foo', 'bar') as item:
                with patch(PATCH_METHOD) as req:
                    req.return_value = {}
                    with pytest.raises(item.DoesNotExist):
                        await item.refresh()

                with patch(PATCH_METHOD) as req:
                    req.return_value = GET_MODEL_ITEM_DATA
                    item.picture = b'to-be-removed'
                    await item.refresh()
                    self.assertEqual(
                        item.custom_user_name,
                        GET_MODEL_ITEM_DATA.get(ITEM).get('user_name').get(STRING_SHORT))
                    self.assertIsNone(item.picture)

    async def test_complex_key(self):
        """
        Model with complex key
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = COMPLEX_TABLE_DATA
            async with ComplexKeyModel('test') as item:
                with patch(PATCH_METHOD) as req:
                    req.return_value = COMPLEX_ITEM_DATA
                    await item.refresh()

    async def test_delete(self):
        """
        Model.delete
        """
        UserModel._meta_table = None
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            async with UserModel('foo', 'bar') as item:
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

    async def test_delete_doesnt_do_validation_on_null_attributes(self):
        """
        Model.delete
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = CAR_MODEL_TABLE_DATA
            async with CarModel('foo') as item:
                await item.delete()

        with patch(PATCH_METHOD) as req:
            req.return_value = CAR_MODEL_TABLE_DATA
            async with CarModel.batch_write() as batch:
                async with CarModel('foo') as car:
                    await batch.delete(car)

    async def test_update(self):
        """
        Model.update
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            async with SimpleUserModel('foo', is_active=True, email='foo@example.com', signature='foo') as item:
                with patch(PATCH_METHOD) as req:
                    req.return_value = {}
                    await item.save()

                with pytest.raises(TypeError):
                    await item.update(['not', 'a', 'dict'])
                with pytest.raises(TypeError):
                    await item.update(actions={'not': 'a list'})

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

                    await item.update(actions=[
                        SimpleUserModel.email.set('foo@example.com'),
                        SimpleUserModel.views.remove(),
                        SimpleUserModel.is_active.set(None),
                        SimpleUserModel.signature.set(None),
                        SimpleUserModel.custom_aliases.set(['bob']),
                        SimpleUserModel.numbers.delete(0, 1)
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
                        'UpdateExpression': 'SET #0 = :0, #1 = :1, #2 = :2, #3 = :3 REMOVE #4 DELETE #5 :4',
                        'ExpressionAttributeNames': {
                            '#0': 'email',
                            '#1': 'is_active',
                            '#2': 'signature',
                            '#3': 'aliases',
                            '#4': 'views',
                            '#5': 'numbers'
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
                                'SS': ['bob']
                            },
                            ':4': {
                                'NS': ['0', '1']
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
                                'SS': {'bob'}
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
                    self.assertEquals(set(['bob']), item.custom_aliases)

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

    async def test_update_item(self):
        """
        Model.update_item
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = SIMPLE_MODEL_TABLE_DATA
            item = await SimpleUserModel('foo', email='bar').__aenter__()

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
            with pytest.raises(ValueError):
                await item.update_item('views', 10)

        with pytest.raises(ValueError):
            await item.update_item('nonexistent', 5)
        with pytest.raises(ValueError):
            await item.update_item('views', 10, action='add', nonexistent__not_contains='-')

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
            user = await UserModel("test_hash", "test_range").__aenter__()
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
            await item.update_item('views', 10, action='add',
                                   condition=SimpleUserModel.email.is_in('1@pynamo.db','2@pynamo.db'))
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

    async def test_save(self):
        """
        Model.save
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = MODEL_TABLE_DATA
            item = await UserModel('foo', 'bar').__aenter__()

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

    async def test_filter_count(self):
        """
        Model.count(**filters)
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 10, 'ScannedCount': 20}
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

    async def test_count(self):
        """
        Model.count()
        """

        async def fake_dynamodb(*args, **kwargs):
            return MODEL_TABLE_DATA

        fake_db = CoroutineMock()
        fake_db.side_effect = fake_dynamodb

        with patch(PATCH_METHOD, new=fake_db) as req:
            res = await UserModel.count()
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
            params = {'TableName': 'UserModel'}
            self.assertEqual(args, params)

    async def test_count_no_hash_key(self):
        with pytest.raises(ValueError):
            await UserModel.count(zip_code__le='94117')

    async def test_index_count(self):
        """
        Model.index.count()
        """
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 42, 'ScannedCount': 42}
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

        # Note: this test is incorrect as uid_index does not have a range key
        with patch(PATCH_METHOD) as req:
            req.return_value = {'Count': 42, 'ScannedCount': 42}
            res = await CustomAttrNameModel.uid_index.count('foo', limit=2, overidden_user_name__begins_with='bar')
            self.assertEqual(res, 42)
            args = req.call_args[0][1]
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
                'Limit': 2,
                'IndexName': 'uid_index',
                'TableName': 'CustomAttrModel',
                'ReturnConsumedCapacity': 'TOTAL',
                'Select': 'COUNT'
            }
            deep_eq(args, params, _assert=True)