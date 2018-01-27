# InPynamoDB
![coverage_badge](./coverage.svg)

This transforms [PynamoDB](https://github.com/pynamodb/PynamoDB)'s basic methods working asynchronously used [aiobotocore](https://github.com/aio-libs/aiobotocore).

THIS LIBRARY IS NOT STABLIZED YET!!
If you find any bugs of suggestions, please leave issue.

There's no main documentation yet, for the time being, you can refer to [PynamoDB documentation](http://pynamodb.readthedocs.io).

## Requirements
- Python 3.6 and above

## Installation
$ pip install InPynamoDB

## Basic Usage

```python
from inpynamodb.models import Model
from inpynamodb.attributes import UnicodeAttribute

class UserModel(Model):
    """
    A DynamoDB User
    """
    class Meta:
        table_name = "dynamodb-user"
    email = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(range_key=True)
    last_name = UnicodeAttribute(hash_key=True)
```

InPynamoDB allows you to create the table if needed (it must exist before you can use it!):
```python
await UserModel.create_table(read_capacity_units=1, write_capacity_units=1)
```

Create a new user:
```python
async def saving_user():
    user = await UserModel.initialize("John", "Denver")  # SHOULD USE METHOD 'initialize()' TO MAKE MODEL.
    user.email = "djohn@company.org"
    await user.save()
```

Now, search your table for all users with a last name of 'John' and whose first name begins with 'D':
```python
async for user in await UserModel.query("John", first_name__begins_with="D"):
    print(user.first_name)
```

Examples of ways to query your table with filter conditions:
```python
async for user in await UserModel.query("John", filter_condition= (UserModel.email=="djohn@company.org")):
    print(user.first_name)
async for user in await UserModel.query("John", UserModel.email=="djohn@company.org"):
    print(user.first_name)
# Deprecated, use UserModel.email=="djohn@company.org" instead
async for user in await UserModel.query("John", email__eq="djohn@company.org"):
    print(user.first_name)
```

Retrieve an existing user:
```python
try:
    user = await UserModel.get("John", "Denver")
    print(user)
except UserModel.DoesNotExist:
    print("User does not exist")
```

## Advanced Usage
Want to use indexes? No problem:
```python
from inpynamodb.models import Model
from inpynamodb.indexes import GlobalSecondaryIndex, AllProjection
from inpynamodb.attributes import NumberAttribute, UnicodeAttribute

class ViewIndex(GlobalSecondaryIndex):
    class Meta:
        read_capacity_units = 2
        write_capacity_units = 1
        projection = AllProjection()
    view = NumberAttribute(default=0, hash_key=True)

class TestModel(Model):
    class Meta:
        table_name = "TestModel"
    forum = UnicodeAttribute(hash_key=True)
    thread = UnicodeAttribute(range_key=True)
    view = NumberAttribute(default=0)
    view_index = ViewIndex()
```

Now query the index for all items with 0 views:
```python
async for item in TestModel.view_index.query(0):
    print("Item queried from index: {0}".format(item))
```

It's really that simple.

Want to use DynamoDB local? Just add a host name attribute and specify your local server.
```python
from inpynamodb.models import Model
from inpynamodb.attributes import UnicodeAttribute

class UserModel(Model):
    """
    A DynamoDB User
    """
    class Meta:
        table_name = "dynamodb-user"
        host = "http://localhost:8000"
    email = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(range_key=True)
    last_name = UnicodeAttribute(hash_key=True)
```

Want to enable streams on a table? Just add a stream_view_type name attribute and specify the type of data you'd like to stream. (NOT TESTED YET)
```python
from inpynamodb.models import Model
from inpynamodb.attributes import UnicodeAttribute
from inpynamodb.constants import STREAM_NEW_AND_OLD_IMAGE

class AnimalModel(Model):
    """
    A DynamoDB Animal
    """
    class Meta:
        table_name = "dynamodb-user"
        host = "http://localhost:8000"
        stream_view_type = STREAM_NEW_AND_OLD_IMAGE
    type = UnicodeAttribute(null=True)
    name = UnicodeAttribute(range_key=True)
    id = UnicodeAttribute(hash_key=True)
```

Want to backup and restore a table? No problem. (NOT TESTED YET)
```python
# Backup the table
UserModel.dump("usermodel_backup.json")

# Restore the table
UserModel.load("usermodel_backup.json")
```
