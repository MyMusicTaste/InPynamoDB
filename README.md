# InPynamoDB

This transforms [PynamoDB](https://github.com/pynamodb/PynamoDB)'s basic methods working asynchronously used [aiobotocore](https://github.com/aio-libs/aiobotocore).

If you find any bugs of suggestions, please leave issue.

There's no main documentation yet, for the time being, you can refer to [PynamoDB documentation](http://pynamodb.readthedocs.io).

## Requirements
- Python 3.6 and above for this library is using `async/await` keyword.

## Installation
$ pip install InPynamoDB

## Basic Usage

- Declare model

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

- GET

```python
user = await UserModel.get(hash_key="John", range_key="Doe")
```

- UPDATE

```python
await user.update()
```