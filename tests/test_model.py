from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.models import Model

import tests.test_settings as settings


class TestModel(Model):
    class Meta:
        table_name = 'test_model'
        host = settings.DYNAMODB_HOST

    temp_id = NumberAttribute(hash_key=True)
    name = UnicodeAttribute()
    description = UnicodeAttribute()
