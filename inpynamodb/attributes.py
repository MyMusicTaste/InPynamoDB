import uuid

from pynamodb.attributes import AttributeContainer as PynamoDBAttributeContainer, UnicodeAttribute
from pynamodb.attributes import MapAttribute as PynamoDBMapAttribute


class AttributeContainer(PynamoDBAttributeContainer):
    def as_dict(self, attributes_to_get=None, include_none=True):
        result = {}

        if include_none:
            if attributes_to_get is None:
                for k in self._get_attributes().keys():
                    attr_value = self.__getattribute__(k)
                    if isinstance(attr_value, MapAttribute):
                        result[k] = attr_value.as_dict(include_none=include_none)
                    else:
                        result[k] = attr_value
            else:
                for k, v in self._get_attributes().items():
                    if k in attributes_to_get:
                        attr_value = self.__getattribute__(k)
                        if isinstance(v, MapAttribute):
                            result[k] = attr_value.as_dict(include_none=include_none)
                        else:
                            result[k] = v

        if attributes_to_get is None:
            for key, value in self.attribute_values.items():
                result[key] = value.as_dict() if isinstance(value, MapAttribute) else value
        else:
            for key, value in self.attribute_values.items():
                if key in attributes_to_get:
                    result[key] = value.as_dict() if isinstance(value, MapAttribute) else value

        return result


class UUIDAttribute(UnicodeAttribute):
    def __init__(self, hash_key=False, range_key=False, null=None,
                 default=None, uuid_version=1, attr_name=None):
        """
        :param hash_key: Indicates that this attribute is hash key of model.
        :param range_key: Indicates that this attribute is range key of model.
        :param null: Indicate this attribute is nullable.
        :param default: Default value of this attribute.
                        If auto == True, this value will be ignored because UUID will be generated as default.
        :param uuid_version: UUID version which this attribute will use. Only supports 1 and 4.
        """
        self.uuid_version = uuid_version

        super(UUIDAttribute, self).__init__(hash_key=hash_key, range_key=range_key,
                                            null=null, default=default, attr_name=attr_name)

    def serialize(self, value):
        if isinstance(value, uuid.UUID):
            value_str = str(value)

            uuid.UUID(value_str, version=self.uuid_version)
            return super(UUIDAttribute, self).serialize(value_str)
        else:
            return super(UUIDAttribute, self).serialize(value)


class MapAttribute(PynamoDBMapAttribute):
    """
    A Map Attribute

    The MapAttribute class can be used to store a JSON document as "raw" name-value pairs, or
    it can be subclassed and the document fields represented as class attributes using Attribute instances.

    To support the ability to subclass MapAttribute and use it as an AttributeContainer, instances of
    MapAttribute behave differently based both on where they are instantiated and on their type.
    Because of this complicated behavior, a bit of an introduction is warranted.

    Models that contain a MapAttribute define its properties using a class attribute on the model.
    For example, below we define "MyModel" which contains a MapAttribute "my_map":

    class MyModel(Model):
       my_map = MapAttribute(attr_name="dynamo_name", default={})

    When instantiated in this manner (as a class attribute of an AttributeContainer class), the MapAttribute
    class acts as an instance of the Attribute class. The instance stores data about the attribute (in this
    example the dynamo name and default value), and acts as a data descriptor, storing any value bound to it
    on the `attribute_values` dictionary of the containing instance (in this case an instance of MyModel).

    Unlike other Attribute types, the value that gets bound to the containing instance is a new instance of
    MapAttribute, not an instance of the primitive type. For example, a UnicodeAttribute stores strings in
    the `attribute_values` of the containing instance; a MapAttribute does not store a dict but instead stores
    a new instance of itself. This difference in behavior is necessary when subclassing MapAttribute in order
    to access the Attribute data descriptors that represent the document fields.

    For example, below we redefine "MyModel" to use a subclass of MapAttribute as "my_map":

    class MyMapAttribute(MapAttribute):
        my_internal_map = MapAttribute()

    class MyModel(Model):
        my_map = MyMapAttribute(attr_name="dynamo_name", default = {})

    In order to set the value of my_internal_map on an instance of MyModel we need the bound value for "my_map"
    to be an instance of MapAttribute so that it acts as a data descriptor:

    MyModel().my_map.my_internal_map = {'foo': 'bar'}

    That is the attribute access of "my_map" must return a MyMapAttribute instance and not a dict.

    When an instance is used in this manner (bound to an instance of an AttributeContainer class),
    the MapAttribute class acts as an AttributeContainer class itself. The instance does not store data
    about the attribute, and does not act as a data descriptor. The instance stores name-value pairs in its
    internal `attribute_values` dictionary.

    Thus while MapAttribute multiply inherits from Attribute and AttributeContainer, a MapAttribute instance
    does not behave as both an Attribute AND an AttributeContainer. Rather an instance of MapAttribute behaves
    EITHER as an Attribute OR as an AttributeContainer, depending on where it was instantiated.

    So, how do we create this dichotomous behavior? Using the AttributeContainerMeta metaclass.
    All MapAttribute instances are initialized as AttributeContainers only. During construction of
    AttributeContainer classes (subclasses of MapAttribute and Model), any instances that are class attributes
    are transformed from AttributeContainers to Attributes (via the `_make_attribute` method call).
    """

    def as_dict(self, attributes_to_get=None, include_none=True):
        result = {}

        if include_none:
            if attributes_to_get is None:
                for k in self._get_attributes().keys():
                    attr_value = self.__getattribute__(k)
                    if isinstance(attr_value, MapAttribute):
                        result[k] = attr_value.as_dict(include_none=include_none)
                    else:
                        result[k] = attr_value
            else:
                for k, v in self._get_attributes().items():
                    if k in attributes_to_get:
                        attr_value = self.__getattribute__(k)
                        if isinstance(v, MapAttribute):
                            result[k] = attr_value.as_dict(include_none=include_none)
                        else:
                            result[k] = v

        if attributes_to_get is None:
            for key, value in self.attribute_values.items():
                result[key] = value.as_dict() if isinstance(value, MapAttribute) else value
        else:
            for key, value in self.attribute_values.items():
                if key in attributes_to_get:
                    result[key] = value.as_dict() if isinstance(value, MapAttribute) else value

        return result
