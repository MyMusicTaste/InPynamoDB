import uuid
from pynamodb.attributes import Attribute as _Attribute, AttributeContainer as _AttributeContainer, \
    AttributeContainerMeta, UnicodeAttribute, MapAttribute
from inpynamodb.exceptions import InvalidParamException


class Attribute(_Attribute):
    """
    An attribute of a model
    """
    attr_type = None
    null = False

    def __init__(self,
                 hash_key=False,
                 range_key=False,
                 defaultable=True,
                 null=None,
                 default=None,
                 attr_name=None
                 ):
        self.defaultable = defaultable
        super(Attribute, self).__init__(hash_key=hash_key, range_key=range_key,
                                        null=null, default=default, attr_name=attr_name)


class AttributeContainer(_AttributeContainer, metaclass=AttributeContainerMeta):

    def __init__(self, **attributes):
        # The `attribute_values` dictionary is used by the Attribute data descriptors in cls._attributes
        # to store the values that are bound to this instance. Attributes store values in the dictionary
        # using the `python_attr_name` as the dictionary key. "Raw" (i.e. non-subclassed) MapAttribute
        # instances do not have any Attributes defined and instead use this dictionary to store their
        # collection of name-value pairs.
        self.attribute_values = {}
        self._set_defaults()
        self._set_attributes(**attributes)

    @classmethod
    def _get_attributes(cls):
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.

        :rtype: dict[str, Attribute]
        """
        return cls._attributes

    def set_defaults(self):
        """
        Sets and fields that provide a default value
        """
        for name, attr in self._get_attributes().items():
            default = attr.default
            defaultable = attr.defaultable

            if not defaultable:
                continue
            elif callable(default):
                value = default()
                if isinstance(value, uuid.UUID):
                    value = str(value)
            else:
                value = default
            setattr(self, name, value)

    def as_dict(self, attributes_to_get=None):
        result = {}

        if attributes_to_get is None:
            for key, value in self.attribute_values.items():
                result[key] = value.as_dict() if isinstance(value, MapAttribute) else value
        else:
            for key, value in self.attribute_values.items():
                if key in attributes_to_get:
                    result[key] = value.as_dict() if isinstance(value, MapAttribute) else value

        return result


class UUIDAttribute(UnicodeAttribute):
    def __init__(self, hash_key=False, range_key=False, null=None, defaultable=True,
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

        super(UUIDAttribute, self).__init__(hash_key=hash_key, range_key=range_key, defaultable=defaultable,
                                            null=null, default=default, attr_name=attr_name)

    def serialize(self, value):
        if isinstance(value, uuid.UUID):
            value_str = str(value)
            try:
                uuid.UUID(value_str, version=self.uuid_version)
                return super(UUIDAttribute, self).serialize(value_str)
            except ValueError:
                raise InvalidParamException(cause="Value is not correct UUID.")
        else:
            return super(UUIDAttribute, self).serialize(value)
