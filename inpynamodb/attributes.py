from pynamodb.attributes import Attribute, MapAttribute
from pynamodb.compat import getmembers_issubclass


class AttributeContainerMeta(type):

    def __init__(cls, name, bases, attrs):
        super(AttributeContainerMeta, cls).__init__(name, bases, attrs)
        AttributeContainerMeta._initialize_attributes(cls)

    @staticmethod
    def _initialize_attributes(cls):
        """
        Initialize attributes on the class.
        """
        cls._attributes = {}
        cls._dynamo_to_python_attrs = {}

        for name, attribute in getmembers_issubclass(cls, Attribute):
            initialized = False
            if isinstance(attribute, MapAttribute):
                # MapAttribute instances that are class attributes of an AttributeContainer class
                # should behave like an Attribute instance and not an AttributeContainer instance.
                initialized = attribute._make_attribute()

            cls._attributes[name] = attribute
            if attribute.attr_name is not None:
                cls._dynamo_to_python_attrs[attribute.attr_name] = name
            else:
                attribute.attr_name = name

            if initialized and isinstance(attribute, MapAttribute):
                # To support creating expressions from nested attributes, MapAttribute instances
                # store local copies of the attributes in cls._attributes with `attr_path` set.
                # Prepend the `attr_path` lists with the dynamo attribute name.
                attribute._update_attribute_paths(attribute.attr_name)
