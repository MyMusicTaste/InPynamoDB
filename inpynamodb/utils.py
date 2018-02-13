from inpynamodb.attributes import UTCDateTimeAttribute, MapAttribute
from dateutil import parser as DateutilParser


def generate_update_attributes(attribute_container, update_data):
    update_actions = list()

    for key, value in update_data.items():
        attr = getattr(attribute_container, key)
        if isinstance(attr, UTCDateTimeAttribute):
            update_actions.append(attr.set(DateutilParser.parse(value, ignoretz=False)))
        elif isinstance(attr, MapAttribute):
            update_actions.extend(generate_update_attributes(attr, value))
        else:
            update_actions.append(attr.set(value))

    return update_actions
