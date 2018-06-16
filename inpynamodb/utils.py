import collections

from pynamodb.attributes import UTCDateTimeAttribute

from inpynamodb.attributes import MapAttribute
from dateutil import parser as DateutilParser


def generate_update_attributes(attribute_container, update_data):
    update_actions = list()
    for key, value in update_data.items():
        attr = getattr(attribute_container, key)
        if attr.is_hash_key or attr.is_range_key:
            continue
        if value is None:
            update_actions.append(attr.remove())
            continue
        if isinstance(attr, UTCDateTimeAttribute):
            if isinstance(value, str) or isinstance(value, int):
                update_actions.append(attr.set(DateutilParser.parse(value, ignoretz=False)))
            else:
                update_actions.append(attr.set(value))

        elif isinstance(attr, MapAttribute):
            update_actions.extend(generate_update_attributes(attr, value))
        else:
            update_actions.append(attr.set(value))

    return update_actions


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.items():
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], collections.Mapping)):
            dict_merge(dct[k], merge_dct[k])
        else:
            dct[k] = merge_dct[k]