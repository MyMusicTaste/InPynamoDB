import imp
import logging
import os
from os import getenv

import aiohttp

log = logging.getLogger(__name__)

default_settings_dict = {
    'request_timeout_seconds': 60,
    'max_retry_attempts': 3,
    'base_backoff_ms': 25,
    'region': 'us-east-1',
    'session_cls': aiohttp.ClientSession,
    'allow_rate_limited_scan_without_consumed_capacity': False,
}

OVERRIDE_SETTINGS_PATH = getenv('INPYNAMODB_CONFIG', '/etc/inpynamodb/global_default_settings.py')

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings = imp.load_source('__inpynamodb_override_settings__', OVERRIDE_SETTINGS_PATH)
    log.info('Override settings for inpynamo available {0}'.format(OVERRIDE_SETTINGS_PATH))
else:
    log.info('Override settings for inpynamo not available {0}'.format(OVERRIDE_SETTINGS_PATH))
    log.info('Using Default settings value')


def get_settings_value(key):
    """
    Fetches the value from the override file.
    If the value is not present, then tries to fetch the values from constants.py
    """
    if hasattr(override_settings, key):
        return getattr(override_settings, key)

    if key in default_settings_dict:
        return default_settings_dict[key]

    return None
