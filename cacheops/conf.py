# -*- coding: utf-8 -*-
from contextlib import contextmanager
from copy import deepcopy
import warnings
import six
import redis
from funcy import memoize, decorator, identity, is_tuple, merge

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured


ALL_OPS = ('get', 'fetch', 'count', 'exists')

LOCK_TIMEOUT = 60


profile_defaults = {
    'ops': (),
    'local_get': False,
    'db_agnostic': True,
    'lock': False,
}
# NOTE: this is a compatibility for old style config,
# TODO: remove in cacheops 3.0
profiles = {
    'just_enable': {},
    'all': {'ops': ALL_OPS},
    'get': {'ops': ('get',)},
    'count': {'ops': ('count',)},
}
for key in profiles:
    profiles[key] = dict(profile_defaults, **profiles[key])


LRU = getattr(settings, 'CACHEOPS_LRU', False)
DEGRADE_ON_FAILURE = getattr(settings, 'CACHEOPS_DEGRADE_ON_FAILURE', False)


# Support DEGRADE_ON_FAILURE
if DEGRADE_ON_FAILURE:
    @decorator
    def handle_connection_failure(call):
        try:
            return call()
        except redis.ConnectionError as e:
            warnings.warn("The cacheops cache is unreachable! Error: %s" % e, RuntimeWarning)
        except redis.TimeoutError as e:
            warnings.warn("The cacheops cache timed out! Error: %s" % e, RuntimeWarning)
else:
    handle_connection_failure = identity

class CacheopsRedis(redis.StrictRedis):
    get = handle_connection_failure(redis.StrictRedis.get)

    @contextmanager
    def getting(self, key, lock=False):
        if not lock:
            yield self.get(key)
        else:
            locked = False
            try:
                data = self._get_or_lock(key)
                locked = data is None
                yield data
            finally:
                if locked:
                    self._release_lock(key)

    @handle_connection_failure
    def _get_or_lock(self, key):
        self._lock = getattr(self, '_lock', self.register_script("""
            local locked = redis.call('set', KEYS[1], 'LOCK', 'nx', 'ex', ARGV[1])
            if locked then
                redis.call('del', KEYS[2])
            end
            return locked
        """))
        signal_key = key + ':signal'

        while True:
            data = self.get(key)
            if data is None:
                if self._lock(keys=[key, signal_key], args=[LOCK_TIMEOUT]):
                    return None
            elif data != 'LOCK':
                return data

            # No data and not locked, wait
            self.brpoplpush(signal_key, signal_key, timeout=LOCK_TIMEOUT)

    @handle_connection_failure
    def _release_lock(self, key):
        self._unlock = getattr(self, '_unlock', self.register_script("""
            if redis.call('get', KEYS[1]) == 'LOCK' then
                redis.del(KEYS[1])
            end
            redis.call('lpush', KEYS[2], 1)
            redis.call('expire', KEYS[2], 1)
        """))
        signal_key = key + ':signal'
        self._unlock(keys=[key, signal_key])


class LazyRedis(object):
    def _setup(self):
        # Connecting to redis
        try:
            redis_conf = settings.CACHEOPS_REDIS
        except AttributeError:
            raise ImproperlyConfigured('You must specify CACHEOPS_REDIS setting to use cacheops')

        client = CacheopsRedis(**redis_conf)

        object.__setattr__(self, '__class__', client.__class__)
        object.__setattr__(self, '__dict__', client.__dict__)

    def __getattr__(self, name):
        self._setup()
        return getattr(self, name)

    def __setattr__(self, name, value):
        self._setup()
        return setattr(self, name, value)

redis_client = LazyRedis()


@memoize
def prepare_profiles():
    """
    Prepares a dict 'app.model' -> profile, for use in model_profile()
    """
    # NOTE: this is a compatibility for old style config,
    # TODO: remove in cacheops 3.0
    if hasattr(settings, 'CACHEOPS_PROFILES'):
        profiles.update(settings.CACHEOPS_PROFILES)

    if hasattr(settings, 'CACHEOPS_DEFAULTS'):
        profile_defaults.update(settings.CACHEOPS_DEFAULTS)

    model_profiles = {}
    ops = getattr(settings, 'CACHEOPS', {})
    for app_model, profile in ops.items():
        if profile is None:
            model_profiles[app_model] = None
            continue

        # NOTE: this is a compatibility for old style config,
        # TODO: remove in cacheops 3.0
        if is_tuple(profile):
            profile_name, timeout = profile[:2]

            try:
                model_profiles[app_model] = mp = deepcopy(profiles[profile_name])
            except KeyError:
                raise ImproperlyConfigured('Unknown cacheops profile "%s"' % profile_name)

            if len(profile) > 2:
                mp.update(profile[2])
            mp['timeout'] = timeout
            mp['ops'] = set(mp['ops'])
        else:
            model_profiles[app_model] = mp = merge(profile_defaults, profile)
            if mp['ops'] == 'all':
                mp['ops'] = ALL_OPS
            # People will do that anyway :)
            if isinstance(mp['ops'], six.string_types):
                mp['ops'] = [mp['ops']]
            mp['ops'] = set(mp['ops'])

        if 'timeout' not in mp:
            raise ImproperlyConfigured(
                'You must specify "timeout" option in "%s" CACHEOPS profile' % app_model)

    return model_profiles

@memoize
def model_profile(model):
    """
    Returns cacheops profile for a model
    """
    model_profiles = prepare_profiles()

    app = model._meta.app_label
    # module_name is fallback for Django 1.5-
    model_name = getattr(model._meta, 'model_name', None) or model._meta.module_name
    app_model = '%s.%s' % (app, model_name)
    for guess in (app_model, '%s.*' % app, '*.*'):
        if guess in model_profiles:
            return model_profiles[guess]
    else:
        return None
