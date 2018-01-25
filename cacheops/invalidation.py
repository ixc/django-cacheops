# -*- coding: utf-8 -*-
import json
from collections import defaultdict
from funcy import memoize, post_processing, ContextDecorator
from django.db.models.expressions import F
# Since Django 1.8, `ExpressionNode` is `Expression`
try:
    from django.db.models.expressions import ExpressionNode as Expression
except ImportError:
    from django.db.models.expressions import Expression

from .conf import redis_client, handle_connection_failure
from .utils import non_proxy, load_script, get_thread_id, NOT_SERIALIZED_FIELDS


__all__ = ('invalidate_obj', 'invalidate_model', 'invalidate_all', 'no_invalidation')


_no_invalidation_depth = defaultdict(int)


@handle_connection_failure
def invalidate_dict(model, obj_dict):

    def _invalidate(model):
        # TODO: Do we need to get `obj_dict` again for every MTI parent or child
        #       model?
        if no_invalidation.active:
            return
        model = non_proxy(model)
        load_script('invalidate')(args=[
            model._meta.db_table,
            json.dumps(obj_dict, default=str)
        ])

    def _invalidate_children(model):
        # Iterate related objects, looking for MTI child models.
        for ro in model._meta.get_all_related_objects():
            # If the model is a parent of the related object model, then it must
            # be an MTI child.
            if model in ro.opts.parents:
                _invalidate(ro.model)
                _invalidate_children(ro.model)  # Recurse

    _invalidate(model)
    _invalidate_children(model)
    for parent in model._meta.parents.keys():
        _invalidate(parent)

def invalidate_obj(obj):
    """
    Invalidates caches that can possibly be influenced by object
    """
    model = non_proxy(obj.__class__)
    invalidate_dict(model, get_obj_dict(model, obj))

@handle_connection_failure
def invalidate_model(model):
    """
    Invalidates all caches for given model.
    NOTE: This is a heavy artilery which uses redis KEYS request,
          which could be relatively slow on large datasets.
    """
    def _invalidate(model):
        if no_invalidation.active:
            return
        model = non_proxy(model)
        conjs_keys = redis_client.keys('conj:%s:*' % model._meta.db_table)
        if conjs_keys:
            cache_keys = redis_client.sunion(conjs_keys)
            redis_client.delete(*(list(cache_keys) + conjs_keys))

    def _invalidate_children(model):
        # Iterate related objects, looking for MTI child models.
        for ro in model._meta.get_all_related_objects():
            # If the model is a parent of the related object model, then it must
            # be an MTI child.
            if model in ro.opts.parents:
                _invalidate(ro.model)
                _invalidate_children(ro.model)  # Recurse

    _invalidate(model)
    _invalidate_children(model)
    for parent in model._meta.parents.keys():
        # TODO: Only invalidate parent instances that have corresponding child
        #       instances. E.g. `Parent.objects.get(pk__in=Child.objects.all())`
        #       If so, we will need to stash deleted MTI parent and child
        #       instances in a `pre_delete` signal handler, so we can invalidate
        #       them in `post_delete`.
        _invalidate(parent)

@handle_connection_failure
def invalidate_all():
    if no_invalidation.active:
        return
    redis_client.flushdb()


class _no_invalidation(ContextDecorator):
    def __enter__(self):
        _no_invalidation_depth[get_thread_id()] += 1

    def __exit__(self, type, value, traceback):
        _no_invalidation_depth[get_thread_id()] -= 1

    @property
    def active(self):
        return _no_invalidation_depth.get(get_thread_id())

no_invalidation = _no_invalidation()


### ORM instance serialization

@memoize
def serializable_fields(model):
    return tuple(f for f in model._meta.fields
                   if not isinstance(f, NOT_SERIALIZED_FIELDS))

@post_processing(dict)
def get_obj_dict(model, obj):
    for field in serializable_fields(model):
        value = getattr(obj, field.attname)
        if value is None:
            yield field.attname, None
        elif isinstance(value, (F, Expression)):
            continue
        else:
            yield field.attname, field.get_prep_value(value)
