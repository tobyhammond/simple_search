import copy
import logging

from django.core.cache import cache
from django.db.models.query import QuerySet
from django.db import models

#Adds basic caching on unique_together and PK fields
# TODO: add unique=True caching

class BasicCachingQueryset(QuerySet):
    def get(self, *args, **kwargs):
        logging.info("Get with kwargs: %s", kwargs)
        for unique_together in self.model._meta.unique_together + [ ("pk", ), ("id",)]:
            if set(unique_together).issubset(set(kwargs.keys())):
                #We can hit the cache
                key = self.model._make_key(unique_together, kwargs)
                logging.info("Attempting cache with key: %s", key)
                instance = cache.get(key)
                if instance:
                    #FIXME: Check against any other arguments
                    logging.info("Hitting the cache with key: %s", key)
                    return instance

        return super(BasicCachingQueryset, self).get(*args, **kwargs)

class BasicCachingManager(models.Manager):
    def get_query_set(self):
        return BasicCachingQueryset(self.model, using=self._db)

class BasicCachedModel(models.Model):
    objects = BasicCachingManager()

    def _as_dict(self):
        result = dict([(f.attname, getattr(self, f.attname)) for f in self._meta.local_fields])
        result["pk"] = result["id"]
        return result

    def _store_state(self):
        self._original_state = copy.deepcopy(self._as_dict())

    def __init__(self, *args, **kwargs):
        super(BasicCachedModel, self).__init__(*args, **kwargs)
        self._store_state()

    @classmethod
    def _make_key(cls, unique_together, state):
        key = "|".join([cls._meta.db_table] + [
                "%s:%s" % (x, state[x]) for x in sorted(unique_together)
        ])
        return key

    def _get_original_keys(self):
        keys = []
        for unique_together in self._meta.unique_together + [ ("pk",), ("id",)]:
            keys.append(self._make_key(unique_together, self._original_state))
        return keys

    def _get_cache_keys(self):
        keys = []
        for unique_together in self._meta.unique_together + [ ("pk",), ("id",)]:
            keys.append(self._make_key(unique_together, self._as_dict()))
        return keys

    def _cache(self):
        logging.info("Caching with keys: %s", self._get_cache_keys())
        cache.set_many( { key:self for key in self._get_cache_keys()} )

    def _uncache(self):
        logging.info("Uncaching with keys: %s", self._get_cache_keys())
        cache.delete_many(self._get_original_keys())

    def save(self, *args, **kwargs):
        if not self._state.adding:
            self._uncache()

        result = super(BasicCachedModel, self).save(*args, **kwargs)
        self._store_state()
        self._cache()
        return result

    def delete(self, *args, **kwargs):
        self._uncache()
        return super(BasicCachedModel, self).delete(*args, **kwargs)

    class Meta:
        abstract = True
