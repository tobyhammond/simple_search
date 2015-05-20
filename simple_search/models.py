import logging
import shlex
import time

from django.db import models
from django.utils.encoding import smart_str, smart_unicode
from django.core.exceptions import ObjectDoesNotExist
from django.conf import settings
from django.db import IntegrityError
from djangae.db import transaction

from google.appengine.ext import deferred

"""
    REMAINING TO DO!

    1. Partial matches. These should be recorded as new Index instances, with an FK to the full term being indexes.
       Partials should only be recorded for between 4, and len(original_term) - 1 characters. Partial matches should be much more highly scored,
       the lower the match, the more the score should be
    2. Cross-join indexing  e.g. book__title on an Author.
    3. Field matches. e.g "id:1234 field1:banana". This should match any other words using indexes, but only return matches that match the field lookups
"""

QUEUE_FOR_INDEXING = getattr(settings, "QUEUE_FOR_INDEXING", "default")

def _do_index(instance, fields_to_index):
    def get_data_from_field(field_, instance_):
        lookups = field_.split("__")
        value = instance
        for lookup in lookups:
            if value is None:
                continue
            value = getattr(value, lookup)

            if "RelatedManager" in value.__class__.__name__:
                if lookup == lookups[-2]:
                    return [ getattr(x, lookups[-1]) for x in value.all() ]
                else:
                    raise TypeError("You can only index one level of related object")

            elif hasattr(value, "__iter__"):
                if lookup == lookups[-1]:
                    return value
                else:
                    raise TypeError("You can only index one level of iterable")

        return [ value ]

    try:
        instance = instance.__class__.objects.get(pk=instance.pk)
    except ObjectDoesNotExist:
        logging.info("Attempting to retrieve object of class: '%s' - with pk: '%s'", instance.__class__.__name__, instance.pk)
        raise


    for field in fields_to_index:
        texts = get_data_from_field(field, instance)
        for text in texts:
            if text is None:
                continue

            text = smart_unicode(text)
            text = text.lower() #Normalize

            words = text.split(" ") #Split on whitespace

            #Build up combinations of adjacent words
            for i in xrange(0, len(words)):
                for j in xrange(1, 5):
                    term_words = words[i:i+j]

                    if len(term_words) != j:
                        break

                    term = u" ".join(term_words)

                    if not term.strip(): continue

                    while True:
                        try:
                            filter_args = dict(
                                iexact=term,
                                instance_db_table=instance._meta.db_table,
                                instance_pk=instance.pk
                            )

                            if Index.objects.filter(**filter_args).exists():
                                # Don't reindex if the index already exists
                                break

                            with transaction.atomic(xg=True):
                                logging.info("Indexing: '%s', %s", term, type(term))
                                term_count = text.count(term)

                                try:
                                    filter_args["occurances"] = term_count
                                    Index.objects.create(
                                        **filter_args
                                    )

                                    counter, created = GlobalOccuranceCount.objects.get_or_create(pk=term)
                                    counter.count += term_count
                                    counter.save()
                                except IntegrityError:
                                    # If we already created this index for this instance, then ignore
                                    pass
                                break
                        except transaction.TransactionFailedError:
                            logging.warning("Transaction collision, retrying!")
                            time.sleep(1)
                            continue

def _unindex_then_reindex(instance, fields_to_index):
    unindex_instance(instance)
    _do_index(instance, fields_to_index)


def index_instance(instance, fields_to_index, defer_index=True):
    if defer_index:
        deferred.defer(_unindex_then_reindex, instance, fields_to_index,
                        _queue=QUEUE_FOR_INDEXING, _transactional=transaction.in_atomic_block())
    else:
        _unindex_then_reindex(instance, fields_to_index)


def unindex_instance(instance):
    indexes = Index.objects.filter(instance_db_table=instance._meta.db_table, instance_pk=instance.pk).all()
    for index in indexes:
        try:
            while True:
                try:
                    with transaction.atomic(xg=True):
                        try:
                            index = Index.objects.get(pk=index.pk)
                        except Index.DoesNotExist:
                            return

                        count = GlobalOccuranceCount.objects.get(pk=index.iexact)
                        count.count -= index.occurances
                        count.save()
                        index.delete()

                        if count.count < 0:
                            logging.error("The GOC of {} was negative ({}) while unindexing {}", count.pk, count.count, index.pk)
                        break

                except transaction.TransactionFailedError:
                    logging.warning("Transaction collision, retrying!")
                    time.sleep(1)
                    continue
        except GlobalOccuranceCount.DoesNotExist:
            logging.warning("A GlobalOccuranceCount for Index: %s does not exist, ignoring", index.pk)
            continue



def parse_terms(search_string):
    terms = shlex.split(smart_str(search_string.lower()))

    # The split requires the unicode string to be encoded to a bytestring, but
    # we need the terms to be decoded back to utf-8 for use in the datastore queries.
    return [smart_unicode(term) for term in terms]

def search(model_class, search_string, per_page=50, current_page=1, total_pages=10, **filters):
    terms = parse_terms(search_string)

    #Get all matching terms
    matching_terms = dict(GlobalOccuranceCount.objects.filter(pk__in=terms).values_list('pk', 'count'))
    matches = Index.objects.filter(iexact__in=terms, instance_db_table=model_class._meta.db_table).all()

    instance_weights = {}

    for match in matches:
        instance_weights.setdefault(match.instance_pk, []).append(matching_terms[match.iexact])

    final_weights = []
    for k, v in instance_weights.items():
        """
            This is where we rank the results. Lower scores are better. Scores are based
            on the commonality of the word. More matches are rewarded, but not too much so
            that rarer terms still have a chance.

            Examples for n matches:

            1 = 1 + (0 * 0.5) = 1    -> scores / 1
            2 = 2 + (1 * 0.5) = 2.5  -> scores / 2.5 (rather than 2)
            3 = 3 + (2 * 0.5) = 4    -> scores / 4 (rather than 3)
        """

        n = float(len(v))
        final_weights.append((sum(v) / (n + ((n-1) * 0.5)), k))

    final_weights.sort()

    #Restrict to the max possible
    final_weights = final_weights[:total_pages*per_page]

    #Restrict to the page
    offset = ((current_page - 1) * per_page)
    final_weights = final_weights[offset:offset + per_page]

    order = {}
    for index, (score, pk) in enumerate(final_weights):
        order[pk] = index

    sorted_results = [None] * len(order.keys())

    queryset = model_class.objects.all()
    if filters:
        queryset = queryset.filter(**filters)

    results = queryset.filter(pk__in=order.keys())
    for result in results:
        position = order[result.pk]
        sorted_results[position] = result

    return [x for x in sorted_results if x ]

class GlobalOccuranceCount(models.Model):
    id = models.CharField(max_length=1024, primary_key=True)
    count = models.PositiveIntegerField(default=0)

    def update(self):
        while True:
            try:
                count = 0
                for index in Index.objects.filter(iexact=self.id):
                    count += Index.objects.get(pk=index.pk).occurances

                with transaction.atomic():
                    goc = GlobalOccuranceCount.objects.get(pk=self.id)
                    goc.count = count
                    goc.save()

            except transaction.TransactionFailedError:
                time.sleep(1)
                continue

class Index(models.Model):
    iexact = models.CharField(max_length=1024)
    instance_db_table = models.CharField(max_length=1024)
    instance_pk = models.PositiveIntegerField(default=0)
    occurances = models.PositiveIntegerField(default=0)

    class Meta:
        unique_together = [
            ('iexact', 'instance_db_table', 'instance_pk')
        ]

from django.dispatch import receiver
from django.db.models.signals import post_save, pre_delete

@receiver(post_save)
def post_save_index(sender, instance, created, raw, *args, **kwargs):
    if getattr(instance, "Search", None):
        fields_to_index = getattr(instance.Search, "fields", [])
        if fields_to_index:
            index_instance(instance, fields_to_index, defer_index=not raw) #Don't defer if we are loading from a fixture

@receiver(pre_delete)
def pre_delete_unindex(sender, instance, using, *args, **kwarg):
    if getattr(instance, "Search", None):
        unindex_instance(instance)
