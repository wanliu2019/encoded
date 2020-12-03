from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
)
from pyramid.view import view_config
from sqlalchemy.exc import StatementError

from urllib3.exceptions import ReadTimeoutError
from snovault.elasticsearch.interfaces import (
    ELASTIC_SEARCH,
    INDEXER,
)
import datetime
import logging
import pytz
import time
import copy
import json
import re
import requests
from pkg_resources import resource_filename
from snovault import STORAGE
from snovault.elasticsearch.indexer import (
    Indexer,
    get_current_xmin
)

from snovault.elasticsearch.indexer_state import (
    IndexerState,
    all_uuids,
    SEARCH_MAX
)

from .vis_defines import (
    VISIBLE_DATASET_TYPES_LC,
    VIS_CACHE_INDEX
)
from .visualization import vis_cache_add


log = logging.getLogger('snovault.elasticsearch.es_index_listener')


def includeme(config):
    config.add_route('index_vis', '/index_vis')
    config.add_route('_visindexer_state', '/_visindexer_state')
    config.scan(__name__)
    registry = config.registry
    is_vis_indexer = registry.settings.get('visindexer')
    if is_vis_indexer:
        registry['vis'+INDEXER] = VisIndexer(registry)

class VisIndexerState(object):

    def __init__(self, es, index):
        # Moved from indexer_state
        self.es = es
        self.index = index
        self.title = 'vis'
        self.state_id = 'vis_indexer'
        self.todo_set = 'vis_in_progress'
        self.troubled_set = 'vis_troubled'
        self.last_set = 'vis_last_cycle'
        self.cleanup_this_cycle = [self.todo_set] 
        self.cleanup_last_cycle = [self.last_set,self.troubled_set]
        self.override = 'reindex_vis'
        self.staged_for_vis_list = 'staged_for_vis_indexer'
        self.followup_lists = []
        self.clock = {}
        self._is_reindex_key = 'vis_is_reindex'
        self.is_reindexing = False
        self.is_initial_indexing = False
        # Pre existing init 
        self.viscached_set      = 'vis_viscached'
        self.success_set        = self.viscached_set
        self.cleanup_last_cycle.append(self.viscached_set)
        self.followup_prep_list = None
        self.staged_cycles_list = 'vis_staged'

    # Moved from indexer_state
    def _get_is_reindex(self):
        obj = self.get_obj(self._is_reindex_key)
        return obj.get('is_reindex') is True

    def _set_is_reindex(self):
        # Flag should be set in request_reindex funciton
        self.put_obj(self._is_reindex_key, {'is_reindex': True})

    def add_errors(self, errors, finished=True):
        '''To avoid 16 worker concurency issues, errors are recorded at the end of a cycle.'''
        uuids = [err['uuid'] for err in errors]
        if len(uuids) > 0:
            if finished:
                self.put_list(self.troubled_set, uuids)

    def delete_objs(self, ids, doc_type='meta'):
        for id in ids:
            try:
                self.es.delete(index=self.index, doc_type=doc_type, id=id)
            except:
                pass

    def elapsed(self, name):
        '''Returns string of time elapsed since named clock started.'''
        start = self.clock.get(name)
        if start is None:
            return 'unknown'
        else:
            return str(datetime.datetime.now(pytz.utc) - start)

    def finish_cycle(self, state, errors=None):
        '''Every indexing cycle must be properly closed.'''
        if errors:
            self.add_errors(errors)
        done_count = self.get_count(self.todo_set)
        self.rename_objs(self.todo_set, self.last_set)
        if self.success_set is not None:
            state['vis_updated'] = self.get_count(self.success_set)
        cycle_count = state.pop('cycle_count', None)
        state['indexed'] = done_count
        self.delete_objs(self.cleanup_this_cycle)
        state['status'] = 'done'
        state['cycles'] = state.get('cycles', 0) + 1
        state['cycle_took'] = self.elapsed('cycle')
        self.put(state)
        self.delete_objs([self._is_reindex_key])
        return state

    def get(self):
        return self.get_obj(self.state_id)

    def get_count(self, id):
        return self.get_obj(id).get('count', 0)
   
    def get_initial_state(self):
        '''Useful to initialize at idle cycle'''
        new_state = { 'title': self.state_id, 'status': 'idle'}
        state = self.get()
        for var in ['cycles']:
            val = state.pop(var,None)
            if val is not None:
                new_state[var] = val
        self.set_add("registered_indexers", [self.state_id])
        return new_state

    def get_list(self, id):
        return self.get_obj(id).get('list',[])

    def get_obj(self, id, doc_type='meta'):
        try:
            return self.es.get(
                index=self.index,
                doc_type=doc_type,
                id=id
            ).get('_source',{})
        except:
            return {}

    def list_extend(self, id, vals):
        list_to_extend = self.get_list(id)
        if len(list_to_extend) > 0:
            list_to_extend.extend(vals)
        else:
            list_to_extend = vals
        self.put_list(id, list_to_extend)

    def log_reindex_init_state(self):
        # Must call after priority cycle
        if self.is_reindexing and self.is_initial_indexing:
            log.info('%s is reindexing all', self.title)
        elif self.is_reindexing:
            log.info('%s is reindexing', self.title)
        elif self.is_initial_indexing:
            log.info('%s is initially indexing', self.title)
    
    def priority_cycle(self, request):
        '''
        Initial startup, reindex, or interupted prior cycle can all lead to a priority cycle.
           returns (discovered xmin, uuids, whether previous cycle was interupted).
        '''
        initialized = self.get_obj("indexing")
        self.is_reindexing = self._get_is_reindex()
        if not initialized:
            self.is_initial_indexing = True
            self.delete_objs([self.override] + self.followup_lists)
            state = self.get()
            state['status'] = 'uninitialized'
            self.put(state)
            return (-1, [], False)
        state = self.get()
        reindex_uuids = self.reindex_requested(request)
        if reindex_uuids is not None and reindex_uuids != []:
            uuids_count = len(reindex_uuids)
            log.warn('%s reindex of %d uuids requested' % (self.state_id, uuids_count))
            return (-1, reindex_uuids, False)
        if state.get('status', '') != 'indexing':
            return (-1, [], False)
        xmin = state.get('xmin', -1)
        if xmin == -1:
            return (-1, [], False)
        undone_uuids = self.get_list(self.todo_set)
        if len(undone_uuids) <= 0:
            return (-1, [], False)
        return (xmin, undone_uuids, True)

    def put(self, state):
        '''Update the basic state info'''
        errors = state.pop('errors', None)
        state['title'] = self.state_id
        self.put_obj(self.state_id, state)
        if errors is not None:
            state['errors'] = errors
    
    def put_obj(self, id, obj, doc_type='meta'):
        try:
            self.es.index(index=self.index, doc_type=doc_type, id=id, body=obj)
        except:
            log.warn("Failed to save to es: " + id, exc_info=True)

    def put_list(self, id, a_list):
        return self.put_obj(id, { 'list': a_list, 'count': len(a_list) })
   
    def reindex_requested(self, request):
        '''returns list of uuids if a reindex was requested.'''
        override = self.get_obj(self.override)
        if override:
            if override.get('all_uuids', False):
                self.delete_objs([self.override] + self.followup_lists)
                return self.all_indexable_uuids(request)
            else:
                uuids =  override.get('uuids',[])
                uuid_count = len(uuids)
                if uuid_count > 0:
                    if uuid_count > SEARCH_MAX:
                        self.delete_objs([self.override] + self.followup_lists)
                    else:
                        self.delete_objs([self.override])
                    return uuids
        return None

    def rename_objs(self, from_id, to_id):
        val = self.get_list(from_id)
        if val:
            self.put_list(to_id, val)
            self.delete_objs([from_id])

    def request_reindex(self,requested):
        '''Requests full reindexing on next cycle'''
        if requested == 'all':
            self._set_is_reindex()
            self.put_obj(self.override, {self.title : 'reindex', 'all_uuids': True})
        else:
            uuid_list = requested.split(',')
            uuids = set()
            while uuid_list:
                uuid = uuid_list.pop(0)
                if len(uuid) > 0 and re.match("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", uuid):
                    uuids.add(uuid)
                else:
                    return "Requesting reindex of at least one uninterpretable uuid: '%s'" % (uuid)
            override_obj = self.get_obj(self.override)
            if 'uuids' not in override_obj.keys():
                override_obj['uuids'] = list(uuids)
            else:
                uuids |= set(override_obj['uuids'])
                override_obj['uuids'] = list(uuids)
            self._set_is_reindex()
            self.put_obj(self.override, override_obj)
        return None

    def set_add(self, id, vals):
        set_to_update = set(self.get_list(id))
        if len(set_to_update) > 0:
            set_to_update.update(vals)
        else:
            set_to_update = set(vals)
        self.put_list(id, set_to_update)

    def start_clock(self, name):
        '''Can start a named clock and use it later to figure out elapsed time'''
        self.clock[name] = datetime.datetime.now(pytz.utc)

    def start_cycle(self, uuids, state=None):
        '''Every indexing cycle must be properly opened.'''
        self.clock = {}
        self.start_clock('cycle')
        if state is None:
            state = self.get()
        state['cycle_started'] = datetime.datetime.now().isoformat()
        state['status'] = 'indexing'
        state['cycle_count'] = len(uuids)

        self.put(state)
        self.delete_objs(self.cleanup_last_cycle)
        self.delete_objs(self.cleanup_this_cycle)
        self.put_list(self.todo_set, set(uuids))
        return state

    # Pre existing functions
    def all_indexable_uuids(self, request):
        '''returns list of uuids pertinant to this indexer.'''
        return all_visualizable_uuids(request.registry)

    def viscached_uuid(self, uuid):
        self.list_extend(self.viscached_set, [uuid])

    def get_one_cycle(self, xmin, request):
        uuids = []
        next_xmin = None

        (undone_xmin, uuids, cycle_interrupted) = self.priority_cycle(request)
        # NOTE: unlike with primary_indexer priority_cycle() can be called after get_initial_state()
        if len(uuids) > 0:
            if not cycle_interrupted:  # AKA reindex signal
                return (-1, next_xmin, uuids)  # -1 ensures using the latest xmin
            if xmin is None or int(xmin) > undone_xmin:
                return (undone_xmin, next_xmin, uuids)

        # To avoid race conditions, move ready_list to end of staged. Then work on staged.
        latest = self.get_list(self.staged_for_vis_list)
        if latest != []:
            self.delete_objs([self.staged_for_vis_list])  # TODO: tighten this by adding a locking semaphore
            self.list_extend(self.staged_cycles_list, latest) # Push back for start of next uuid cycle

        staged_list = self.get_list(self.staged_cycles_list)
        if not staged_list or len(staged_list) == 0:
            return (xmin, None, [])
        looking_at = 0
        for val in staged_list:
            looking_at += 1
            if val.startswith("xmin:"):
                if xmin is None:
                    #assert(len(uuids) == 0)  # This is expected but is it assertable?  Shouldn't bet on it
                    xmin = val[5:]
                    continue
                else:
                    next_xmin = val[5:]
                    if next_xmin == xmin:  # shouldn't happen, but just in case
                        next_xmin = None
                        continue
                    looking_at -= 1
                    break   # got all the uuids for the current xmin
            else:
                uuids.append(val)

        if xmin is None:  # could happen if first and only cycle did not start with xmin
            xmin = self.get().get('last_xmin',-1)

        uuid_count = len(uuids)
        if len(uuids) > 0:
            if len(staged_list) == looking_at:
               self.delete_objs([self.staged_cycles_list])
            elif looking_at > 0:
                self.put_list(self.staged_cycles_list,staged_list[looking_at:]) # Push back for start of next uuid cycle
        return (xmin, next_xmin, uuids)

    def display(self, uuids=None):
        display = {}
        display['state'] = self.get()
        if display['state'].get('status', '') == 'indexing' and 'cycle_started' in display['state']:
            started = datetime.datetime.strptime(display['state']['cycle_started'], '%Y-%m-%dT%H:%M:%S.%f')
            display['state']['indexing_elapsed'] = str(datetime.datetime.now() - started)
        display['title'] = display['state'].get('title', self.state_id)
        display['uuids_in_progress'] = self.get_count(self.todo_set)
        display['uuids_troubled'] = self.get_count(self.troubled_set)
        display['uuids_last_cycle'] = self.get_count(self.last_set)
        if self.followup_prep_list is not None:
            display['to_be_staged_for_follow_up_indexers'] = self.get_count(self.followup_prep_list)
        id = 'staged_for_%s_list' % (self.title)
        display['staged_by_primary'] = self.get_count(id)
        reindex = self.get_obj(self.override)
        if reindex:
            uuids = reindex.get('uuids')
            if uuids is not None:
                display['reindex_requested'] = uuids
            elif reindex.get('all_uuids',False):
                display['reindex_requested'] = 'all'
        display['now'] = datetime.datetime.now().isoformat()
        if uuids is not None:
            uuids_to_show = []
            uuid_list = self.get_obj(self.todo_set)
            if not uuid_list:
                uuids_to_show = 'No uuids indexing'
            else:
                uuid_start = 0
                try:
                    uuid_start = int(uuids)
                except:
                    pass
                if uuid_start < uuid_list.get('count',0):
                    uuid_end = uuid_start+100
                    if uuid_start > 0:
                        uuids_to_show.append("... skipped first %d uuids" % (uuid_start))
                    uuids_to_show.extend(uuid_list['list'][uuid_start:uuid_end])
                    if uuid_list.get('count',0) > uuid_end:
                        uuids_to_show.append("another %d uuids..." % (uuid_list.get('count',0) - uuid_end))
                elif uuid_start > 0:
                    uuids_to_show.append("skipped past all %d uuids" % (uuid_list.get('count',0)))
                else:
                    uuids_to_show = 'No uuids indexing'
            display['uuids_in_progress'] = uuids_to_show
        display['staged_to_process'] = self.get_count(self.staged_cycles_list)
        display['datasets_vis_cached_current_cycle'] = self.get_count(self.success_set)
        return display


@view_config(route_name='_visindexer_state', request_method='GET', permission="index")
def visindexer_state_show(request):
    es = request.registry[ELASTIC_SEARCH]
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    state = VisIndexerState(es,INDEX)

    # requesting reindex
    reindex = request.params.get("reindex")
    if reindex is not None:
        msg = state.request_reindex(reindex)
        if msg is not None:
            return msg

    display = state.display(uuids=request.params.get("uuids"))
    try:
        count = es.count(index=VIS_CACHE_INDEX, doc_type='default').get('count',0)
        if count:
            display['vis_blobs_in_index'] = count
    except:
        display['vis_blobs_in_index'] = 'Not Found'
        pass

    if not request.registry.settings.get('testing',False):  # NOTE: _indexer not working on local instances
        try:
            r = requests.get(request.host_url + '/_visindexer')
            display['listener'] = json.loads(r.text)
            display['status'] = display['listener']['status']
            #subreq = Request.blank('/_visindexer')
            #result = request.invoke_subrequest(subreq)
            #result = request.embed('_visindexer')
        except:
            log.error('Error getting /_visindexer', exc_info=True)

    # always return raw json
    if len(request.query_string) > 0:
        request.query_string = "&format=json"
    else:
        request.query_string = "format=json"
    return display


@view_config(route_name='index_vis', request_method='POST', permission="index")
def index_vis(request):
    INDEX = request.registry.settings['snovault.elasticsearch.index']
    # vis_indexer works off of already indexed elasticsearch objects!
    request.datastore = 'elasticsearch'

    record = request.json.get('record', False)
    dry_run = request.json.get('dry_run', False)
    es = request.registry[ELASTIC_SEARCH]
    indexer = request.registry['vis'+INDEXER]

    # keeping track of state
    state = VisIndexerState(es, INDEX)

    last_xmin = None
    result = state.get_initial_state()
    last_xmin = result.get('xmin')
    next_xmin = None
    xmin = None  # will be at the beginning of the queue
    result.update(
        last_xmin=last_xmin,
        xmin=xmin
    )

    uuid_count = 0
    indexing_errors = []
    first_txn = datetime.datetime.now(pytz.utc)
    cycles = 0

    (xmin, next_xmin, uuids) = state.get_one_cycle(xmin, request)
    state.log_reindex_init_state()
    uuid_count = len(uuids)
    if uuid_count > 0 and (xmin is None or int(xmin) <= 0):  # Happens when the a reindex all signal occurs.
        xmin = get_current_xmin(request)

    ### NOTE: These lines may not be appropriate when work other than vis_caching is being done.
    if uuid_count > 500:  # some arbitrary cutoff.
        # There is an efficiency trade off examining many non-visualizable uuids
        # # vs. the cost of eliminating those uuids from the list ahead of time.
        uuids = list(set(all_visualizable_uuids(request.registry)).intersection(uuids))
        uuid_count = len(uuids)
    ### END OF NOTE

    if uuid_count and not dry_run:
        # Starts one cycle of uuids to followup index
        result.update(
            last_xmin=last_xmin,
            xmin=xmin,
        )
        result = state.start_cycle(uuids, result)

        # Make no effort to incrementally index... all in
        errors = indexer.update_objects(request, uuids, xmin)     # , snapshot_id)

        indexing_errors.extend(errors)  # ignore errors?
        result['errors'] = indexing_errors

        result = state.finish_cycle(result, indexing_errors)

    if uuid_count == 0:
        result.pop('indexed',None)

    return result


def all_visualizable_uuids(registry):
    return list(all_uuids(registry, types=VISIBLE_DATASET_TYPES_LC))


class VisIndexer(Indexer):
    def __init__(self, registry):
        super(VisIndexer, self).__init__(registry)
        self.es = registry[ELASTIC_SEARCH]
        self.esstorage = registry[STORAGE]
        self.index = registry.settings['snovault.elasticsearch.index']
        self.state = VisIndexerState(self.es, self.index)  # WARNING, race condition is avoided because there is only one worker

    def get_from_es(request, comp_id):
        '''Returns composite json blob from elastic-search, or None if not found.'''
        return None

    def update_objects(self, request, uuids, xmin):
        # pylint: disable=too-many-arguments, unused-argument
        '''Run indexing process on uuids'''
        errors = []
        for i, uuid in enumerate(uuids):
            error = self.update_object(request, uuid, xmin)
            if error is not None:
                errors.append(error)
            if (i + 1) % 1000 == 0:
                log.info('Indexing %d', i + 1)
        return errors

    def update_object(self, request, uuid, xmin, restart=False):

        last_exc = None
        # First get the object currently in es
        try:
            result = self.esstorage.get_by_uuid(uuid)  # No reason to restrict by version and that could interfere with reindex all signal.
            #result = self.es.get(index=self.index, id=str(uuid), version=xmin, version_type='external_gte')
            doc = result.source
        except StatementError:
            # Can't reconnect until invalid transaction is rolled back
            raise
        except Exception as e:
            log.error("Error can't find %s in %s", uuid, ELASTIC_SEARCH)
            last_exc = repr(e)

        ### NOTE: if other work is to be done, this can be renamed "secondary indexer", and work can be added here

        if last_exc is None:
            try:
                result = vis_cache_add(
                    request,
                    doc['embedded'],
                    is_vis_indexer=True,
                )
                if len(result):
                    # Warning: potentiallly slow uuid-level accounting, but single process so no concurency issue
                    self.state.viscached_uuid(uuid)
            except Exception as e:
                log.error('Error indexing %s', uuid, exc_info=True)
                #last_exc = repr(e)
                pass  # It's only a vis_blob.

        if last_exc is not None:
            timestamp = datetime.datetime.now().isoformat()
            return {'error_message': last_exc, 'timestamp': timestamp, 'uuid': str(uuid)}
