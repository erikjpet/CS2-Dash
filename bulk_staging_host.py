"""Explicit host adapter. No worker, DB, network or timer starts at import."""
import time
import sqlite3
from bulk_derived_staging import Deferred, RevisionConflict, PublishUncertain, bind_legacy


class Host:
    def __init__(self, staging, server_namespace, cycle_seconds, max_units):
        if cycle_seconds <= 0 or type(max_units) is not int or max_units <= 0:
            raise ValueError('Explicit cycle duration and unit count are required')
        self.engine, self.ns = staging, server_namespace
        self.cycle_seconds, self.max_units = cycle_seconds, max_units

    def aliases(self, state):
        cached = self.ns.get('COLLECTION_CASE_ALIAS_CACHE')
        if cached is not None:
            return dict(cached).get
        from bulk_derived_staging import Guard
        rows = self.engine.source_rows(
            "SELECT name,collection FROM item_catalog WHERE kind='collections' AND collection IS NOT NULL",
            (), state, Guard(self.engine.stage_budget))
        aliases = {}
        for row in rows:
            value = (row['collection'] or '').strip()
            if value and self.ns['is_weapon_case_name'](value):
                aliases[(row['name'] or '').strip()] = value
        return aliases.get

    def pump(self, progress=None):
        started = time.monotonic()
        state = self.engine.create_or_resume()
        metric, lookup = bind_legacy(
            self.ns['rebuild_market_metric_windows'], self.ns['rebuild_market_item_lookup'],
            self.ns['build_unpriced_market_lookup_rows'], self.ns['market_group_tags'], self.aliases(state))
        units = 0
        while not state['complete'] and units < self.max_units:
            if time.monotonic() - started >= self.cycle_seconds:
                break
            state = self.engine.run_unit(state['generation'], metric, lookup, self.ns['CURRENT_PRICE_PROVIDERS'])
            units += 1
            if progress:
                progress('derived_staging', 'Source committed; derived sidecar remains unpublished',
                         derivedPhase=state['phase'], derivedCursor=state['cursor'],
                         derivedGeneration=state['generation'], commitState='source_committed_derived_pending')
        result = {'state': 'source_committed_derived_pending', 'generation': state['generation'],
                  'stageUnits': units, 'stagingSeconds': time.monotonic() - started,
                  'publicationSeconds': 0, 'complete': False}
        if state['complete']:
            result['admissionEstimate'] = self.engine.estimate(state['generation'])
            publication_started = time.monotonic()
            result.update(self.engine.publish(state['generation']))
            result.update(complete=True, publicationSeconds=time.monotonic() - publication_started)
        return result


def pump_saved(host, progress=None):
    """Use existing pending data only; the host has no provider/request callback."""
    try:
        return host.pump(progress)
    except PublishUncertain as exc:
        return {'state':'publication_uncertain','complete':False,'publishToken':exc.publish_token,
                'requiresExactTokenOutcomeCheck':True,'stagingSeconds':None,'publicationSeconds':None}
    except (Deferred, RevisionConflict, sqlite3.Error) as exc:
        return {'state': 'source_committed_derived_pending', 'complete': False,
                'deferredReason': str(exc), 'stagingSeconds': None, 'publicationSeconds': None}
