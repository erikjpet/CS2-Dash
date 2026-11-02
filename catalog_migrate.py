#!/usr/bin/env python3
"""Plan or explicitly bootstrap the additive catalog registry in small batches.

Default is a read-only schema plan. --apply seeds current metadata and discovers
historical stream prefixes using bounded index seeks. History stays in place.
Live use requires the separately reviewed migration/activation plan.
"""
import argparse
import datetime
import json
from pathlib import Path
import sqlite3
import sys

from catalog_store import CatalogStore
from catalog_migration_guard import (
    Budget, CONTROL, CONTROL_SCHEMA, RECEIPTS, MigrationFailure, canonical,
    digest, load_checkpoint, require_fresh_or_checkpoint, snapshot_affected,
    storage_observation, validate_schema, write_external_receipt,
    reconcile_pending_receipt,
    current_proof_snapshot, current_proof_schema, install_current_proof,
)

STAGES = ('legacy:item_catalog', 'legacy:market_items', 'history:price_history',
          'history:price_observations', 'history:shared_price_history',
          'history:shared_price_observations')
PROGRESS_KEY = 'catalog-registry-v2'
CURRENT_STORES = ('item_catalog', 'market_items', 'market_item_prices', 'price_quotes',
                  'shared_item_prices', 'shared_source_quotes')
CURRENT_STAGES = STAGES[:2] + tuple('current:' + store for store in CURRENT_STORES[2:]) + tuple(
    'verify:' + store for store in CURRENT_STORES)


def current_source_revisions(connection):
    if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='settings'").fetchone():
        return {'catalog_metadata_revision': None, 'market_source_revision': None}
    values = dict(connection.execute("SELECT key,value FROM settings WHERE key IN ('catalog_metadata_revision','market_source_revision')"))
    return {key: values.get(key) for key in ('catalog_metadata_revision', 'market_source_revision')}


def current_readiness(progress, revisions=None, proof=None):
    verification = progress.get('verification') or {}
    observed = (progress.get('bootstrapMode') == 'current' and progress['complete']
                and set(verification.get('stores', {})) == set(CURRENT_STORES)
                and all(value.get('done') for value in verification['stores'].values()))
    coherent = bool(observed and proof is not None and verification.get('proofStable')
                    and verification.get('proofBefore') == proof)
    return {'observedParityVerified': observed,
            'serverWriterRevisionsStable': bool(observed and verification.get('revisionsStable') and
                                               verification.get('revisionsBefore') == revisions),
            'snapshotComplete': coherent, 'activationAuthorized': False,
            'proofScope': 'all six current source row changes via qualified ordinary SQLite triggers; registry generation guarded; no activation',
            'sourceChangeCoverage': 'SQL INSERT/UPDATE/DELETE including REPLACE; owned proof objects must not be edited or bypassed',
            'registryWriterScope': 'migration-owned registry with qualified generation-maintaining ingestion; arbitrary direct registry edits are not certified',
            'proofIntervalStart': verification.get('proofStartedAt'),
            'revisionCoverage': 'server-managed revision tokens; external writer coverage requires separate qualification',
            'requiresNewPassForEarlierInserts': True}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def read_plan(path, budget=None):
    path = Path(path)
    if not path.is_file():
        raise ValueError('existing database path required')
    connection = sqlite3.connect(path.resolve().as_uri()+'?mode=ro', uri=True, timeout=0)
    budget = budget or Budget()
    try:
        connection.execute('PRAGMA query_only=ON')
        budget.attach(connection)
        connection.execute('BEGIN')
        rows = connection.execute('SELECT type,name FROM sqlite_master LIMIT 257').fetchall()
        if len(rows) == 257:
            raise ValueError('schema exceeds bounded plan inventory')
        tables = {name for kind,name in rows if kind=='table'}
        required = {'item_catalog','market_items'}
        if not required.issubset(tables):
            raise ValueError('database lacks expected current CS2 catalog tables')
        validate_schema(connection, partial=True)
        progress, head = None, None
        if CONTROL in tables:
            row = connection.execute('SELECT progress_json,receipt_hash FROM ' + CONTROL + ' WHERE migration_key=?',(PROGRESS_KEY,)).fetchone()
            if row:
                progress, head = json.loads(row[0]), row[1]
        journal_mode = connection.execute('PRAGMA journal_mode').fetchone()[0]
        budget.check()
        return {
            'mode':'read_only_plan','database':str(path.resolve()),
            'stages':list(CURRENT_STAGES if progress and progress.get('bootstrapMode') == 'current' else STAGES),
            'progress':progress,'receiptHead':head,'journalMode':journal_mode,
            'history_preserved_in_place':[name for name in ('price_history','price_observations','shared_price_history','shared_price_observations') if name in tables],
            'history_full_scan_or_copy':False,'historyIndexedPrefixDiscovery':True,'retention_change':False,
            'recovery':'Additive logical receipts, not a physical disaster backup or corruption guarantee.',
            'next_step':'Review architecture and approve explicit additive migration before --apply. Source activation/restart is separate.',
        }
    finally:
        budget.detach()
        connection.close()


def apply_batches(path, batch_size=10, max_batches=1, *, receipt_dir=None,
                  expected_receipt=None, new_history_pass=False, quiescent=False,
                  current_only=False, new_current_pass=False, restart_verification=False,
                  enable_current_proof=False,
                  max_seconds=3, max_queries=2000, max_vm_steps=1000000,
                  max_changed_rows=4000, max_write_sql_bytes=1048576,
                  max_new_page_bytes=1048576, receipt_storage_qualified=False,
                  database_free_floor=1073741824, receipt_free_floor=67108864):
    path = Path(path)
    if isinstance(batch_size, bool) or isinstance(max_batches, bool) or not 1 <= batch_size <= 100 or not 1 <= max_batches <= 4:
        raise ValueError('batch-size 1..100 and max-batches 1..4 required; no unbounded override')
    if receipt_dir is None:
        raise ValueError('explicit --receipt-dir required for preserved logical recovery records')
    if not receipt_storage_qualified:
        raise ValueError('qualified receipt storage must be explicitly asserted; file fsync alone cannot guarantee rename/device durability')
    observation_start = storage_observation(path,receipt_dir,database_free_floor,receipt_free_floor)
    budget = Budget(max_seconds, max_queries, max_vm_steps, max_changed_rows,
                    max_write_sql_bytes, max_new_page_bytes)
    no_close_checkpoint = getattr(sqlite3, 'SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE', None)
    if no_close_checkpoint is None or not hasattr(sqlite3.Connection, 'setconfig'):
        raise RuntimeError('Explicit migration requires Python 3.12+ with SQLite NO_CKPT_ON_CLOSE support; no write connection was opened.')
    plan = read_plan(path, budget=budget)
    if plan['journalMode'].lower() != 'wal':
        raise MigrationFailure('Expected existing WAL mode; no journal-mode conversion permitted')
    connection = sqlite3.connect(path.resolve().as_uri()+'?mode=rw', uri=True, timeout=0)
    connection.row_factory = sqlite3.Row
    batches, candidate, commit_started, commit_succeeded = [], None, False, False
    try:
        connection.setconfig(no_close_checkpoint, True)
        if not connection.getconfig(no_close_checkpoint):
            raise RuntimeError('Could not suppress automatic checkpoint at connection close')
        connection.execute('PRAGMA wal_autocheckpoint=0')
        connection.execute('PRAGMA busy_timeout=0')
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA cache_spill=OFF')
        connection.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 262144)
        budget.attach(connection)
        validate_schema(connection, partial=True)
        page_start = connection.execute('PRAGMA page_count').fetchone()[0]
        connection.execute('BEGIN IMMEDIATE')
        # No arbitrary preexisting registry overwrite/rollback promise. A fresh
        # job may reconstruct its IDs from its complete external receipt chain.
        if require_fresh_or_checkpoint(connection,PROGRESS_KEY):
            # A missing structure on resume is loss/mismatch, not a request for
            # automatic schema repair that would hide missing stable IDs.
            validate_schema(connection)
        repository = CatalogStore(connection)
        repository.create_schema()
        for statement in CONTROL_SCHEMA:
            connection.execute(statement)
        schema_hash = validate_schema(connection)
        initial = {'stage':0,'cursor':None,'processed':0,'complete':False,'pass':1,'startedAt':now(),
                   'historyRestartPending':[],
                   'receiptDirectory':str(Path(receipt_dir).resolve()),
                   'coverageSemantics':'observed traversal; concurrent earlier-prefix inserts require another pass'}
        progress, receipt_no, previous_hash = load_checkpoint(connection, PROGRESS_KEY, schema_hash, expected_receipt, initial,receipt_dir)
        if progress.get('bootstrapMode') not in (None, 'current'):
            raise MigrationFailure('unknown persisted bootstrap mode')
        current_mode = progress.get('bootstrapMode') == 'current'
        if current_only and not current_mode:
            if progress.get('complete') or progress.get('stage', 2) >= 2:
                raise MigrationFailure('current-only mode can adopt a fresh/pilot metadata checkpoint only; historical checkpoints require a separate reviewed transition')
            progress = dict(progress, bootstrapMode='current', currentRestartPending=list(CURRENT_STAGES[2:6]),
                            metadataRestartPending=[], verification={})
            current_mode = True
        stages = CURRENT_STAGES if current_mode else STAGES
        if not isinstance(progress.get('stage'), int) or not 0 <= progress['stage'] <= len(stages) or progress['complete'] != (progress['stage'] == len(stages)):
            raise MigrationFailure('invalid progress shape')
        if type(progress.get('pass')) is not int or progress['pass'] < 1:
            raise MigrationFailure('invalid historical pass epoch')
        # Older v2 receipts did not persist restart intent. An unfinished later
        # pass has entered its current store iff it has a continuation cursor;
        # every later store still needs its first restart for this pass epoch.
        remaining_history = [stage for stage in stages[progress['stage']:] if stage.startswith('history:')]
        pending = progress.get('historyRestartPending')
        if 'historyRestartPending' not in progress:
            pending = remaining_history if progress['pass'] > 1 else []
            if progress['cursor'] is not None and pending:
                pending = pending[1:]
        if (not isinstance(pending, list) or any(not isinstance(stage, str) for stage in pending)
                or len(pending) != len(set(pending)) or any(stage not in remaining_history for stage in pending)
                or (progress['cursor'] is not None and progress['stage'] < len(stages)
                    and stages[progress['stage']] in pending)):
            raise MigrationFailure('invalid persisted historical restart intent')
        progress = dict(progress, historyRestartPending=list(pending))
        if new_history_pass:
            if current_mode:
                raise MigrationFailure('historical traversal is separate from current-only bootstrap')
            if not progress['complete']:
                raise MigrationFailure('cannot restart an unfinished historical pass')
            progress = dict(progress, stage=2, cursor=None, complete=False,
                            historyRestartPending=list(STAGES[2:]), **{'pass':progress['pass']+1})
        if new_current_pass or restart_verification:
            if not current_mode or (not progress['complete'] and progress['stage'] < 6):
                raise MigrationFailure('finish current registration before starting another current/verification pass')
            if new_current_pass and restart_verification:
                raise MigrationFailure('choose a new current pass or verification-only restart')
            progress = dict(progress, stage=0 if new_current_pass else 6, cursor=None, complete=False, verification={},
                metadataRestartPending=list(CURRENT_STAGES[:2]) if new_current_pass else [],
                currentRestartPending=list(CURRENT_STAGES[2:6]) if new_current_pass else [],
                **{'pass': progress['pass'] + int(new_current_pass)})
        if current_mode:
            for field, allowed in (('metadataRestartPending', CURRENT_STAGES[:2]), ('currentRestartPending', CURRENT_STAGES[2:6])):
                markers = progress.get(field)
                if (not isinstance(markers, list) or any(not isinstance(stage, str) or stage not in allowed for stage in markers)
                        or len(markers) != len(set(markers))
                        or any(stage not in stages[progress['stage']:] for stage in markers)
                        or (progress['cursor'] is not None and progress['stage'] < len(stages) and stages[progress['stage']] in markers)):
                    raise MigrationFailure('invalid current pass restart markers')
        proof_installation = None
        if enable_current_proof:
            if not current_mode or progress.get('currentProof'):
                raise MigrationFailure('proof installation requires current mode without an existing proof enrollment')
            installed = install_current_proof(connection)
            proof_installation = list(current_proof_schema())
            progress = dict(progress, currentProof={'abi': installed['abi'], 'manifestHash': installed['manifestHash']})
            if progress['stage'] >= 6:
                progress = dict(progress, stage=6, cursor=None, complete=False, verification={})
        if progress.get('currentProof'):
            current_proof_snapshot(connection, progress['currentProof']['manifestHash'])
        reconciled_head = None
        if previous_hash and not (Path(receipt_dir) / ('batch-%08d-%s.committed.json' % (receipt_no, previous_hash))).exists():
            reconciled_head = reconcile_pending_receipt(connection, PROGRESS_KEY, receipt_dir, receipt_no, previous_hash, promote=True)
        while not progress['complete'] and len(batches) < max_batches:
            budget.check()
            space_before = storage_observation(path,receipt_dir,database_free_floor,receipt_free_floor)
            if not connection.in_transaction:
                connection.execute('BEGIN IMMEDIATE')
            commit_started = commit_succeeded = False
            source = stages[progress['stage']]
            before = dict(progress)
            if source.startswith('legacy:'):
                result = repository.bootstrap_legacy_batch(source=source, after=progress['cursor'], limit=batch_size,
                    restart=source in progress.get('metadataRestartPending', []), current_only=current_mode)
                cursor = result.get('lastKey')
            elif source.startswith('current:'):
                result = repository.register_legacy_current_batch(source.split(':', 1)[1], cursor=progress['cursor'],
                    limit=batch_size, max_queries=min(400,max(1,budget.max_queries-budget.queries)),
                    seconds=min(1,budget.remaining_seconds()), restart=source in progress['currentRestartPending'])
                cursor = result.get('cursor')
            elif source.startswith('verify:'):
                revisions = current_source_revisions(connection)
                verification = dict(progress.get('verification') or {'revisionsBefore': revisions, 'revisionsStable': all(
                    value is not None for value in revisions.values()), 'stores': {}})
                verification['revisionsStable'] = verification['revisionsStable'] and verification['revisionsBefore'] == revisions
                verification['revisionsLast'] = revisions
                if progress.get('currentProof'):
                    proof = current_proof_snapshot(connection, progress['currentProof']['manifestHash'])
                    if 'proofBefore' not in verification:
                        verification.update(proofBefore=proof, proofStable=True, proofStartedAt=now())
                    verification['proofStable'] = verification['proofStable'] and verification['proofBefore'] == proof
                result = repository.verify_current_batch(source.split(':', 1)[1], cursor=progress['cursor'], limit=batch_size,
                    max_queries=min(400,max(1,budget.max_queries-budget.queries)), seconds=min(1,budget.remaining_seconds()))
                cursor = result.get('cursor')
                totals = dict(verification['stores'])
                prior = totals.get(result['store'], {'checked': 0, 'sourceDigest': '0'})
                totals[result['store']] = {'checked': prior['checked'] + result['checked'], 'done': result['done'],
                    'sourceDigest': format((int(prior['sourceDigest'], 16) + int(result['sourceDigest'], 16)) % (1 << 256), '064x')}
                verification['stores'] = totals
                progress = dict(progress, verification=verification)
            else:
                result = repository.register_legacy_history_batch(source.split(':',1)[1],
                    cursor=progress['cursor'],limit=batch_size,
                    max_queries=min(400,max(1,budget.max_queries-budget.queries)),
                    seconds=min(1,budget.remaining_seconds()),quiescent=quiescent,
                    restart=source in progress['historyRestartPending'])
                cursor = result.get('cursor')
            if result.get('stoppedReason') not in (None,'limit','eof'):
                raise MigrationFailure('core discovery stopped before commit: '+str(result['stoppedReason']))
            validation = repository.validate_affected(result)
            if not validation.get('valid'):
                raise MigrationFailure('affected registry/binding invariant mismatch')
            recovery = snapshot_affected(connection,result)
            # The first successful batch consumes this store's restart intent
            # atomically with its core pass, external receipt and control head.
            # A rollback leaves the old intent available for an ordinary retry.
            progress = dict(progress,cursor=cursor,processed=progress['processed']+int(result.get('processed',0)),
                            historyRestartPending=[stage for stage in progress['historyRestartPending'] if stage != source])
            if current_mode:
                progress = dict(progress,
                    metadataRestartPending=[stage for stage in progress['metadataRestartPending'] if stage != source],
                    currentRestartPending=[stage for stage in progress['currentRestartPending'] if stage != source])
            if result['done']:
                progress['stage'] += 1
                progress['cursor'] = None
                progress['complete'] = progress['stage'] == len(stages)
            receipt_no += 1
            body = {'version':2,'receiptNo':receipt_no,'previousHash':previous_hash,
                    'schemaHash':schema_hash,'source':source,'createdAt':now(),
                    'progressBefore':before,'progressAfter':progress,'result':result,
                    'validation':validation,'affectedRegistryAfter':recovery,
                    'registryMetaAfter':[dict(row) for row in connection.execute(
                        "SELECT key,value FROM catalog_store_meta WHERE key IN ('schema_version','generation') ORDER BY key")],
                    'metadataProgressAfter':[dict(row) for row in connection.execute(
                        'SELECT * FROM catalog_migration_v2 WHERE source=?',(source,))] if source.startswith('legacy:') else [],
                    'historyProgressAfter':[dict(row) for row in connection.execute(
                        'SELECT * FROM catalog_history_coverage_v2 WHERE store=?',(source.split(':',1)[1],))] if source.startswith(('history:', 'current:')) else [],
                    'historyBatchAfter':[dict(row) for row in connection.execute(
                        'SELECT * FROM catalog_history_batches_v2 WHERE batch_hash=?',(result.get('batchHash'),))] if source.startswith(('history:', 'current:')) else [],
                    'recoveryScope':'fresh-job additive reconstruction; not arbitrary prior-state undo or history backup',
                    'receiptDurability':'file fsync + atomic rename; operator-qualified receipt storage, not a hardware guarantee',
                    'storageBefore':space_before,
                    'currentProofSchemaInstalled': proof_installation,
                    'currentProofAfter': current_proof_snapshot(connection, progress['currentProof']['manifestHash']) if progress.get('currentProof') else None,
                    'workBeforeReceipt':budget.snapshot()}
            head = digest(body)
            candidate = {'receiptNo':receipt_no,'receiptHash':head}
            connection.execute('INSERT INTO '+RECEIPTS+' VALUES(?,?,?,?)',(PROGRESS_KEY,receipt_no,canonical(body),head))
            connection.execute('INSERT INTO '+CONTROL+' VALUES(?,?,?,?,?) ON CONFLICT(migration_key) DO UPDATE SET '
                'progress_json=excluded.progress_json,receipt_no=excluded.receipt_no,receipt_hash=excluded.receipt_hash,schema_hash=excluded.schema_hash',
                (PROGRESS_KEY,canonical(progress),receipt_no,head,schema_hash))
            budget.check(connection,page_start)
            pending = write_external_receipt(receipt_dir,body,head,'pending')
            budget.check()
            # COMMIT/rollback device latency cannot honestly be time-capped.
            connection.set_progress_handler(None,0)
            try:
                commit_started = True
                connection.commit()
                commit_succeeded = True
            except sqlite3.Error as exc:
                outcome = 'uncertain_commit_check_candidate_receipt' if not connection.in_transaction else 'rolled_back_pending_batch'
                raise MigrationFailure(str(exc),batches,candidate,outcome) from exc
            batches.append({**candidate,'source':source,'pendingReceipt':pending,'result':result})
            previous_hash,candidate = head,None
            proof_installation = None
            connection.set_progress_handler(budget.progress,100)
            try:
                batches[-1]['committedReceipt'] = write_external_receipt(receipt_dir,body,head,'committed')
            except Exception as exc:
                raise MigrationFailure(str(exc),batches,None,'committed_db_pending_external_receipt_preserved') from exc
        if not connection.in_transaction:
            connection.execute('BEGIN')
        final_proof = current_proof_snapshot(connection, progress['currentProof']['manifestHash']) if progress.get('currentProof') else None
        final_revisions = current_source_revisions(connection) if current_mode else None
        if connection.in_transaction:
            connection.rollback()
        observation_end = storage_observation(path,receipt_dir,database_free_floor,receipt_free_floor)
        return {'mode':'explicit_apply','database':str(path.resolve()),'batches':batches,
                'reconciledPendingHead': reconciled_head,
                'currentReadiness': current_readiness(progress, final_revisions, final_proof),
                'progress':progress,'receiptHead':previous_hash,'history_full_scan_or_copy':False,'history_rows_modified':0,
                'automatic_checkpoint_disabled':True,
                'activation_or_restart':False,'work':budget.snapshot(),
                'storageAfter':observation_end,
                'observedByteDelta':{key:observation_end[key]-observation_start[key] for key in ('databaseBytes','walBytes')},
                'recovery':'Logical fresh-job reconstruction from schema+receipt chain, not a physical disaster backup.',
                'rollback':'Before ingestion retain IDs and disable flags. After ingestion retain v2 data+reader or export/verify exact new deltas before old code; never restore an old database.'}
    except Exception as exc:
        connection.set_progress_handler(None,0)
        if connection.in_transaction:
            connection.rollback()
        if isinstance(exc,MigrationFailure):
            if not exc.committed:
                exc.committed=batches
            if exc.candidate is None:
                exc.candidate=candidate
            if commit_succeeded:
                exc.outcome='committed_db_check_receipt_head'
            raise
        outcome = ('committed_db_check_receipt_head' if commit_succeeded else
                   'uncertain_commit_check_candidate_receipt' if commit_started and not connection.in_transaction else
                   'rolled_back_pending_batch')
        raise MigrationFailure(budget.reason or str(exc),batches,candidate,outcome) from exc
    finally:
        budget.detach()
        connection.close()


def reconcile_receipt(path, receipt_dir, receipt_no, receipt_hash, promote=False, receipt_storage_qualified=False):
    if receipt_dir is None or (promote and not receipt_storage_qualified):
        raise ValueError('original receipt directory and qualified storage required for promotion')
    path = Path(path)
    if not path.is_file():
        raise ValueError('existing database path required')
    connection = sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=0)
    budget = Budget(seconds=2, queries=100, vm_steps=100000)
    try:
        connection.execute('PRAGMA query_only=ON')
        budget.attach(connection)
        connection.execute('BEGIN')
        validate_schema(connection, partial=True)
        result = reconcile_pending_receipt(connection, PROGRESS_KEY, receipt_dir, receipt_no, receipt_hash, promote)
        budget.check()
        return {'mode': 'exact_pending_receipt_reconciliation', **result, 'work': budget.snapshot()}
    finally:
        budget.detach()
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', type=Path, required=True)
    parser.add_argument('--apply', action='store_true', help='explicitly create additive catalog schema and commit bounded source batches')
    parser.add_argument('--batch-size',type=int,default=10)
    parser.add_argument('--max-batches',type=int,default=1)
    parser.add_argument('--receipt-dir',type=Path)
    parser.add_argument('--receipt-storage-qualified',action='store_true',help='operator qualification of receipt-volume durability; does not create an independent DB backup')
    parser.add_argument('--database-free-floor',type=int,default=1073741824,help='engineering free-space stop floor in bytes, not a reservation')
    parser.add_argument('--receipt-free-floor',type=int,default=67108864)
    parser.add_argument('--expected-receipt')
    parser.add_argument('--new-history-pass',action='store_true')
    parser.add_argument('--current-only',action='store_true',help='adopt fresh/pilot metadata progress; register and verify six public current stores without history')
    parser.add_argument('--new-current-pass',action='store_true')
    parser.add_argument('--restart-verification',action='store_true')
    parser.add_argument('--enable-current-proof',action='store_true',help='explicitly install/enroll six-source revision triggers atomically with the next receipt; activation remains separate')
    parser.add_argument('--reconcile-receipt-number',type=int)
    parser.add_argument('--candidate-hash')
    parser.add_argument('--promote-pending',action='store_true')
    parser.add_argument('--quiescent',action='store_true',help='operator assertion only; never stops writers')
    parser.add_argument('--max-seconds',type=float,default=3)
    parser.add_argument('--max-queries',type=int,default=2000)
    parser.add_argument('--max-vm-steps',type=int,default=1000000)
    parser.add_argument('--max-changed-rows',type=int,default=4000)
    parser.add_argument('--max-write-sql-bytes',type=int,default=1048576)
    parser.add_argument('--max-new-page-bytes',type=int,default=1048576)
    args=parser.parse_args()
    try:
        if args.reconcile_receipt_number is not None:
            if args.apply:
                raise ValueError('reconcile an exact candidate separately from apply')
            result = reconcile_receipt(args.database, args.receipt_dir, args.reconcile_receipt_number,
                args.candidate_hash, args.promote_pending, args.receipt_storage_qualified)
            print(json.dumps(result, indent=2))
            return 0
        result=apply_batches(args.database,args.batch_size,args.max_batches,
            receipt_dir=args.receipt_dir,expected_receipt=args.expected_receipt,
            new_history_pass=args.new_history_pass,quiescent=args.quiescent,
            current_only=args.current_only,new_current_pass=args.new_current_pass,restart_verification=args.restart_verification,
            enable_current_proof=args.enable_current_proof,
            max_seconds=args.max_seconds,max_queries=args.max_queries,max_vm_steps=args.max_vm_steps,
            max_changed_rows=args.max_changed_rows,max_write_sql_bytes=args.max_write_sql_bytes,
            max_new_page_bytes=args.max_new_page_bytes,receipt_storage_qualified=args.receipt_storage_qualified,
            database_free_floor=args.database_free_floor,receipt_free_floor=args.receipt_free_floor) if args.apply else read_plan(args.database)
    except MigrationFailure as exc:
        print(json.dumps({'mode':'failed_closed','error':str(exc),'outcome':exc.outcome,
            'committedBatches':exc.committed,'candidateReceipt':exc.candidate,
            'nextStep':'Check persisted head before retrying uncertain commit; preserve all history.'},indent=2))
        return 2
    print(json.dumps(result,indent=2))
    return 0


if __name__=='__main__':
    sys.exit(main())
