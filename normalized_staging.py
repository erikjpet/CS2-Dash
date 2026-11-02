"""Private normalized stage bridge for the pinned legacy build callbacks.

The existing bounded engine still owns units, source fences, transactions and
budgets. Triggers apply only to an unpublished C: stage, never the main DB.
"""
from pathlib import Path
import hashlib
import json
import sqlite3

import bulk_derived_staging as core
import lookup_group_codec as codec
import normalized_lookup as layout
from generation_repository import DICTIONARY_KEY,INDEX_DDL

def fingerprint(*values):
    return hashlib.sha256(b'lookup-common-v1\0'+json.dumps(values,ensure_ascii=False,
        separators=(',',':'),allow_nan=False).encode()).digest()

def schema(metric_table_sql):
    common=','.join(layout.COMMON_COLUMNS)
    newcommon=','.join('NEW.'+name for name in layout.COMMON_COLUMNS)
    windows=','.join(layout.WINDOW_COLUMNS)
    newwindows=','.join('saved_encode_groups(NEW.groups_json)' if name=='groups_json' else 'NEW.'+name
                        for name in layout.WINDOW_COLUMNS)
    fp='saved_common_fingerprint('+newcommon+')'
    prior='SELECT common_id FROM lookup_common WHERE fingerprint='+fp
    exact=' AND '.join('c.'+name+' IS NEW.'+name for name in layout.COMMON_COLUMNS)
    result=metric_table_sql+';\n'+layout.schema_sql()+'''
CREATE TABLE settings(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE INDEX idx_item_market_metrics_window ON item_market_metrics(provider,window_days,name);
'''+ ';\n'.join(INDEX_DDL)+''';
CREATE TRIGGER normalized_lookup_insert INSTEAD OF INSERT ON market_item_lookup BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM lookup_common c WHERE fingerprint='''+fp+''' AND NOT('''+exact+'''))
 THEN RAISE(ABORT,'Common fingerprint collision; never merge nonidentical variants') END;
 INSERT INTO lookup_common(fingerprint,'''+common+''')
 SELECT '''+fp+','+newcommon+''' WHERE NOT EXISTS('''+prior+''');
 DELETE FROM lookup_windows WHERE window_days=NEW.window_days AND name=NEW.name;
 INSERT INTO lookup_windows(source_rowid,common_id,'''+windows+''')
 VALUES(CASE WHEN NEW.rowid IS NULL OR NEW.rowid<0 THEN NULL ELSE NEW.rowid END,('''+prior+'''),'''+newwindows+''');
END;
CREATE TRIGGER normalized_lookup_delete INSTEAD OF DELETE ON market_item_lookup BEGIN
 DELETE FROM lookup_windows WHERE source_rowid=OLD.rowid;
 DELETE FROM lookup_common WHERE common_id=OLD.common_id
  AND NOT EXISTS(SELECT 1 FROM lookup_windows WHERE common_id=OLD.common_id);
END;
'''
    # The public lookup view deliberately exposes only its original28 fields
    # plus rowid. The delete trigger resolves the internal common id itself.
    result=result.replace('common_id=OLD.common_id',
        'common_id=(SELECT common_id FROM lookup_common WHERE fingerprint=saved_common_fingerprint('+','.join('OLD.'+name for name in layout.COMMON_COLUMNS)+'))')
    result=result.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ').replace('CREATE VIEW ',
        'CREATE VIEW IF NOT EXISTS ').replace('CREATE INDEX ','CREATE INDEX IF NOT EXISTS ').replace('CREATE TRIGGER ',
        'CREATE TRIGGER IF NOT EXISTS ')
    return result

class StageConnection(sqlite3.Connection):
    builder=None

    def initialize_codec(self):
        exists=self.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='settings'").fetchone()
        raw=core.get_setting(self,DICTIONARY_KEY) if exists else None
        self.builder=codec.DictionaryBuilder(raw)
        self.create_function('saved_encode_groups',1,self.builder.encode)
        self.create_function('saved_common_fingerprint',len(layout.COMMON_COLUMNS),fingerprint,deterministic=True)

    def commit(self):
        if self.in_transaction:
            self.execute('INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                         (DICTIONARY_KEY,self.builder.serialized()))
        return super().commit()

    def rollback(self):
        super().rollback()
        self.initialize_codec()

    def __exit__(self,kind,value,traceback):
        if kind is None:
            try:self.commit()
            except BaseException:self.rollback();raise
        else:self.rollback()
        return False

class NormalizedStaging(core.Staging):
    """Uses the unchanged unit algorithm; pointer publication is explicit."""
    def open_stage(self,generation,create=False):
        path=self.path(generation)
        conn=sqlite3.connect(path.as_uri()+('?mode=rwc' if create else '?mode=rw'),
            uri=True,timeout=0,isolation_level=None,factory=StageConnection)
        try:
            conn.row_factory=sqlite3.Row
            conn.setconfig(sqlite3.SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,True)
            conn.execute('PRAGMA temp_store=MEMORY')
            conn.execute('PRAGMA cache_size=-2048')
            conn.execute('PRAGMA journal_mode=DELETE')
            page=conn.execute('PRAGMA page_size').fetchone()[0]
            conn.execute('PRAGMA max_page_count='+str(self.stage_budget.file_bytes//page))
            conn.initialize_codec()
            if create:conn.executescript(self.schema)
            if self.stage_size(path)>self.stage_budget.file_bytes:
                raise core.Deferred('Complete normalized stage plus journal exceeds the unchanged cap')
            return conn
        except BaseException:conn.close();raise

    def publish(self,generation):
        raise core.Deferred('Normalized stage requires a fully sealed repository pointer publication; canonical replacement is disabled')
