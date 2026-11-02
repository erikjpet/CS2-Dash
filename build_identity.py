"""Public build identity from application code only; never reads runtime data."""
import hashlib
import os
from pathlib import Path
import re
import subprocess

CODE_SUFFIXES=frozenset({'.py','.mjs','.sql'})

def capture(base,version):
 base=Path(base).resolve();files={}
 for item in sorted(base.iterdir(),key=lambda p:p.name):
  if item.is_file()and(item.suffix in CODE_SUFFIXES or item.name=='index.html'):
   files[item.name]=hashlib.sha256(item.read_bytes()).hexdigest()
 digest=hashlib.sha256()
 for name,checksum in files.items():digest.update((name+'\0'+checksum+'\n').encode('utf-8'))
 revision=os.environ.get('CS2DASH_SOURCE_REVISION','').strip().lower()
 origin='environment'
 if not re.fullmatch(r'[0-9a-f]{40}',revision):
  revision=None;origin=None
  if(base/'.git').exists():
   try:
    result=subprocess.run(['git','-c','safe.directory='+base.as_posix(),'-C',str(base),'rev-parse','HEAD'],capture_output=True,text=True,timeout=2,check=True)
    value=result.stdout.strip().lower()
    if re.fullmatch(r'[0-9a-f]{40}',value):revision=value;origin='checkout'
   except(OSError,subprocess.SubprocessError):pass
 return {'version':str(version),'sourceRevision':revision,'revisionOrigin':origin,'runtimeSha256':digest.hexdigest(),
         'frontendSha256':files.get('index.html'),'serverSha256':files.get('server.py'),'runtimeFileCount':len(files)}
