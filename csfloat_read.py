"""Backend-only CSFloat credentials and bounded read transport. No import I/O."""
import base64,ctypes,json,os,subprocess,threading,time
from pathlib import Path

class CredentialUnavailable(ValueError):pass

def key_path():
 return Path(os.environ.get('CS2DASH_CSFLOAT_KEY_FILE')or(Path(os.environ.get('LOCALAPPDATA')or Path.home()/'AppData/Local')/'CS2Dash/csfloat-listing-key.dpapi'))

def configured():return bool(os.environ.get('CS2DASH_CSFLOAT_API_KEY'))or key_path().is_file()

def credential():
 """Read only this configured secret; never return its value in diagnostics."""
 value=os.environ.get('CS2DASH_CSFLOAT_API_KEY')
 if not value:
  try:
   if os.name!='nt':raise ValueError()
   path=key_path()
   if path.stat().st_size>8192:raise ValueError()
   encrypted=base64.b64decode(path.read_bytes(),validate=True)
   class Blob(ctypes.Structure):_fields_=[('size',ctypes.c_uint32),('data',ctypes.POINTER(ctypes.c_ubyte))]
   memory=(ctypes.c_ubyte*len(encrypted)).from_buffer_copy(encrypted);source=Blob(len(encrypted),memory);output=Blob()
   crypt=ctypes.WinDLL('crypt32',use_last_error=True);kernel=ctypes.WinDLL('kernel32',use_last_error=True)
   if not crypt.CryptUnprotectData(ctypes.byref(source),None,None,None,None,1,ctypes.byref(output)):raise ValueError()
   try:value=ctypes.string_at(output.data,output.size).decode('utf8')
   finally:kernel.LocalFree(ctypes.cast(output.data,ctypes.c_void_p))
  except Exception:raise CredentialUnavailable('Private CSFloat credential unavailable for this runtime user')from None
 if not isinstance(value,str)or not 8<=len(value)<=1024 or any(ord(c)<33 or ord(c)>126 for c in value):raise CredentialUnavailable('Private CSFloat credential format rejected')
 return value

def fetch(name):
 if not isinstance(name,str)or not 0<len(name)<=512 or any(c in name for c in '\0\r\n'):raise ValueError('One exact public market hash required')
 key=credential();binary=Path(os.environ.get('CS2_CASH_NODE')or(Path(__import__('sys').executable).resolve().parent.parent/'node/bin/node.exe'))
 if not binary.is_file():return {'status':None,'error':'Existing Node runtime unavailable'}
 env={k:v for k,v in os.environ.items()if k.upper()in('PATH','SYSTEMROOT','WINDIR','TEMP','TMP','USERPROFILE','LOCALAPPDATA','NODE_EXTRA_CA_CERTS')}
 env['CS2DASH_CSFLOAT_API_KEY']=key
 process=subprocess.Popen([str(binary),'--max-old-space-size=64',str(Path(__file__).with_name('csfloat_transport.mjs'))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=env,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
 out=bytearray();failed=threading.Event()
 def drain(pipe,save,cap):
  length=0
  try:
   while block:=pipe.read(16384):
    length+=len(block)
    if length>cap:failed.set();break
    if save:out.extend(block)
  except OSError:failed.set()
  finally:pipe.close()
 threads=[threading.Thread(target=drain,args=(process.stdout,True,262144),daemon=True),threading.Thread(target=drain,args=(process.stderr,False,8192),daemon=True)]
 for thread in threads:thread.start()
 process.stdin.write(json.dumps({'name':name}).encode());process.stdin.close();until=time.monotonic()+25
 while process.poll()is None:
  if failed.is_set()or time.monotonic()>until:process.kill();break
  time.sleep(.02)
 process.wait(timeout=2)
 for thread in threads:thread.join(1)
 if failed.is_set()or any(t.is_alive()for t in threads):return {'status':None,'error':'CSFloat bounded transport failed'}
 # Fail closed on accidental reflection before data can reach storage or status.
 if key.encode()in out:return {'status':None,'terminal':True,'error':'CSFloat response redaction guard rejected output'}
 try:return json.loads(out)
 except Exception:return {'status':None,'error':'Invalid CSFloat transport response'}

def admission(headers,now):
 """Honor known endpoint headers; unknown limits keep conservative local pacing."""
 headers=headers or {};deadline=now+60
 from quote_book import retry_deadline
 if headers.get('retry-after'):deadline=max(deadline,retry_deadline(headers['retry-after'],now))
 remaining=headers.get('x-ratelimit-remaining',headers.get('ratelimit-remaining'))
 reset=headers.get('x-ratelimit-reset',headers.get('ratelimit-reset'))
 try:
  left=int(remaining);value=int(reset)
  # Only an unambiguous Unix reset is used. Other reset formats cannot shorten pacing.
  if value>1000000000000:value=(value+999)//1000
  if 0<=value<=86400:value=now+value
  if 0<=left<=1000000 and now<=value<=now+86400:
   if left<=1:deadline=max(deadline,value+1)
   else:deadline=max(deadline,now+((value-now)*2+left-1)//left)
 except(ValueError,TypeError):
  if str(remaining)=='0':deadline=max(deadline,now+900)
 return deadline
