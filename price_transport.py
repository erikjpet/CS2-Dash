"""Bounded existing-Node bridge for registered public price GETs only."""
import json,os,shutil,subprocess,sys,threading,time
from pathlib import Path

def node_binary():
 """Honor explicit configuration, then normal PATH, then the bundled runtime."""
 configured=os.environ.get('CS2_CASH_NODE')
 if configured:
  selected=Path(configured).expanduser()
  return selected if selected.is_absolute()and selected.is_file()else None
 for name in ('node','nodejs'):
  found=shutil.which(name)
  if found:
   selected=Path(found)
   if selected.is_file():return selected.resolve()
 runtime=Path(sys.executable).resolve().parent.parent
 for relative in ('node/bin/node.exe','node/bin/node','node/node.exe'):
  selected=runtime/relative
  if selected.is_file():return selected
 return None

def fetch(provider,name=None,held_names=None):
 binary=node_binary()
 if binary is None:return {'status':None,'error':'Node runtime unavailable; install Node on PATH or set CS2_CASH_NODE to its absolute executable path'}
 request={'provider':provider}
 if name is not None:request['name']=name
 if provider=='skinport_sales':request['heldNames']=held_names or []
 payload=json.dumps(request,ensure_ascii=False).encode()
 if len(payload)>(262144 if provider=='skinport_sales'else 4096):raise ValueError('Bounded local request required')
 p=subprocess.Popen([str(binary),'--max-old-space-size=128',str(Path(__file__).with_suffix('.mjs'))],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
 out,err=bytearray(),bytearray();failed=threading.Event()
 def reader(pipe,target,cap):
  try:
   while block:=pipe.read(65536):
    if len(target)+len(block)>cap:failed.set();break
    target.extend(block)
  except OSError:failed.set()
  finally:pipe.close()
 threads=[threading.Thread(target=reader,args=(p.stdout,out,8*1024**2),daemon=True),threading.Thread(target=reader,args=(p.stderr,err,16384),daemon=True)]
 for t in threads:t.start()
 p.stdin.write(payload);p.stdin.close();until=time.monotonic()+25
 while p.poll()is None:
  if failed.is_set()or time.monotonic()>until:p.kill();break
  time.sleep(.02)
 p.wait(timeout=2)
 for t in threads:t.join(1)
 if any(t.is_alive()for t in threads)or failed.is_set():return {'status':None,'error':'Transport pipe cap/closure failure'}
 try:return json.loads(out)
 except (ValueError,UnicodeError):return {'status':None,'error':'Invalid bounded transport response'}
