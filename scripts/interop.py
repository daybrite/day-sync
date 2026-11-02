#!/usr/bin/env python3
# Copyright © The Daybrite Project
# SPDX-License-Identifier: MPL-2.0
"""Run synthetic sync lifecycles against disposable Apache and a prestarted Nextcloud.
Uses the sibling DAV fixture launcher; never targets a real account. A forwarding proxy
can drop a response after the real server accepts PUT. Evidence contains no credentials.
"""
import contextlib, http.client, http.server, importlib.util, json, os, socket, subprocess, threading, time, urllib.parse
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]
DAV = Path(os.environ.get("DAY_WEBDAV", ROOT.parent / "day-webdav"))
spec = importlib.util.spec_from_file_location('dav_capture', DAV / 'scripts/capture.py')
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)

@contextlib.contextmanager
def proxy(upstream):
    target = urllib.parse.urlsplit(upstream)
    evidence = {'accepted_put_responses_dropped': 0, 'requests': {}}
    seen = set()
    lock = threading.Lock()
    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        def log_message(self, *args): pass
        def forward(self):
            body = self.rfile.read(int(self.headers.get('Content-Length', '0')))
            connection = http.client.HTTPConnection(target.hostname, target.port, timeout=20)
            headers = {k:v for k,v in self.headers.items() if k.lower() not in ('host','connection','transfer-encoding')}
            connection.request(self.command, self.path, body, headers)
            response = connection.getresponse(); data = response.read(); status = response.status
            with lock:
                evidence['requests'][self.command] = evidence['requests'].get(self.command, 0) + 1
                # Lose precisely the first PUT response per replica namespace (one per test).
                namespace = self.path.rsplit('/', 1)[0]
                drop = self.command == 'PUT' and '/tx-' in self.path and namespace not in seen and status in (200,201,204)
                if drop: seen.add(namespace); evidence['accepted_put_responses_dropped'] += 1
            if drop:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR); self.connection.close()
            else:
                self.send_response(status)
                for k,v in response.getheaders():
                    if k.lower() not in ('content-length','transfer-encoding','connection','server','date'):
                        self.send_header(k,v)
                self.send_header('Content-Length', str(len(data))); self.end_headers(); self.wfile.write(data)
            connection.close()
        do_MKCOL = do_PROPFIND = do_PUT = do_GET = do_DELETE = forward
    server = http.server.ThreadingHTTPServer(('127.0.0.1',0), Handler)
    thread = threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try: yield f'http://127.0.0.1:{server.server_port}{target.path}', evidence
    finally: server.shutdown(); server.server_close(); thread.join()

def run(name, url, version, fault=False):
    env=os.environ.copy();env.update(DAV_FIXTURE_URL=url,DAV_FIXTURE_PASSWORD=capture.SYNTHETIC_PASSWORD)
    if fault:env['DAY_SYNC_FAULT_PROXY']='1'
    else:env.pop('DAY_SYNC_FAULT_PROXY',None)
    result=subprocess.run(['cargo','test','--offline','-p','day-sync-webdav','--features','reqwest','--test','live','--','--ignored','--nocapture'],cwd=ROOT,env=env,text=True,capture_output=True)
    print(name,'lost-ack' if fault else 'direct',result.stdout,flush=True)
    if result.returncode: raise RuntimeError(result.stderr+result.stdout)
    return dict(server=name,version=version,mode='lost-ack' if fault else 'direct',providers=['native','reqwest'],passed=True)

if __name__ == '__main__':
    results=[]
    with capture.apache() as (url,version):
        results.append(run('Apache',url,version))
        with proxy(url) as (proxied,evidence):
            result=run('Apache',proxied,version,True);result['proxy']=evidence
            assert evidence['accepted_put_responses_dropped']==2
            results.append(result)
    c=http.client.HTTPConnection('127.0.0.1',18443,timeout=10);c.request('GET','/status.php');version=json.loads(c.getresponse().read())['versionstring'];c.close()
    url='http://127.0.0.1:18443/remote.php/dav/files/day_fixture/'
    results.append(run('Nextcloud',url,version))
    with proxy(url) as (proxied,evidence):
        result=run('Nextcloud',proxied,version,True);result['proxy']=evidence
        assert evidence['accepted_put_responses_dropped']==2
        results.append(result)
    destination=ROOT/"target"/"interop-results.json";destination.parent.mkdir(exist_ok=True)
    destination.write_text(json.dumps(dict(captured_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),fixture_only=True,results=results),indent=2)+'\n')
