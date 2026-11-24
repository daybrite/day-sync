#!/usr/bin/env python3
"""Disposable authenticated DAV fault fixture and device command mailbox.

Only synthetic data belongs here. Bind localhost; put a trusted HTTPS tunnel in front.
The controller edits commands/ROLE.json and fault.json atomically in --state.
All accepted writes and completed requests are journaled for reconstruction.
"""
import argparse
import base64
import hashlib
import json
import os
import re
from pathlib import Path
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit, quote
from xml.sax.saxutils import escape


def atomic(path, body):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('wb') as f:
        f.write(body)
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state', type=Path, required=True)
    p.add_argument('--port', type=int, default=18765)
    a = p.parse_args()
    cfg = json.loads((a.state / 'config.json').read_text())
    root = a.state / 'dav'
    root.mkdir(exist_ok=True)
    lock = threading.RLock()
    fault_counts = {}

    def journal(record):
        with (a.state / 'requests.jsonl').open('a') as out:
            out.write(json.dumps({'time': time.time(), **record}) + '\n')

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, status, body=b'', extra=None):
            self.send_response(status)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Cache-Control', 'no-store')
            for k, v in (extra or {}).items():
                self.send_header(k, v)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass
            journal({'role': self.role, 'method': self.command, 'path': self.route,
                     'status': status, 'fault': self.fault})

        def handle_request(self):
            with lock:
                self.handle_locked()

        def handle_locked(self):
            self.role, self.fault = 'unknown', None
            self.route = unquote(urlsplit(self.path).path)
            try:
                auth = base64.b64decode(self.headers.get('Authorization', '').removeprefix('Basic ')).decode()
                self.role, password = auth.split(':', 1)
            except (ValueError, UnicodeError):
                password = ''
            if self.role not in ('pixel', 'iphone', 'desktop') or password != cfg['password']:
                self.reply(401, extra={'WWW-Authenticate': 'Basic realm="sync-lab"'})
                return
            if not self.route.startswith('/' + cfg['token'] + '/'):
                self.reply(404)
                return
            route = self.route[len(cfg['token']) + 2:]
            n = int(self.headers.get('Content-Length', '0'))
            if n > 16 * 1024 * 1024:
                self.reply(413)
                return
            body = self.rfile.read(n)
            if route == f'control/{self.role}.json' and self.command == 'GET':
                f = a.state / 'commands' / f'{self.role}.json'
                self.reply(200, f.read_bytes() if f.exists() else b'{"id":"idle","op":"idle"}')
                return
            if route == f'reports/{self.role}.json' and self.command == 'PUT':
                try:
                    report = json.loads(body)
                    valid = isinstance(report, dict) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', report.get('id', ''))
                except (ValueError, TypeError):
                    valid = False
                if not valid:
                    self.reply(400)
                    return
                atomic(a.state / 'reports' / f'{self.role}.json', body)
                atomic(a.state / 'history' / self.role / f"{report['id']}.json", body)
                self.reply(201)
                return
            if not route.startswith('dav/'):
                self.reply(404)
                return
            rel = route[4:].strip('/')
            if any(x in ('.', '..') for x in rel.split('/')):
                self.reply(400)
                return
            target = root / rel
            fault_file = a.state / 'fault.json'
            fault = json.loads(fault_file.read_text()) if fault_file.exists() else {}
            if (fault.get('role', self.role) == self.role and
                    self.command in fault.get('methods', []) and
                    fault.get('contains', '') in rel):
                key = fault['id']
                count = fault_counts.get(key, 0)
                fault_counts[key] = count + 1
                if count >= fault.get('skip', 0) and count < fault.get('skip', 0) + fault.get('count', 1):
                    self.fault = fault['mode']
            if self.fault and self.fault.startswith('status-'):
                self.reply(int(self.fault[7:]), extra={'Retry-After': '1'})
                return
            if self.fault == 'delay':
                # Release the global state lock during intentional network stalls.
                journal({'entered_fault':self.fault, 'fault_id':fault['id'], 'role':self.role,
                         'method':self.command, 'resource':rel})
                lock.release()
                try:
                    time.sleep(fault.get('seconds', 3))
                finally:
                    lock.acquire()
            if self.command == 'MKCOL':
                if target.exists():
                    self.reply(405)
                elif not target.parent.is_dir():
                    self.reply(409)
                else:
                    target.mkdir()
                    self.reply(201)
            elif self.command == 'PROPFIND':
                if not target.is_dir():
                    self.reply(404)
                    return
                entries = [target]
                if self.headers.get('Depth') != '0':
                    entries += sorted(target.iterdir())
                if self.fault == 'omit-root':
                    entries = entries[1:]
                xml = ['<?xml version="1.0"?><d:multistatus xmlns:d="DAV:">']
                for f in entries:
                    href = '/' + cfg['token'] + '/dav/' + ('' if f == root else str(f.relative_to(root)))
                    if f.is_dir() and not href.endswith('/'):
                        href += '/'
                    props = '<d:resourcetype><d:collection/></d:resourcetype>' if f.is_dir() else '<d:resourcetype/>'
                    props += '<d:getcontentlength>' + str(0 if f.is_dir() else f.stat().st_size) + '</d:getcontentlength>'
                    xml.append('<d:response><d:href>' + escape(quote(href, safe='/')) + '</d:href><d:propstat><d:prop>' + props + '</d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>')
                xml.append('</d:multistatus>')
                data = ''.join(xml).encode()
                if self.fault == 'malformed-xml':
                    data = data[:len(data)//2]
                self.reply(207, data, {'Content-Type': 'application/xml'})
            elif self.command in ('GET', 'HEAD'):
                if not target.is_file():
                    self.reply(404)
                    return
                data = target.read_bytes()
                if self.fault == 'corrupt-body':
                    data = data[:len(data)//2] + b'!'
                self.reply(200, data if self.command == 'GET' else b'', {'ETag': '"' + hashlib.sha256(data).hexdigest() + '"'})
            elif self.command == 'PUT':
                if not target.parent.is_dir():
                    self.reply(409)
                elif self.headers.get('If-None-Match') == '*' and target.exists():
                    self.reply(412)
                else:
                    atomic(target, body)
                    journal({'write': rel, 'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body), 'role': self.role})
                    if self.fault == 'stall-after-write':
                        journal({'entered_fault':self.fault, 'fault_id':fault['id'],
                                 'role':self.role, 'resource':rel})
                        lock.release()
                        try:
                            time.sleep(fault.get('seconds', 45))
                        finally:
                            lock.acquire()
                    self.reply(503 if self.fault == 'lost-ack' else 201)
            elif self.command == 'DELETE':
                if target.is_file():
                    target.unlink()
                    self.reply(503 if self.fault == 'lost-ack' else 204)
                else:
                    self.reply(404)
            else:
                self.reply(405)

        do_GET = do_HEAD = do_PUT = do_DELETE = do_MKCOL = do_PROPFIND = handle_request

    ThreadingHTTPServer(('127.0.0.1', a.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
