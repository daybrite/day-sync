"""Exercise fixture behavior over HTTP so an injection cannot silently be a no-op."""
import base64
import json
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request


class FixtureTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.state = Path(self.temp.name)
        (self.state/'config.json').write_text(json.dumps({'token':'synthetic', 'password':'fixture-password'}))
        with socket.socket() as s:
            s.bind(('127.0.0.1',0))
            port = s.getsockname()[1]
        self.url = f'http://127.0.0.1:{port}/synthetic/'
        self.proc = subprocess.Popen([sys.executable,str(Path(__file__).with_name('server.py')),
                                      '--state',str(self.state),'--port',str(port)],
                                     stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        for _ in range(100):
            try:
                self.request('GET','control/pixel.json')
                return
            except OSError:
                time.sleep(.02)
        self.fail('fixture did not start')

    def tearDown(self):
        self.proc.terminate()
        self.proc.communicate(timeout=5)
        self.temp.cleanup()

    def request(self, method, path, body=None, auth=True, headers=None):
        h = dict(headers or {})
        if auth:
            h['Authorization'] = 'Basic '+base64.b64encode(b'pixel:fixture-password').decode()
        request = urllib.request.Request(self.url+path,data=body,headers=h,method=method)
        try:
            response = urllib.request.urlopen(request,timeout=3)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, response.read()

    def test_lost_ack_persists_exact_bytes_and_create_only_refuses_overwrite(self):
        self.assertEqual(self.request('MKCOL','dav/space/')[0],201)
        (self.state/'fault.json').write_text(json.dumps({'id':'lost','role':'pixel','methods':['PUT'],'mode':'lost-ack'}))
        self.assertEqual(self.request('PUT','dav/space/tx-fixture',b'synthetic-data')[0],503)
        self.assertEqual(self.request('GET','dav/space/tx-fixture'),(200,b'synthetic-data'))
        self.assertEqual(self.request('PUT','dav/space/tx-fixture',b'wrong',headers={'If-None-Match':'*'})[0],412)
        self.assertEqual(self.request('GET','dav/space/tx-fixture')[1],b'synthetic-data')

    def test_auth_and_incomplete_listing(self):
        self.assertEqual(self.request('PUT','dav/unauthorized',b'wrong',auth=False)[0],401)
        self.assertFalse((self.state/'dav/unauthorized').exists())
        self.request('MKCOL','dav/space/')
        self.request('PUT','dav/space/tx-fixture',b'synthetic-data')
        (self.state/'fault.json').write_text(json.dumps({'id':'listing','methods':['PROPFIND'],'mode':'omit-root'}))
        status, body = self.request('PROPFIND','dav/space/',headers={'Depth':'1'})
        self.assertEqual(status,207)
        self.assertNotIn(b'<d:href>/synthetic/dav/space/</d:href>',body)
        self.assertIn(b'tx-fixture',body)


if __name__ == '__main__':
    unittest.main()
