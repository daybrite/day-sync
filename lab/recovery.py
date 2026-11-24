#!/usr/bin/env python3
"""Continue a device campaign with real process kills, racing compaction and server loss."""
import argparse
import json
from pathlib import Path
import subprocess
import time
from campaign import Campaign
from server import atomic

APP = 'dev.daybrite.syncdevicelab'


class Devices:
    def __init__(self, campaign, adb, pixel, iphone):
        self.c = campaign
        self.adb, self.pixel, self.iphone = adb, pixel, iphone

    def command(self, args):
        r = subprocess.run(args, capture_output=True, text=True, timeout=90)
        self.c.event(device_command=args, returncode=r.returncode, stdout=r.stdout, stderr=r.stderr)
        r.check_returncode()
        return r.stdout

    def kill(self, role):
        if role == 'pixel':
            self.command([self.adb,'-s',self.pixel,'shell','am','force-stop',APP])
        else:
            output = self.c.evidence / 'iphone-processes.json'
            self.command(['xcrun','devicectl','device','info','processes','--device',self.iphone,
                          '--search','SyncDeviceLab','--json-output',str(output)])
            processes = json.loads(output.read_text())['result']['runningProcesses']
            assert len(processes) == 1 and processes[0]['executable'].endswith('/SyncDeviceLab.app/SyncDeviceLab')
            self.command(['xcrun','devicectl','device','process','terminate','--device',self.iphone,
                          '--pid',str(processes[0]['processIdentifier']),'--kill'])

    def launch(self, role):
        if role == 'pixel':
            self.command([self.adb,'-s',self.pixel,'shell','am','start','-n',APP+'/dev.daybrite.day.bridge.DayActivity'])
        else:
            self.command(['xcrun','devicectl','device','process','launch','--device',self.iphone,APP])


def wait_fault(c, fault_id):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        rows = (c.state / 'requests.jsonl').read_text().splitlines()
        if any(json.loads(line).get('fault_id') == fault_id for line in rows[-300:]):
            return
        time.sleep(.1)
    raise TimeoutError('Server never reached the requested crash boundary')


def forced_compaction_races(c):
    """Force the observed LIST/GET race on each native transport and both entry points."""
    for reader, peer in [('pixel','iphone'),('iphone','pixel')]:
        for operation in ('sync','compact'):
            c.phase = f'{reader}: forced peer cleanup during {operation}'
            c.commit(peer, c.edits(5))
            c.call(peer, 'sync')
            started = time.time()
            c.fault(reader, 'delay', ['GET'], contains='tx-'+c.cfg['actors'][peer]+'.', count=1, seconds=15)
            fault_id = json.loads((c.state/'fault.json').read_text())['id']
            command_id = c.send(reader, operation)
            wait_fault(c, fault_id)
            c.call(peer, 'compact')
            report = c.wait(reader, command_id)
            c.clear_fault()
            assert report['ok'], (reader, operation, report['result'])
            requests = [json.loads(line) for line in (c.state/'requests.jsonl').read_text().splitlines()]
            assert any(r.get('time',0) >= started and r.get('role') == reader and
                       r.get('fault') == 'delay' and r.get('method') == 'GET' and r.get('status') == 404
                       for r in requests), 'The fixture did not actually delete the listed record before GET'
            c.converge()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--campaign', type=Path, required=True)
    p.add_argument('--adb', default='adb')
    p.add_argument('--pixel', required=True)
    p.add_argument('--iphone', required=True)
    p.add_argument('--races', type=int, default=12)
    a = p.parse_args()
    c = Campaign.resume(a.campaign)
    devices = Devices(c, a.adb, a.pixel, a.iphone)
    c.clear_fault()
    try:
        c.phase = 'resumed state verification'
        c.converge()
        for role in ('pixel','iphone'):
            c.phase = f'{role}: native timeout after server accepted upload'
            c.commit(role, c.edits(1))
            c.fault(role, 'stall-after-write', ['PUT'], contains='tx-', count=1, seconds=110)
            timed_out = c.call(role, 'sync', expect_ok=False)
            c.require_injected(timed_out)
            assert 10_000 <= timed_out['elapsed_ms'] <= 150_000, timed_out
            c.clear_fault()
            c.converge()

            c.phase = f'{role}: process death after acknowledged local writes'
            c.commit(role, c.edits(16))
            before = c.latest[role]['snapshot']
            old_boot = c.latest[role]['boot']
            devices.kill(role)
            devices.launch(role)
            after = c.call(role, 'snapshot')
            assert after['boot'] != old_boot
            assert after['snapshot'] == before, 'Acknowledged local state changed across process death'
            c.converge()

            c.phase = f'{role}: process death after server PUT before acknowledgement'
            c.commit(role, c.edits(1))
            old_boot = c.latest[role]['boot']
            c.fault(role, 'stall-after-write', ['PUT'], contains='tx-', count=1, seconds=60)
            fault_id = json.loads((c.state/'fault.json').read_text())['id']
            command_id = c.send(role, 'sync')
            wait_fault(c, fault_id)
            devices.kill(role)
            c.clear_fault()
            devices.launch(role)
            after = c.wait(role, command_id)
            assert after['boot'] != old_boot and after['ok'], after
            c.converge()

        for n in range(a.races):
            c.phase = f'overlapping publication and compaction {n}'
            for role in ('pixel','iphone'):
                c.commit(role, c.edits(5))
            for operation in ('sync','compact'):
                pending = {role:c.send(role, operation) for role in ('pixel','iphone')}
                for role, command in pending.items():
                    report = c.wait(role, command)
                    assert report['ok'], ('concurrent maintenance failed', role, report['result'])
            c.converge()

        forced_compaction_races(c)

        for author, reader in [('pixel','iphone'),('iphone','pixel')]:
            c.phase = f'{reader}: corrupted incoming transaction'
            c.commit(author, c.edits(1))
            c.call(author,'sync')
            c.fault(reader,'corrupt-body',['GET'],contains='tx-',count=20)
            c.require_injected(c.call(reader,'sync',expect_ok=False))
            c.clear_fault()
            c.converge()

        c.phase = 'remote snapshot part lost after successful compaction'
        c.call('pixel','compact')
        c.converge()
        dav = c.state / 'dav' / ('day-sync-v5-' + c.cfg['space'])
        frontier = c.oracle()[2]
        covering = []
        for manifest in sorted(dav.glob('snap-*.ds5')):
            if '.part-' in manifest.name:
                continue
            m = json.loads(manifest.read_text())
            if all(m['heads'].get(actor, {}).get('sequence', 0) >= n for actor, n in frontier.items()):
                covering.append(m)
        assert covering, 'No complete-state snapshot to exercise'
        m = covering[0]
        lost = dav / f"snap-{m['state']}.part-0.{m['parts'][0]['hash']}.ds5"
        assert lost.is_file(), 'The selected snapshot must be complete before injection'
        atomic(c.evidence / ('lost-' + lost.name), lost.read_bytes())
        c.event(remote_loss=lost.name)
        lost.unlink()
        repaired = c.call('pixel', 'sync')
        assert 'repaired_history: true' in repaired['result'], 'The injected loss did not exercise repair'
        c.converge()

        c.phase = 'fresh bootstrap after compaction and repaired server loss'
        for role in ('pixel','iphone'):
            report = c.call(role,'fresh')
            primary = report['snapshot']
            fresh = report['result']
            for key in ('bookmarks','candidates','pending','waiting','held','poisoned'):
                assert fresh[key] == primary[key], (role,'fresh reader differs',key)
            assert {a:n for a,n in fresh['frontier'].items() if n} == {a:n for a,n in primary['frontier'].items() if n}
        c.converge()
        atomic(c.evidence/'recovery-result.json',json.dumps({'passed':True,'commands':c.counter,
               'transactions':len(c.history),'races':a.races,'physical_kills':4,'fresh_readers':2}).encode())
    except BaseException as error:
        c.event(failed=repr(error))
        atomic(c.evidence/'recovery-result.json',json.dumps({'passed':False,'phase':c.phase,'error':repr(error)}).encode())
        raise
    finally:
        c.clear_fault()


if __name__ == '__main__':
    main()
