#!/usr/bin/env python3
"""Reproducible physical-device campaign with an independent causal-history oracle."""
import argparse
import json
from pathlib import Path
import random
import time
import uuid
from server import atomic


class Campaign:
    def __init__(self, state, seed):
        self.state = state
        self.cfg = json.loads((state / 'config.json').read_text())
        self.rng = random.Random(seed)
        self.run = f'{seed}-{int(time.time())}'
        self.evidence = state / 'campaigns' / self.run
        self.evidence.mkdir(parents=True)
        self.counter = 0
        self.latest = {}
        self.history = []
        self.ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f'synthetic://{self.run}/{i}')) for i in range(24)]
        self.phase = 'start'

    def event(self, **data):
        with (self.evidence / 'events.jsonl').open('a') as f:
            f.write(json.dumps({'time': time.time(), 'phase': self.phase, **data}) + '\n')

    def send(self, role, op, **args):
        self.counter += 1
        command = {'id': f'{self.run}-{self.counter:05}', 'op': op, **args}
        self.event(role=role, command=command)
        atomic(self.state / 'commands' / f'{role}.json', json.dumps(command).encode())
        return command['id']

    def wait(self, role, command_id, timeout=180):
        deadline = time.monotonic() + timeout
        f = self.state / 'reports' / f'{role}.json'
        while time.monotonic() < deadline:
            if f.exists():
                report = json.loads(f.read_text())
                if report['id'] == command_id:
                    self.latest[role] = report
                    self.event(role=role, report=report)
                    return report
            time.sleep(.2)
        raise TimeoutError(f'{role}: {command_id} did not complete in {timeout}s')

    def call(self, role, op, expect_ok=True, **args):
        report = self.wait(role, self.send(role, op, **args))
        if expect_ok:
            assert report['ok'], (role, op, report['result'])
        return report

    def commit(self, role, batches):
        context = dict(self.latest[role]['snapshot']['frontier'])
        actor = self.cfg['actors'][role]
        report = self.call(role, 'commit', batches=batches)
        for changes in batches:
            sequence = context.get(actor, 0) + 1
            known = [t['lamport'] for t in self.history if context.get(t['actor'], 0) >= t['sequence']]
            tx = {'actor':actor, 'sequence':sequence, 'context':dict(context),
                  'lamport':max(known, default=0)+1, 'changes':changes}
            self.history.append(tx)
            context[actor] = sequence
        assert report['snapshot']['frontier'] == context
        atomic(self.evidence / 'original-operations.json', json.dumps(self.history, indent=2).encode())

    def edits(self, count):
        return [[{'id':self.rng.choice(self.ids), 'value':None if self.rng.randrange(5)==0 else
                  {'url':'https://fixture.invalid/synthetic', 'title':f'synthetic-{self.rng.getrandbits(64)}'}}]
                for _ in range(count)]

    def oracle(self):
        candidates, winners, frontier = [], {}, {}
        for tx in self.history:
            frontier[tx['actor']] = max(frontier.get(tx['actor'], 0), tx['sequence'])
            for change in tx['changes']:
                if any(other['context'].get(tx['actor'], 0) >= tx['sequence'] and
                       any(edit['id'] == change['id'] for edit in other['changes']) for other in self.history):
                    continue
                candidates.append({'id':change['id'], 'actor':tx['actor'], 'sequence':tx['sequence'], 'value':change['value']})
                priority = (change['value'] is None, tx['lamport'], tx['actor'], tx['sequence'])
                if change['id'] not in winners or winners[change['id']][0] < priority:
                    winners[change['id']] = (priority, change['value'])
        bookmarks = [{'id':key, **value} for key, (_, value) in winners.items() if value is not None]
        return candidates, bookmarks, frontier

    def check(self):
        candidates, bookmarks, frontier = self.oracle()
        canonical = lambda rows: sorted(rows, key=lambda x:json.dumps(x, sort_keys=True))
        for role, report in self.latest.items():
            s = report['snapshot']
            assert s['pending'] == s['waiting'] == s['held'] == 0 and not s['poisoned'], (role, s)
            assert canonical(s['candidates']) == canonical(candidates), (role, 'causal candidates differ')
            assert canonical(s['bookmarks']) == canonical(bookmarks), (role, 'visible state differs')
            # An empty actor may have an explicit zero entry; both denote the same frontier.
            assert {a:n for a,n in s['frontier'].items() if n} == frontier, (role, 'frontier differs')
        self.event(check='passed', transactions=len(self.history), candidates=len(candidates))
        print(f'PASS {self.phase}: {len(self.history)} acknowledged transactions', flush=True)

    def converge(self):
        for _ in range(2):
            for role in ('pixel', 'iphone'):
                self.call(role, 'sync')
        self.check()

    def fault(self, role, mode, methods, **args):
        fault = {'id':f'{self.run}-{self.counter}-{mode}', 'role':role, 'mode':mode,
                 'methods':methods, 'count':3, **args}
        self.event(fault=fault)
        atomic(self.state / 'fault.json', json.dumps(fault).encode())

    def clear_fault(self):
        atomic(self.state / 'fault.json', b'{}')

    @classmethod
    def resume(cls, evidence):
        """Continue a successful campaign without resetting its oracle or device state."""
        c = object.__new__(cls)
        c.evidence = evidence
        c.state = evidence.parent.parent
        c.cfg = json.loads((c.state / 'config.json').read_text())
        c.run = evidence.name
        c.rng = random.Random(int(c.run.split('-')[0]) + 1)
        events = [json.loads(line) for line in (evidence / 'events.jsonl').read_text().splitlines()]
        c.counter = max(int(e['command']['id'].rsplit('-',1)[1]) for e in events if 'command' in e)
        c.history = json.loads((evidence / 'original-operations.json').read_text())
        c.ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f'synthetic://{c.run}/{i}')) for i in range(24)]
        c.latest = {role:json.loads((c.state/'reports'/f'{role}.json').read_text()) for role in ('pixel','iphone')}
        c.phase = 'resumed'
        return c

    def require_injected(self, report):
        assert not report['ok'], 'The selected fault did not interrupt the attempt'
        self.event(expected_failure=report['result'])

    def run_campaign(self, rounds):
        self.clear_fault()
        for role in ('pixel', 'iphone'):
            self.call(role, 'snapshot')
            assert not self.latest[role]['snapshot']['candidates'], 'Use a fresh lab space for a new campaign'
        self.phase = 'concurrent offline edits'
        for role in ('pixel', 'iphone'):
            self.commit(role, self.edits(12))
        self.converge()

        # The real bug found by the seeded host campaign, now over each phone's native stack.
        for author, peer in [('pixel','iphone'),('iphone','pixel')]:
            self.phase = f'{author}: lost acknowledgement and peer compaction'
            self.commit(author, self.edits(1))
            self.fault(author, 'lost-ack', ['PUT'], contains='tx-')
            self.require_injected(self.call(author, 'sync', expect_ok=False))
            self.clear_fault()
            self.call(peer, 'sync')
            self.call(peer, 'compact', deletions=0)
            self.call(author, 'sync')
            self.call(peer, 'compact')
            self.call(author, 'reopen')
            self.converge()

        modes = [('status-503',['PROPFIND']), ('status-401',['PROPFIND']),
                 ('status-429',['PROPFIND']), ('status-507',['PUT']),
                 ('omit-root',['PROPFIND']), ('malformed-xml',['PROPFIND']),
                 ('lost-ack',['PUT'])]
        for n in range(rounds):
            author = ('pixel','iphone')[n % 2]
            mode, methods = modes[n % len(modes)]
            self.phase = f'round {n}: {author} {mode}'
            for role in ('pixel', 'iphone'):
                self.commit(role, self.edits(self.rng.randrange(2, 9)))
            self.fault(author, mode, methods, contains='tx-' if methods == ['PUT'] else '')
            self.require_injected(self.call(author, 'sync', expect_ok=False))
            self.clear_fault()
            self.call(author, 'reopen')
            self.converge()
            if n % 4 == 0:
                self.call(author, 'compact', deletions=3)
                self.converge()

        for author in ('pixel','iphone'):
            self.phase = f'{author}: large offline outbox and snapshots'
            self.commit(author, self.edits(96))
            self.call(author, 'reopen')
            self.converge()
            self.call(author, 'compact', deletions=2)
            self.converge()
        self.phase = 'final complete audit'
        for role in ('pixel','iphone'):
            self.call(role, 'compact')
            self.call(role, 'reopen')
        self.converge()
        atomic(self.evidence / 'result.json', json.dumps({'passed':True,'commands':self.counter,'transactions':len(self.history),'rounds':rounds}).encode())


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state', type=Path, required=True)
    p.add_argument('--seed', type=int, default=20261123)
    p.add_argument('--rounds', type=int, default=28)
    a = p.parse_args()
    campaign = Campaign(a.state, a.seed)
    print(campaign.evidence, flush=True)
    try:
        campaign.run_campaign(a.rounds)
    except BaseException as error:
        campaign.event(failed=repr(error))
        atomic(campaign.evidence / 'result.json', json.dumps({'passed':False,'phase':campaign.phase,'error':repr(error)}).encode())
        raise
    finally:
        campaign.clear_fault()


if __name__ == '__main__':
    main()
