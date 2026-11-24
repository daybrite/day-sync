"""Sanity-check the lab's independent oracle, including deliberately wrong results."""
import json
from pathlib import Path
import tempfile
import unittest
from campaign import Campaign


class OracleTests(unittest.TestCase):
    def test_concurrent_delete_survives_and_causal_recreation_replaces_it(self):
        c = object.__new__(Campaign)
        value = {'url':'https://fixture.invalid', 'title':'synthetic'}
        c.history = [
            {'actor':'a','sequence':1,'lamport':1,'context':{},'changes':[{'id':'x','value':value}]},
            {'actor':'b','sequence':1,'lamport':1,'context':{},'changes':[{'id':'x','value':None}]},
        ]
        candidates, bookmarks, frontier = c.oracle()
        self.assertEqual(len(candidates), 2)
        self.assertEqual(bookmarks, [])
        self.assertEqual(frontier, {'a':1,'b':1})
        c.history.append({'actor':'a','sequence':2,'lamport':2,'context':frontier,
                          'changes':[{'id':'x','value':value}]})
        candidates, bookmarks, _ = c.oracle()
        self.assertEqual(len(candidates), 1)
        self.assertEqual(bookmarks, [{'id':'x',**value}])

    def test_matching_devices_with_wrong_data_fail(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d)
            (p/'config.json').write_text(json.dumps({}))
            c = Campaign(p, 1)
            c.history = [{'actor':'a','sequence':1,'lamport':1,'context':{},
                          'changes':[{'id':'x','value':None}]}]
            wrong = {'snapshot':{'pending':0,'waiting':0,'held':0,'poisoned':False,
                                 'frontier':{'a':1},'candidates':[],'bookmarks':[]}}
            c.latest = {'pixel':wrong,'iphone':wrong}
            with self.assertRaisesRegex(AssertionError, 'causal candidates differ'):
                c.check()


if __name__ == '__main__':
    unittest.main()
