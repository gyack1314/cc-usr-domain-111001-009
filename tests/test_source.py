"""来源样本和本地HTTP服务的回归检查。"""
import hashlib
import json
import unittest
from urllib.request import urlopen
from source_site.source import Source
from source_site.__main__ import running


class SourceTests(unittest.TestCase):
    def test_source_1(self):
        s = Source()
        self.assertEqual([3,1,3], [e["offset"] for e in s.request("GET", "/push-sequence")[2]])

    def test_source_2(self):
        s = Source()
        s.phase = "rotated"
        self.assertEqual(410, s.request("GET", "/events", {"epoch":["e1"], "after":["3"]})[0])

    def test_source_3(self):
        s = Source()
        s.phase = "snapshot-fail"
        self.assertEqual(503, s.request("GET", "/snapshot", {"page":["2"]})[0])

    def test_source_4(self):
        s = Source()
        s.phase = "rotated"
        r = s.request("GET", "/events", {"epoch":["e2"], "after":["10"]})[2]
        self.assertEqual("delete", r["events"][0]["operation"])

    def test_http_source(self):
        with running() as (base, source):
            with urlopen(base + "/push-sequence", timeout=3) as response:
                self.assertEqual(200, response.status)
                self.assertTrue(response.read())


if __name__ == "__main__":
    unittest.main()
