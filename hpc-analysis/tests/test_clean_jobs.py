import unittest
import numpy as np
from src.clean_jobs import parse_hms_or_dhms, parse_reqmem

class TestCleanJobs(unittest.TestCase):
    def test_parse_hms_or_dhms(self):
        self.assertEqual(parse_hms_or_dhms("01:02:03"), 3723)
        self.assertEqual(parse_hms_or_dhms("2-00:00:00"), 172800)
        self.assertTrue(np.isnan(parse_hms_or_dhms("")))
        self.assertTrue(np.isnan(parse_hms_or_dhms(None)))
        self.assertTrue(np.isnan(parse_hms_or_dhms("not-a-time")))

    def test_parse_reqmem(self):
        self.assertEqual(parse_reqmem("4Gn", nnodes=2, ncpus=1), 8192)
        self.assertEqual(parse_reqmem("4000M", nnodes=1, ncpus=1), 4000)
        self.assertEqual(parse_reqmem("2Gc", nnodes=1, ncpus=4), 8192)
        self.assertAlmostEqual(parse_reqmem("8K", nnodes=1, ncpus=1), 8/1024)
        self.assertTrue(np.isnan(parse_reqmem("", nnodes=1, ncpus=1)))
        self.assertTrue(np.isnan(parse_reqmem("bad", nnodes=1, ncpus=1)))

if __name__ == "__main__":
    unittest.main()
