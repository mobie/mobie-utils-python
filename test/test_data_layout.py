import os
import unittest
from shutil import rmtree


class TestDataLayout(unittest.TestCase):
    root = './tmp'

    def tearDown(self):
        try:
            rmtree(self.root)
        except OSError:
            pass

    def test_make_initial_layout(self):
        from mmb.data_layout import make_initial_layout
        make_initial_layout(self.root)
        self.assertTrue(os.path.exists(os.path.join(self.root, '0.1.0')))

    def test_copy_version_folder(self):
        from mmb.data_layout import copy_version_folder


if __name__ == '__main__':
    unittest.main()
