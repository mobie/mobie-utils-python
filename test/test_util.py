import json
import os
import unittest
from shutil import rmtree

import imageio
import h5py
import numpy as np
from elf.io import open_file
from pybdv.util import get_key


# TODO
class TestUtil(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    tmp_folder = './test-folder/tmp'

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_clone_dataset(self):
        from mobie import clone_dataset

        ds1 = 'test'
        raw_name = 'test-raw'
        shape = (128, 128, 128)
        self.init_h5_dataset(ds1, raw_name, shape)

        ds2 = 'test-clone'
        clone_dataset(self.root, ds1, ds2)
        self.check_dataset(os.path.join(self.root, ds2), shape, raw_name)


if __name__ == '__main__':
    unittest.main()
