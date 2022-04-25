import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file

from mobie import add_image
from mobie.validation import validate_project


class TestUtil(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    tmp_folder = './test-folder/tmp'
    dataset_name = 'test'

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        shape = (64,) * 3
        with open_file(data_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.rand(*shape))

        tmp_folder = os.path.join(self.test_folder, 'tmp-init')

        raw_name = 'test-raw'
        scales = [[2, 2, 2]]
        add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(32,)*3, scale_factors=scales,
                  tmp_folder=tmp_folder)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_clone_dataset(self):
        from mobie.utils import clone_dataset
        ds2 = 'test-clone'
        clone_dataset(self.root, self.dataset_name, ds2)
        validate_project(
            self.root, assert_true=self.assertTrue, assert_in=self.assertIn, assert_equal=self.assertEqual
        )


if __name__ == '__main__':
    unittest.main()
