import json
import os
import subprocess
import unittest
from shutil import rmtree

import numpy as np

from elf.io import open_file
from pybdv.util import get_key
from mobie import initialize_dataset


class TestImageData(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    shape = (128, 128, 128)
    dataset_name = 'test'

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        with open_file(data_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        tmp_folder = os.path.join(self.test_folder, 'tmp-init')

        raw_name = 'test-raw'
        scales = [[2, 2, 2]]
        initialize_dataset(data_path, data_key, self.root, self.dataset_name, raw_name,
                           resolution=(1, 1, 1), chunks=(64, 64, 64), scale_factors=scales,
                           tmp_folder=tmp_folder)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

        self.im_path = os.path.join(self.test_folder, 'seg.h5')
        self.im_key = 'seg'
        self.data = np.random.rand(*self.shape)
        with open_file(self.im_path, 'a') as f:
            f.create_dataset(self.im_key, data=self.data)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_data(self, dataset_folder, im_name):
        self.assertTrue(os.path.exists(dataset_folder))
        exp_data = self.data

        # check the seg data
        seg_path = os.path.join(dataset_folder, 'images', 'local', f'{im_name}.n5')
        self.assertTrue(os.path.exists(seg_path))
        key = get_key(False, 0, 0, 0)
        with open_file(seg_path, 'r') as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))

        # check the image dict
        im_dict_path = os.path.join(dataset_folder, 'images', 'images.json')
        with open(im_dict_path) as f:
            im_dict = json.load(f)
        self.assertIn(im_name, im_dict)

    def test_add_image_data(self):
        from mobie import add_image_data
        dataset_folder = os.path.join(self.root, self.dataset_name)
        im_name = 'extra-im'

        tmp_folder = os.path.join(self.test_folder, 'tmp-im')

        scales = [[2, 2, 2]]
        add_image_data(self.im_path, self.im_key,
                       self.root, self.dataset_name, im_name,
                       resolution=(1, 1, 1), scale_factors=scales,
                       chunks=(64, 64, 64), tmp_folder=tmp_folder)
        self.check_data(dataset_folder, im_name)

    def test_cli(self):
        im_name = 'extra-im'

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        tmp_folder = os.path.join(self.test_folder, 'tmp-im')

        cmd = ['add_image_data', self.im_path, self.im_key,
               self.root, self.dataset_name, im_name,
               resolution, scales, chunks,
               '--tmp_folder', tmp_folder]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_data(dataset_folder, im_name)


if __name__ == '__main__':
    unittest.main()
