import json
import os
import unittest
from shutil import rmtree

import numpy as np
import pandas as pd

from elf.io import open_file
from pybdv.util import get_key
from mobie import initialize_dataset


class TestSegmentation(unittest.TestCase):
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

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_segmentation(self, dataset_folder, seg_name, exp_data):
        self.assertTrue(os.path.exists(dataset_folder))

        # check the seg data
        seg_path = os.path.join(dataset_folder, 'images', 'local', f'{seg_name}.n5')
        self.assertTrue(os.path.exists(seg_path))
        key = get_key(False, 0, 0, 0)
        with open_file(seg_path, 'r') as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))

        # check the image dict
        im_dict_path = os.path.join(dataset_folder, 'images', 'images.json')
        with open(im_dict_path) as f:
            im_dict = json.load(f)
        self.assertIn(seg_name, im_dict)

        # check the table
        table_path = os.path.join(dataset_folder, 'tables', seg_name, 'default.csv')
        self.assertTrue(os.path.exists(table_path))
        table = pd.read_csv(table_path, sep='\t')

        label_ids = table['label_id'].values
        exp_label_ids = np.unique(data)
        self.assertTrue(np.array_equal(label_ids, exp_label_ids))

    def test_add_segmentation(self):
        from mobie import add_segmentation
        dataset_folder = os.path.join(self.root, self.dataset_name)
        seg_name = 'seg'

        seg_path = os.path.join(self.test_folder, 'seg.h5')
        seg_key = 'seg'
        data = np.random.randint(0, 100, size=self.shape)
        with open_file(seg_path, 'a') as f:
            f.create_dataset(seg_key, data=data)

        tmp_folder = os.path.join(self.test_folder, 'tmp-seg')

        scales = [[2, 2, 2]]
        add_segmentation(seg_path, seg_key,
                         self.root, self.dataset_name, seg_name,
                         resolution=(1, 1, 1), scale_factors=scales,
                         chunks=(64, 64, 64), tmp_folder=tmp_folder)

        self.check_segmentation(dataset_folder, seg_name, data)


if __name__ == '__main__':
    unittest.main()
