import json
import os
import subprocess
import unittest
from shutil import rmtree

import numpy as np
import pandas as pd

from elf.io import open_file
from pybdv.util import get_key
from mobie import add_image
from mobie.validation import validate_source_metadata
from mobie.metadata import read_dataset_metadata


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
        add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(64, 64, 64), scale_factors=scales,
                  tmp_folder=tmp_folder)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

        self.seg_path = os.path.join(self.test_folder, 'seg.h5')
        self.seg_key = 'seg'
        self.data = np.random.randint(0, 100, size=self.shape)
        with open_file(self.seg_path, 'a') as f:
            f.create_dataset(self.seg_key, data=self.data)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_segmentation(self, dataset_folder, name):
        self.assertTrue(os.path.exists(dataset_folder))
        exp_data = self.data

        # check the segmentation metadata
        metadata = read_dataset_metadata(dataset_folder)
        self.assertIn(name, metadata['sources'])
        validate_source_metadata(name, metadata['sources'][name], dataset_folder)

        # check the segmentation data
        seg_path = os.path.join(dataset_folder, 'images', 'bdv.n5', f'{name}.n5')
        self.assertTrue(os.path.exists(seg_path))
        key = get_key(False, 0, 0, 0)
        with open_file(seg_path, 'r') as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))

        # check the table
        table_path = os.path.join(dataset_folder, 'tables', name, 'default.tsv')
        self.assertTrue(os.path.exists(table_path)), table_path
        table = pd.read_csv(table_path, sep='\t')

        label_ids = table['label_id'].values
        exp_label_ids = np.unique(data)
        if 0 in exp_label_ids:
            exp_label_ids = exp_label_ids[1:]
        self.assertTrue(np.array_equal(label_ids, exp_label_ids))

    def test_add_segmentation(self):
        from mobie import add_segmentation
        dataset_folder = os.path.join(self.root, self.dataset_name)
        seg_name = 'seg'

        tmp_folder = os.path.join(self.test_folder, 'tmp-seg')

        scales = [[2, 2, 2]]
        add_segmentation(self.seg_path, self.seg_key,
                         self.root, self.dataset_name, seg_name,
                         resolution=(1, 1, 1), scale_factors=scales,
                         chunks=(64, 64, 64), tmp_folder=tmp_folder)
        self.check_segmentation(dataset_folder, seg_name)

    def test_cli(self):
        seg_name = 'seg'

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        tmp_folder = os.path.join(self.test_folder, 'tmp-seg')

        cmd = ['mobie.add_segmentation',
               '--input_path', self.seg_path,
               '--input_key', self.seg_key,
               '--root', self.root,
               '--dataset_name', self.dataset_name,
               '--name', seg_name,
               '--resolution', resolution,
               '--scale_factors', scales,
               '--chunks', chunks,
               '--tmp_folder', tmp_folder]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_segmentation(dataset_folder, seg_name)


if __name__ == '__main__':
    unittest.main()
