import json
import os
import subprocess
import unittest
from shutil import rmtree

import numpy as np
import pandas as pd

from elf.io import open_file
from mobie import initialize_dataset


# TODO need to write nmx traces for a proper test
class TestTraces(unittest.TestCase):
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

    def check_traces(self, dataset_folder, trace_name):
        self.assertTrue(os.path.exists(dataset_folder))
        data = self.data

        # check the trace data
        # TODO

        # check the image dict
        im_dict_path = os.path.join(dataset_folder, 'images', 'images.json')
        with open(im_dict_path) as f:
            im_dict = json.load(f)
        self.assertIn(trace_name, im_dict)

        # check the table
        table_path = os.path.join(dataset_folder, 'tables', trace_name, 'default.csv')
        self.assertTrue(os.path.exists(table_path))
        table = pd.read_csv(table_path, sep='\t')

        label_ids = table['label_id'].values
        exp_label_ids = np.unique(data)
        self.assertTrue(np.array_equal(label_ids, exp_label_ids))

    def test_add_traces(self):
        from mobie import add_traces
        dataset_folder = os.path.join(self.root, self.dataset_name)
        traces_name = 'traces'

        scales = [[2, 2, 2]]
        add_traces(self.traces_path, self.traces_key,
                   self.root, self.dataset_name, traces_name,
                   resolution=(1, 1, 1), scale_factors=scales,
                   chunks=(64, 64, 64))
        self.check_traces(dataset_folder, traces_name)

    def test_cli(self):
        traces_name = 'traces'

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        cmd = ['add_traces', self.traces_path, self.traces_key,
               self.root, self.dataset_name, traces_name,
               resolution, scales, chunks]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_traces(dataset_folder, traces_name)


if __name__ == '__main__':
    unittest.main()
