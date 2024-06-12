import os
import unittest
from glob import glob
from shutil import rmtree

import h5py
import numpy as np
import pandas as pd
from mobie import initialize_dataset, add_segmentation
from mobie.metadata import load_image_dict


# TODO use better test data that has at least some overlap between segmentation and ground-truth
class TestMetricsTable(unittest.TestCase):
    project_folder = './data'
    dataset_folder = './data/test'
    gt_name = 'gt'
    seg_name = 'seg'
    n_segments = 32

    def setUp(self):
        os.makedirs(self.project_folder, exist_ok=True)
        shape = (256, 256, 256)
        chunks = (64, 64, 64)
        tmp_data = './data/data.h5'
        with h5py.File(tmp_data, 'w') as f:
            f.create_dataset('raw', data=np.random.rand(*shape), chunks=chunks)
            f.create_dataset('gt', data=np.random.randint(0, 25, size=shape).astype('uint64'), chunks=chunks)
            f.create_dataset('seg', data=np.random.randint(0, self.n_segments, size=shape).astype('uint64'),
                             chunks=chunks)

        initialize_dataset(tmp_data, 'raw', self.project_folder, 'test', 'raw',
                           resolution=(1, 1, 1), chunks=chunks, scale_factors=[[2, 2, 2]])
        add_segmentation(tmp_data, 'gt', self.project_folder, 'test', self.gt_name,
                         resolution=(1, 1, 1), chunks=chunks, scale_factors=[[2, 2, 2]])
        add_segmentation(tmp_data, 'seg', self.project_folder, 'test', self.seg_name,
                         resolution=(1, 1, 1), chunks=chunks, scale_factors=[[2, 2, 2]])

    def tearDown(self):
        rmtree(self.project_folder)
        tmp_folders = glob('tmp*')
        for tmp_folder in tmp_folders:
            rmtree(tmp_folder)

    def _load_table(self):
        image_folder = os.path.join(self.dataset_folder, 'images')
        image_dict = load_image_dict(os.path.join(image_folder, 'images.json'))

        seg_entry = image_dict[self.seg_name]
        table_dir = os.path.join(self.dataset_folder, seg_entry['tableFolder'])
        table_path = os.path.join(table_dir, 'metrics.csv')

        return pd.read_csv(table_path, sep='\t')

    def test_iou(self):
        from mobie.tables.metrics_table import compute_metrics_table
        compute_metrics_table(
            self.dataset_folder, self.seg_name, self.gt_name,
            metric='iou'
        )
        table = self._load_table()
        self.assertIn('iou-score', table.columns)
        scores = table['iou-score'].values
        self.assertTrue(0 <= np.min(scores) <= 1)
        self.assertTrue(0 <= np.max(scores) <= 1)

    def test_voi(self):
        from mobie.tables.metrics_table import compute_metrics_table
        compute_metrics_table(
            self.dataset_folder, self.seg_name, self.gt_name,
            metric='voi'
        )
        table = self._load_table()
        for score_name in ('voi-split-score', 'voi-merge-score'):
            self.assertIn(score_name, table.columns)
            scores = table[score_name].values
            self.assertTrue(0 <= np.min(scores))
            self.assertTrue(0 <= np.max(scores))


if __name__ == '__main__':
    unittest.main()
