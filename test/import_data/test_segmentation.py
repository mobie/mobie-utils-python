import multiprocessing
import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file
from pybdv.downsample import sample_shape

from mobie.import_data.utils import get_scale_key


class TestImportSegmentation(unittest.TestCase):
    test_folder = './test-folder'
    tmp_folder = './test-folder/tmp'
    out_path = './test-folder/imported-data.ome.zarr'
    n_jobs = multiprocessing.cpu_count()

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        rmtree(self.test_folder)

    def check_seg(self, exp_data, scales, out_path=None, file_format="ome.zarr"):
        out_path = self.out_path if out_path is None else out_path
        key = get_scale_key(file_format)
        with open_file(out_path, 'r') as f:
            ds = f[key]
            data = ds[:]
            max_id = ds.attrs['maxId']
        self.assertEqual(data.shape, exp_data.shape)
        self.assertTrue(np.array_equal(data, exp_data))
        self.assertEqual(int(max_id), int(exp_data.max()))

        exp_shape = data.shape
        for scale, scale_facor in enumerate(scales, 1):
            key = get_scale_key(file_format, scale)
            with open_file(out_path, 'r') as f:
                this_shape = f[key].shape
            exp_shape = sample_shape(exp_shape, scale_facor)
            self.assertEqual(this_shape, exp_shape)

    def test_import_segmentation(self):
        from mobie.import_data import import_segmentation
        shape = (64, 128, 128)
        data = np.random.randint(0, 100, size=shape, dtype='uint64')

        test_path = os.path.join(self.test_folder, 'data.h5')
        key = 'data'
        with open_file(test_path, mode="a") as f:
            f.create_dataset(key, data=data)

        scales = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_segmentation(test_path, key, self.out_path,
                            resolution=(0.5, 1, 1), chunks=(32, 64, 64),
                            scale_factors=scales, tmp_folder=self.tmp_folder,
                            target='local', max_jobs=self.n_jobs)

        self.check_seg(data, scales)

    def _write_fragments(self, shape=(64, 128, 128), n_ids=100):
        data = np.random.randint(0, n_ids, size=shape, dtype='uint64')
        test_path = os.path.join(self.test_folder, 'data.h5')
        with open_file(test_path, mode="a") as f:
            f.create_dataset('data', data=data)
        return test_path, 'data', data

    def test_import_from_node_labels_dense(self):
        from mobie.import_data import import_segmentation_from_node_labels
        n_ids = 100
        test_path, key, data = self._write_fragments(n_ids=n_ids)

        # dense 1d labeling: labeling[old_id] = new_id (background stays background)
        labeling = np.random.randint(0, 50, size=n_ids, dtype='uint64')
        labeling[0] = 0
        node_label_path = os.path.join(self.test_folder, 'node_labels.h5')
        with open_file(node_label_path, mode="a") as f:
            f.create_dataset('labels', data=labeling)

        scales = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_segmentation_from_node_labels(
            test_path, key, self.out_path, node_label_path, 'labels',
            resolution=(0.5, 1, 1), scale_factors=scales, chunks=(32, 64, 64),
            tmp_folder=self.tmp_folder, target='local', max_jobs=self.n_jobs,
        )

        exp_data = np.take(labeling, data)
        self.check_seg(exp_data, scales)

    def test_import_from_node_labels_table(self):
        from mobie.import_data import import_segmentation_from_node_labels
        n_ids = 100
        test_path, key, data = self._write_fragments(n_ids=n_ids)

        # 2d (N, 2) assignment table of (old_id, new_id) pairs, covering all ids present
        old_ids = np.unique(data)
        new_ids = np.random.randint(0, 50, size=old_ids.shape, dtype='uint64')
        new_ids[old_ids == 0] = 0  # keep background as background
        table = np.stack([old_ids, new_ids], axis=1)
        node_label_path = os.path.join(self.test_folder, 'node_labels.h5')
        with open_file(node_label_path, mode="a") as f:
            f.create_dataset('assignment', data=table)

        scales = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_segmentation_from_node_labels(
            test_path, key, self.out_path, node_label_path, 'assignment',
            resolution=(0.5, 1, 1), scale_factors=scales, chunks=(32, 64, 64),
            tmp_folder=self.tmp_folder, target='local', max_jobs=self.n_jobs,
        )

        lut = np.zeros(int(data.max()) + 1, dtype='uint64')
        lut[old_ids] = new_ids
        exp_data = lut[data]
        self.check_seg(exp_data, scales)

    def test_import_from_node_labels_bdv_n5(self):
        # exercises the file_format argument: node-label import must honor bdv.n5
        from mobie.import_data import import_segmentation_from_node_labels
        n_ids = 100
        test_path, key, data = self._write_fragments(n_ids=n_ids)

        labeling = np.random.randint(0, 50, size=n_ids, dtype='uint64')
        labeling[0] = 0
        node_label_path = os.path.join(self.test_folder, 'node_labels.h5')
        with open_file(node_label_path, mode="a") as f:
            f.create_dataset('labels', data=labeling)

        out_path = os.path.join(self.test_folder, 'imported-data.n5')
        scales = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_segmentation_from_node_labels(
            test_path, key, out_path, node_label_path, 'labels',
            resolution=(0.5, 1, 1), scale_factors=scales, chunks=(32, 64, 64),
            tmp_folder=self.tmp_folder, target='local', max_jobs=self.n_jobs,
            file_format='bdv.n5',
        )

        exp_data = np.take(labeling, data)
        self.check_seg(exp_data, scales, out_path=out_path, file_format='bdv.n5')


if __name__ == '__main__':
    unittest.main()
