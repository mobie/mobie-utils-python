import multiprocessing
import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file
from pybdv.downsample import sample_shape


class TestImportSegmentation(unittest.TestCase):
    test_folder = './test-folder'
    tmp_folder = './test-folder/tmp'
    out_path = './test-folder/imported-data.ome.zarr'
    n_jobs = multiprocessing.cpu_count()

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        rmtree(self.test_folder)

    def check_seg(self, exp_data, scales):
        key = "s0"
        with open_file(self.out_path, 'r') as f:
            ds = f[key]
            data = ds[:]
            max_id = ds.attrs['maxId']
        self.assertEqual(data.shape, exp_data.shape)
        self.assertTrue(np.array_equal(data, exp_data))
        self.assertAlmostEqual(max_id, data.max())

        exp_shape = data.shape
        for scale, scale_facor in enumerate(scales, 1):
            key = f"s{scale}"
            with open_file(self.out_path, 'r') as f:
                self.assertIn(key, f)
                this_shape = f[key].shape
            exp_shape = sample_shape(exp_shape, scale_facor)
            self.assertEqual(this_shape, exp_shape)

    def test_import_segmentation(self):
        from mobie.import_data import import_segmentation
        shape = (64, 128, 128)
        data = np.random.randint(0, 100, size=shape, dtype='uint64')

        test_path = os.path.join(self.test_folder, 'data.h5')
        key = 'data'
        with open_file(test_path) as f:
            f.create_dataset(key, data=data)

        scales = [[1, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_segmentation(test_path, key, self.out_path,
                            resolution=(0.5, 1, 1), chunks=(32, 64, 64),
                            scale_factors=scales, tmp_folder=self.tmp_folder,
                            target='local', max_jobs=self.n_jobs)

        self.check_seg(data, scales)


if __name__ == '__main__':
    unittest.main()
