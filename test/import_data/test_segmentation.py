import multiprocessing
import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file
from pybdv.util import get_key


class TestImportSegmentation(unittest.TestCase):
    test_folder = './test-folder'
    tmp_folder = './test-folder/tmp'
    out_path = './test-folder/imported-data.n5'
    n_jobs = multiprocessing.cpu_count()

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        rmtree(self.test_folder)

    # TODO check scales
    def check_seg(self, exp_data, scales):
        key = get_key(False, 0, 0, 0)
        with open_file(self.out_path, 'r') as f:
            ds = f[key]
            data = ds[:]
            max_id = ds.attrs['maxId']
        self.assertEqual(data.shape, exp_data.shape)
        self.assertTrue(np.array_equal(data, exp_data))
        self.assertAlmostEqual(max_id, data.max())

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
