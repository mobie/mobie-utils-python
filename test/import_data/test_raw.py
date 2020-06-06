import multiprocessing
import os
import unittest
from shutil import rmtree

import imageio
import numpy as np
from elf.io import open_file
from pybdv.util import get_key
from pybdv.downsample import sample_shape

try:
    import mrcfile
except ImportError:
    mrcfile = None


class TestImportRaw(unittest.TestCase):
    test_folder = './test-folder'
    tmp_folder = './test-folder/tmp'
    out_path = './test-folder/imported-data.n5'
    n_jobs = multiprocessing.cpu_count()

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        rmtree(self.test_folder)

    def check_data(self, exp_data, scales):
        key = get_key(False, 0, 0, 0)
        with open_file(self.out_path, 'r') as f:
            data = f[key][:]
        self.assertEqual(data.shape, exp_data.shape)
        self.assertTrue(np.allclose(data, exp_data))

        exp_shape = data.shape
        for scale, scale_facor in enumerate(scales, 1):
            key = get_key(False, 0, 0, scale)
            with open_file(self.out_path, 'r') as f:
                self.assertIn(key, f)
                this_shape = f[key].shape
            exp_shape = sample_shape(exp_shape, scale_facor)
            self.assertEqual(this_shape, exp_shape)

    def test_import_tif(self):
        from mobie.import_data import import_raw_volume
        shape = (32, 128, 128)
        data = np.random.rand(*shape)

        im_folder = os.path.join(self.test_folder, 'im-stack')
        os.makedirs(im_folder, exist_ok=True)
        for z in range(shape[0]):
            path = os.path.join(im_folder, 'z_%03i.tif' % z)
            imageio.imsave(path, data[z])

        scales = [[1, 2, 2], [1, 2, 2], [2, 2, 2]]
        import_raw_volume(im_folder, '*.tif', self.out_path,
                          resolution=(0.25, 1, 1), chunks=(16, 64, 64),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target='local', max_jobs=self.n_jobs)

        self.check_data(data, scales)

    def test_import_hdf5(self):
        from mobie.import_data import import_raw_volume
        shape = (128, 128, 128)
        data = np.random.rand(*shape)

        test_path_h5 = os.path.join(self.test_folder, 'data-h5.h5')
        key = 'data'
        with open_file(test_path_h5) as f:
            f.create_dataset(key, data=data)

        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        import_raw_volume(test_path_h5, key, self.out_path,
                          resolution=(1, 1, 1), chunks=(64, 64, 64),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target='local', max_jobs=self.n_jobs)

        self.check_data(data, scales)

    # TODO
    @unittest.skipIf(mrcfile is None, "Need mrcfile")
    def test_import_mrc(self):
        pass


if __name__ == '__main__':
    unittest.main()
