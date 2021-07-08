import json
import multiprocessing
import os
import subprocess
import unittest
from shutil import rmtree

import imageio
import numpy as np
import h5py

from elf.io import open_file
from pybdv.metadata import get_data_path
from pybdv.util import get_key
from mobie import add_image
from mobie.validation import validate_project, validate_source_metadata
from mobie.metadata import read_dataset_metadata


class TestImageData(unittest.TestCase):
    test_folder = './test-folder'
    tmp_folder = './test-folder/tmp'
    root = './test-folder/data'
    dataset_name = 'test'
    shape = (128, 128, 128)
    max_jobs = min(8, multiprocessing.cpu_count())

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.im_path = os.path.join(self.test_folder, 'im.h5')
        self.im_key = 'im'
        self.data = np.random.rand(*self.shape)
        with open_file(self.im_path, 'a') as f:
            f.create_dataset(self.im_key, data=self.data)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    #
    # tests with dataset initialization
    #

    def make_tif_data(self, im_folder, shape):
        os.makedirs(im_folder, exist_ok=True)
        for z in range(shape[0]):
            path = os.path.join(im_folder, 'z_%03i.tif' % z)
            imageio.imsave(path, np.random.rand(*shape[1:]))

    def test_init_from_tif(self):
        shape = (32, 128, 128)

        im_folder = os.path.join(self.test_folder, 'im-stack')
        self.make_tif_data(im_folder, shape)

        dataset_name = 'test'
        raw_name = 'test-raw'
        scales = [[1, 2, 2], [1, 2, 2], [2, 2, 2]]
        add_image(im_folder, '*.tif', self.root, dataset_name, raw_name,
                  resolution=(0.25, 1, 1), chunks=(16, 64, 64),
                  scale_factors=scales, tmp_folder=self.tmp_folder,
                  target='local', max_jobs=self.max_jobs)

        self.check_dataset(os.path.join(self.root, dataset_name), shape, raw_name)

    def make_hdf5_data(self, path, key, shape):
        with h5py.File(path, 'a') as f:
            f.create_dataset(key, data=np.random.rand(*shape))

    def init_h5_dataset(self, dataset_name, raw_name, shape, file_format='bdv.n5'):

        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        self.make_hdf5_data(data_path, data_key, shape)

        n_jobs = 1 if file_format == 'bdv.hdf5' else self.max_jobs
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        add_image(data_path, data_key, self.root, dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(32, 32, 32),
                  scale_factors=scales,
                  tmp_folder=self.tmp_folder,
                  file_format=file_format,
                  target='local', max_jobs=n_jobs)

    def test_init_from_hdf5(self):
        dataset_name = 'test'
        raw_name = 'test-raw'
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape)
        self.check_dataset(os.path.join(self.root, dataset_name), shape, raw_name)

    #
    # tests with different output data formats
    #

    def test_bdv_hdf5(self):
        dataset_name = 'test'
        raw_name = 'test-raw'
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, file_format='bdv.hdf5')

    def test_bdv_ome_zarr(self):
        dataset_name = 'test'
        raw_name = 'test-raw'
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, file_format='bdv.ome.zarr')

    def test_ome_zarr(self):
        dataset_name = 'test'
        raw_name = 'test-raw'
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, file_format='ome.zarr')

    #
    # tests with existing dataset
    #

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
                  tmp_folder=tmp_folder, target='local', max_jobs=self.max_jobs)

    def test_add_image_with_dataset(self):
        self.init_dataset()
        dataset_folder = os.path.join(self.root, self.dataset_name)
        im_name = 'extra-im'

        tmp_folder = os.path.join(self.test_folder, 'tmp-im')

        scales = [[2, 2, 2]]
        add_image(self.im_path, self.im_key,
                  self.root, self.dataset_name, im_name,
                  resolution=(1, 1, 1), scale_factors=scales,
                  chunks=(64, 64, 64), tmp_folder=tmp_folder,
                  target='local', max_jobs=self.max_jobs)
        self.check_data(dataset_folder, im_name)

    def test_cli(self):
        im_name = 'extra-im'

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        tmp_folder = os.path.join(self.test_folder, 'tmp-im')

        cmd = ['mobie.add_image',
               '--input_path', self.im_path,
               '--input_key', self.im_key,
               '--root', self.root,
               '--dataset_name', self.dataset_name,
               '--name', im_name,
               '--resolution', resolution,
               '--scale_factors', scales,
               '--chunks', chunks,
               '--tmp_folder', tmp_folder]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_data(dataset_folder, im_name)

    #
    # data validation
    #

    def check_dataset(self, dataset_folder, exp_shape, raw_name, file_format='bdv.n5'):
        # validate the full project
        validate_project(self.root, self.assertTrue, self.assertIn, self.assertEqual)

        # check the raw data
        folder_name = file_format.replace('.', '-')
        if file_format.startswith('bdv'):
            xml_path = os.path.join(dataset_folder, 'images', folder_name, f'{raw_name}.xml')
            raw_path = get_data_path(xml_path, return_absolute_path=True)
            is_h5 = file_format == 'bdv.hdf5'
            key = get_key(is_h5, 0, 0, 0)
        else:
            self.assertEqual(file_format, 'ome.zarr')
            raw_path = os.path.join(dataset_folder, 'images', folder_name, f'{raw_name}.ome.zarr')
            key = 's0'

        with open_file(raw_path, 'r') as f:
            data = f[key][:]
            shape = data.shape
        self.assertEqual(shape, exp_shape)
        self.assertFalse(np.allclose(data, 0.))

    def check_data(self, dataset_folder, name):
        exp_data = self.data

        # check the image metadata
        metadata = read_dataset_metadata(dataset_folder)
        sources = metadata['sources']
        self.assertIn(name, sources)
        validate_source_metadata(name, sources[name], dataset_folder)

        # check the image data
        im_path = os.path.join(dataset_folder, 'images', 'bdv-n5', f'{name}.n5')
        self.assertTrue(os.path.exists(im_path))
        key = get_key(False, 0, 0, 0)
        with open_file(im_path, 'r') as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))


if __name__ == '__main__':
    unittest.main()
