import json
import multiprocessing
import os
import subprocess
import unittest
from shutil import rmtree
from sys import platform

import imageio
import mobie
import numpy as np
import h5py

from elf.io import open_file
from pybdv.metadata import get_data_path
from pybdv.util import get_key


class TestImageData(unittest.TestCase):
    test_folder = "./test-folder"
    tmp_folder = "./test-folder/tmp"
    root = "./test-folder/data"
    dataset_name = "test"
    shape = (128, 128, 128)
    max_jobs = min(8, multiprocessing.cpu_count())

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.im_path = os.path.join(self.test_folder, "im.h5")
        self.im_key = "im"
        self.data = np.random.rand(*self.shape)
        with open_file(self.im_path, "a") as f:
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
            path = os.path.join(im_folder, "z_%03i.tif" % z)
            imageio.imsave(path, np.random.rand(*shape[1:]))

    def test_init_from_tif(self):
        shape = (32, 128, 128)

        im_folder = os.path.join(self.test_folder, "im-stack")
        self.make_tif_data(im_folder, shape)

        dataset_name = "test"
        raw_name = "test-raw"
        scales = [[1, 2, 2], [1, 2, 2], [2, 2, 2]]
        mobie.add_image(im_folder, "*.tif", self.root, dataset_name, raw_name,
                        resolution=(0.25, 1, 1), chunks=(16, 64, 64),
                        scale_factors=scales, tmp_folder=self.tmp_folder,
                        target="local", max_jobs=self.max_jobs)

        self.check_dataset(os.path.join(self.root, dataset_name), shape, raw_name)

    def make_hdf5_data(self, path, key, shape, func=None):
        if func is None:
            data = np.random.rand(*shape)
        else:
            data = func(shape)
        with h5py.File(path, "a") as f:
            f.create_dataset(key, data=data)

    def init_h5_dataset(
        self, dataset_name, raw_name, shape, file_format="bdv.n5", func=None, int_to_uint=False
    ):

        data_path = os.path.join(self.test_folder, "data.h5")
        data_key = "data"
        self.make_hdf5_data(data_path, data_key, shape, func)

        n_jobs = 1 if file_format == "bdv.hdf5" else self.max_jobs
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        mobie.add_image(data_path, data_key, self.root, dataset_name, raw_name,
                        resolution=(1, 1, 1), chunks=(32, 32, 32),
                        scale_factors=scales,
                        tmp_folder=self.tmp_folder,
                        file_format=file_format,
                        target="local", max_jobs=n_jobs,
                        int_to_uint=int_to_uint)

    def test_init_from_hdf5(self, func=None, int_to_uint=False):
        dataset_name = "test"
        raw_name = "test-raw"
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, func=func, int_to_uint=False)
        self.check_dataset(os.path.join(self.root, dataset_name), shape, raw_name)

    #
    # tests with different data types
    #
    def _test_float(self, dtype):

        def float_data(shape):
            data = np.random.rand(*shape).astype(dtype)
            return data

        self.test_init_from_hdf5(float_data)
        ds_folder = os.path.join(self.root, self.dataset_name)
        mdata = mobie.metadata.read_dataset_metadata(ds_folder)
        clims = mdata["views"]["test-raw"]["sourceDisplays"][0]["imageDisplay"]["contrastLimits"]
        c0, c1 = clims
        self.assertEqual(c0, 0.0)
        self.assertEqual(c1, 1.0)

    def test_float32(self):
        self._test_float("float32")

    def test_float64(self):
        self._test_float("float64")

    def _test_int(self, dtype, int_to_uint=False):

        def int_data(shape):
            if dtype == "int8":
                min_, max_ = -127, 127
            elif dtype == "uint8":
                min_, max_ = 0, 255
            elif dtype == "int16":
                min_, max_ = -32000, 32000
            elif dtype == "uint16":
                min_, max_ = 0, 64000
            else:
                min_, max_ = 0, int(1e6)
            data = np.random.randint(min_, max_, size=shape, dtype=dtype)
            return data

        self.test_init_from_hdf5(int_data, int_to_uint=int_to_uint)
        ds_folder = os.path.join(self.root, self.dataset_name)
        mdata = mobie.metadata.read_dataset_metadata(ds_folder)
        clims = mdata["views"]["test-raw"]["sourceDisplays"][0]["imageDisplay"]["contrastLimits"]
        c0, c1 = clims
        self.assertEqual(c0, np.iinfo(dtype).min)
        self.assertEqual(c1, np.iinfo(dtype).max)

    def test_int8(self):
        self._test_int("int8")

    def test_uint8(self):
        self._test_int("uint8")

    def test_int16(self):
        self._test_int("int16")

    def test_uint16(self):
        self._test_int("uint16")

    def test_int8_int_to_uint(self):
        self._test_int("int8", int_to_uint=True)

    #
    # tests with different output data formats
    #

    def test_bdv_hdf5(self):
        dataset_name = "test"
        raw_name = "test-raw"
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, file_format="bdv.hdf5")

    def test_ome_zarr(self):
        dataset_name = "test"
        raw_name = "test-raw"
        shape = (64, 64, 64)
        self.init_h5_dataset(dataset_name, raw_name, shape, file_format="ome.zarr")

    #
    # tests with existing dataset
    #

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, "data.h5")
        data_key = "data"
        with open_file(data_path, "a") as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        tmp_folder = os.path.join(self.test_folder, "tmp-init")

        raw_name = "test-raw"
        scales = [[2, 2, 2]]
        mobie.add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                        resolution=(1, 1, 1), chunks=(64, 64, 64), scale_factors=scales,
                        tmp_folder=tmp_folder, target="local", max_jobs=self.max_jobs)

    def test_add_image_with_dataset(self):
        self.init_dataset()
        dataset_folder = os.path.join(self.root, self.dataset_name)
        im_name = "extra-im"

        tmp_folder = os.path.join(self.test_folder, "tmp-im")

        scales = [[2, 2, 2]]
        mobie.add_image(self.im_path, self.im_key,
                        self.root, self.dataset_name, im_name,
                        resolution=(1, 1, 1), scale_factors=scales,
                        chunks=(64, 64, 64), tmp_folder=tmp_folder,
                        target="local", max_jobs=self.max_jobs)
        self.check_data(dataset_folder, im_name)

    @unittest.skipIf(platform == "win32", "CLI does not work on windows")
    def test_cli(self):
        im_name = "extra-im"

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        tmp_folder = os.path.join(self.test_folder, "tmp-im")

        cmd = ["mobie.add_image",
               "--input_path", self.im_path,
               "--input_key", self.im_key,
               "--root", self.root,
               "--dataset_name", self.dataset_name,
               "--name", im_name,
               "--resolution", resolution,
               "--scale_factors", scales,
               "--chunks", chunks,
               "--tmp_folder", tmp_folder]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_data(dataset_folder, im_name)

    #
    # test with numpy data
    #

    def test_numpy(self):
        im_name = "test-data"
        scales = [[2, 2, 2]]
        mobie.add_image(self.data, None, self.root, self.dataset_name, im_name,
                        resolution=(1, 1, 1), scale_factors=scales,
                        chunks=(64, 64, 64), tmp_folder=self.tmp_folder,
                        target="local", max_jobs=self.max_jobs)
        self.check_data(os.path.join(self.root, self.dataset_name), im_name)

    def test_with_view(self):
        im_name = "test-data"
        scales = [[2, 2, 2]]

        clims = [0.1, 0.9]
        view = mobie.metadata.get_default_view("image", im_name, contrastLimits=clims)

        mobie.add_image(self.data, None, self.root, self.dataset_name, im_name,
                        resolution=(1, 1, 1), scale_factors=scales,
                        chunks=(64, 64, 64), tmp_folder=self.tmp_folder,
                        target="local", max_jobs=self.max_jobs, view=view)
        self.check_data(os.path.join(self.root, self.dataset_name), im_name)

        mdata = mobie.metadata.read_dataset_metadata(os.path.join(self.root, self.dataset_name))
        clims_read = mdata["views"][im_name]["sourceDisplays"][0]["imageDisplay"]["contrastLimits"]
        self.assertEqual(clims, clims_read)

    #
    # data validation
    #

    def check_dataset(self, dataset_folder, exp_shape, raw_name, file_format="bdv.n5"):
        # validate the full project
        mobie.validation.validate_project(
            self.root, assert_true=self.assertTrue, assert_in=self.assertIn, assert_equal=self.assertEqual
        )

        # check the raw data
        folder_name = file_format.replace(".", "-")
        if file_format.startswith("bdv"):
            xml_path = os.path.join(dataset_folder, "images", folder_name, f"{raw_name}.xml")
            raw_path = get_data_path(xml_path, return_absolute_path=True)
            is_h5 = file_format == "bdv.hdf5"
            key = get_key(is_h5, 0, 0, 0)
        else:
            self.assertEqual(file_format, "ome.zarr")
            raw_path = os.path.join(dataset_folder, "images", folder_name, f"{raw_name}.ome.zarr")
            key = "s0"

        with open_file(raw_path, "r") as f:
            data = f[key][:]
            shape = data.shape
        self.assertEqual(shape, exp_shape)
        self.assertFalse(np.allclose(data, 0.))

    def check_data(self, dataset_folder, name):
        exp_data = self.data

        # check the image metadata
        metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        sources = metadata["sources"]
        self.assertIn(name, sources)
        mobie.validation.validate_source_metadata(name, sources[name], dataset_folder)

        # check the image data
        im_path = os.path.join(dataset_folder, "images", "bdv-n5", f"{name}.n5")
        self.assertTrue(os.path.exists(im_path))
        key = get_key(False, 0, 0, 0)
        with open_file(im_path, "r") as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))


if __name__ == "__main__":
    unittest.main()
