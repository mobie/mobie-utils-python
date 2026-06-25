import json
import os
import unittest
from multiprocessing import cpu_count
from shutil import rmtree

import bioimage_py as bp
import imageio
import numpy as np
from elf.io import open_file
from pybdv.util import get_key, relative_to_absolute_scale_factors
from pybdv.downsample import sample_shape

try:
    import mrcfile
except ImportError:
    mrcfile = None

try:
    import nibabel
except ImportError:
    nibabel = None


class TestImportImage(unittest.TestCase):
    test_folder = "./test-folder"
    tmp_folder = "./test-folder/tmp"
    out_path = "./test-folder/imported-data.ome.zarr"
    n_jobs = min(4, cpu_count())

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        rmtree(self.test_folder)

    def _check_data(self, exp_data, scale_data, scales):
        self.assertEqual(len(scale_data), len(scales) + 1)
        data = scale_data[0]
        self.assertEqual(data.shape, exp_data.shape)
        self.assertTrue(np.allclose(data, exp_data))

        exp_shape = data.shape
        for scale_factor, this_data in zip(scales, scale_data[1:]):
            this_shape = this_data.shape
            exp_shape = sample_shape(exp_shape, scale_factor)
            self.assertEqual(this_shape, exp_shape)

    def check_data(self, exp_data, scales, is_h5=False, out_path=None):
        out_path = self.out_path if out_path is None else out_path
        scale_data = []
        with open_file(out_path, "r") as f:
            for scale in range(len(scales) + 1):
                key = get_key(is_h5, 0, 0, scale)
                self.assertIn(key, f)
                scale_data.append(f[key][:])
        self._check_data(exp_data, scale_data, scales)

    def check_data_ome_zarr(self, exp_data, scales, out_path, resolution, scale_factors):
        out_path = self.out_path if out_path is None else out_path
        scale_data = []
        with open_file(out_path, "r") as f:

            metadata = f.attrs
            self.assertIn("multiscales", metadata)
            multiscales = metadata["multiscales"]
            self.assertEqual(len(multiscales), 1)
            multiscales = multiscales[0]

            self.assertIn("axes", multiscales)
            axes = multiscales["axes"]
            self.assertTrue(all(ax["unit"] == "micrometer" for ax in axes))

            datasets = multiscales["datasets"]
            scale_factors = [len(axes) * [1.]] + scale_factors
            scale_factors = relative_to_absolute_scale_factors(scale_factors)
            self.assertEqual(len(scale_factors), len(datasets))
            for ds, scale_factor in zip(datasets, scale_factors):
                scale = ds["coordinateTransformations"][0]
                self.assertEqual(scale["type"], "scale")
                scale = scale["scale"]
                expected_scale = [res * sf for res, sf in zip(resolution, scale_factor)]
                self.assertTrue(np.allclose(expected_scale, scale))

            for scale in range(len(scales) + 1):
                key = f"s{scale}"
                self.assertIn(key, f)

                attrs_path = os.path.join(out_path, key, ".zarray")
                with open(attrs_path, "r") as ff:
                    dimension_separator = json.load(ff).get("dimension_separator", ".")
                self.assertEqual(dimension_separator, "/")

                scale_data.append(f[key][:])
        self._check_data(exp_data, scale_data, scales)

    def create_h5_input_data(self, shape=3*(64,)):
        data = np.random.rand(*shape)
        test_path = os.path.join(self.test_folder, "data-h5.h5")
        key = "data"
        with open_file(test_path, mode="a") as f:
            f.create_dataset(key, data=data)
        return test_path, key, data

    #
    # test imports from different file formats (to default output format = ome.zarr)
    #

    def test_import_tif(self):
        from mobie.import_data import import_image_data
        shape = (32, 128, 128)
        data = np.random.rand(*shape)

        im_folder = os.path.join(self.test_folder, "im-stack")
        os.makedirs(im_folder, exist_ok=True)

        resolution = (0.25, 1, 1)

        for z in range(shape[0]):
            path = os.path.join(im_folder, "z_%03i.tif" % z)
            imageio.imwrite(path, data[z])

        scales = [[1, 2, 2], [1, 2, 2], [2, 2, 2]]
        import_image_data(im_folder, "*.tif", self.out_path,
                          resolution=resolution, chunks=(16, 64, 64),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs)

        self.check_data_ome_zarr(data, scales, self.out_path, resolution, scales)

    def test_import_hdf5(self):
        from mobie.import_data import import_image_data
        test_path, key, data = self.create_h5_input_data()
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        resolution = (1, 1, 1)
        import_image_data(test_path, key, self.out_path,
                          resolution=resolution, chunks=(32, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs)
        self.check_data_ome_zarr(data, scales, self.out_path, resolution, scales)

    @unittest.skipIf(mrcfile is None, "Need mrcfile")
    def test_import_mrc(self):
        from mobie.import_data import import_image_data
        # mrcfile does not support float64, so use uint8 here.
        shape = (32, 64, 64)
        data = (np.random.rand(*shape) * 255).astype("uint8")

        mrc_path = os.path.join(self.test_folder, "data.mrc")
        with mrcfile.new(mrc_path, overwrite=True) as mrc:
            mrc.set_data(data)

        scales = [[2, 2, 2], [2, 2, 2]]
        resolution = (0.5, 0.5, 0.5)
        import_image_data(mrc_path, None, self.out_path,
                          resolution=resolution, chunks=(16, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs)

        # the mrc reader applies an axis convention, so compare against what the importer reads.
        expected = bp.open_source(mrc_path, "data")[:]
        self.check_data_ome_zarr(expected, scales, self.out_path, resolution, scales)

    @unittest.skipIf(nibabel is None, "Need nibabel")
    def test_import_nifti(self):
        from mobie.import_data import import_image_data
        shape = (32, 64, 64)
        data = np.random.rand(*shape).astype("float32")

        nifti_path = os.path.join(self.test_folder, "data.nii.gz")
        nibabel.save(nibabel.Nifti1Image(data, np.eye(4)), nifti_path)

        scales = [[2, 2, 2], [2, 2, 2]]
        resolution = (0.5, 0.5, 0.5)
        import_image_data(nifti_path, None, self.out_path,
                          resolution=resolution, chunks=(16, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs)

        # the nifti reader transposes axes, so compare against what the importer reads.
        expected = bp.open_source(nifti_path)[:]
        self.check_data_ome_zarr(expected, scales, self.out_path, resolution, scales)

    #
    # test exports to different output formats
    #

    def test_import_bdv_hdf5(self):
        from mobie.import_data import import_image_data
        test_path, key, data = self.create_h5_input_data()
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        out_path = os.path.join(self.test_folder, "imported_data.h5")
        import_image_data(test_path, key, out_path,
                          resolution=(1, 1, 1), chunks=(32, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=1, file_format="bdv.hdf5")
        self.check_data(data, scales, is_h5=True, out_path=out_path)

    def test_import_bdv_n5(self):
        from mobie.import_data import import_image_data
        test_path, key, data = self.create_h5_input_data()
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        out_path = os.path.join(self.test_folder, "imported_data.n5")
        import_image_data(test_path, key, out_path,
                          resolution=(1, 1, 1), chunks=(32, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=1, file_format="bdv.n5")
        self.check_data(data, scales, is_h5=False, out_path=out_path)

    def test_import_2d_to_bdv(self):
        from mobie.import_data import import_image_data
        # a 2d input is promoted to a 3d (1, y, x) volume on the fly (via ExpandDimsSource) for the
        # bdv formats, which require 3d data. The scale factors must keep z=1 (sample_shape floors).
        data = np.random.rand(128, 128)
        test_path = os.path.join(self.test_folder, "data-2d.h5")
        key = "data"
        with open_file(test_path, mode="a") as f:
            f.create_dataset(key, data=data)

        scales = [[1, 2, 2], [1, 2, 2]]
        out_path = os.path.join(self.test_folder, "imported_data.n5")
        import_image_data(test_path, key, out_path,
                          resolution=(1, 1, 1), chunks=(1, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=1, file_format="bdv.n5")
        self.check_data(data[None], scales, is_h5=False, out_path=out_path)

    def test_import_ome_zarr(self):
        from mobie.import_data import import_image_data
        test_path, key, data = self.create_h5_input_data()
        scales = [[2, 2, 2], [2, 2, 2], [2, 2, 2]]
        resolution = (0.5, 0.5, 0.5)
        out_path = os.path.join(self.test_folder, "imported_data.ome.zarr")
        import_image_data(test_path, key, out_path,
                          resolution=resolution, chunks=(32, 32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs,
                          file_format="ome.zarr")
        self.check_data_ome_zarr(data, scales, out_path, resolution, scales)

    def test_import_ome_zarr_2d(self):
        from mobie.import_data import import_image_data
        test_path, key, data = self.create_h5_input_data(shape=(128, 128))
        scales = [[2, 2], [2, 2], [2, 2]]
        resolution = (0.5, 0.5)
        out_path = os.path.join(self.test_folder, "imported_data.ome.zarr")
        import_image_data(test_path, key, out_path,
                          resolution=resolution, chunks=(32, 32),
                          scale_factors=scales, tmp_folder=self.tmp_folder,
                          target="local", max_jobs=self.n_jobs,
                          file_format="ome.zarr")
        self.check_data_ome_zarr(data, scales, out_path, resolution, scales)


if __name__ == "__main__":
    unittest.main()
