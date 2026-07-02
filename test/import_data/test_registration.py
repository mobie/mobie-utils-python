import os
import unittest
from multiprocessing import cpu_count
from shutil import rmtree

import numpy as np
import z5py
from scipy.ndimage import affine_transform as scipy_affine

import mobie
import mobie.utils as mobie_utils
from elf.transformation import elastix_to_native
from mobie.import_data.utils import _open_data, get_scale_key


def _write_elastix_affine(path, shape, resolution, translation_mm):
    """Write a minimal elastix AffineTransform parameter file (identity rotation + translation).

    The elastix axis convention is xyz, so shape / spacing / translation are written reversed
    relative to the numpy (zyx) inputs.
    """
    spacing_mm = [r / 1e3 for r in resolution[::-1]]  # elastix spacing is in millimeter, xyz
    tz, ty, tx = translation_mm  # given in zyx, written xyz
    with open(path, "w") as f:
        f.write('(Transform "AffineTransform")\n')
        f.write('(NumberOfParameters 12)\n')
        f.write('(TransformParameters 1 0 0 0 1 0 0 0 1 %f %f %f)\n' % (tx, ty, tz))
        f.write('(Size %i %i %i)\n' % (shape[2], shape[1], shape[0]))
        f.write('(Spacing %f %f %f)\n' % tuple(spacing_mm))
        f.write('(ResampleInterpolator "FinalNearestNeighborInterpolator")\n')
        f.write('(ResultImagePixelType "short")\n')
        f.write('(InitialTransformParametersFileName "NoInitialTransform")\n')


class TestRegistration(unittest.TestCase):
    test_folder = "./test-folder"
    tmp_folder = "./test-folder/tmp"
    root = "./test-folder/project"
    ds_name = "ds"
    n_jobs = min(4, cpu_count())

    shape = (32, 48, 40)
    resolution = [1.0, 1.0, 1.0]
    chunks = (16, 16, 16)
    scale_factors = [[2, 2, 2]]
    translation_mm = (0.001, -0.002, 0.003)  # zyx, micrometer-scaled

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        rng = np.random.RandomState(42)
        self.data = rng.randint(0, 2000, size=self.shape).astype("uint16")
        self.in_path = os.path.join(self.test_folder, "input.n5")
        self.in_key = "volume"
        with z5py.File(self.in_path, "a") as f:
            f.create_dataset(self.in_key, data=self.data, chunks=self.chunks)
        self.trafo_file = os.path.join(self.test_folder, "TransformParameters.0.txt")
        _write_elastix_affine(self.trafo_file, self.shape, self.resolution, self.translation_mm)

    def tearDown(self):
        rmtree(self.test_folder)

    def _scipy_reference(self, order):
        matrix = elastix_to_native(self.trafo_file, resolution=self.resolution)
        ref = scipy_affine(self.data, matrix[:3, :3], offset=matrix[:3, 3], order=order,
                           output_shape=self.shape, mode="constant", cval=0)
        return ref.astype("uint16")

    def _run_affine(self, file_format, source_name, bounding_box=None):
        mobie.add_registered_source(
            input_path=self.in_path, input_key=self.in_key, transformation=self.trafo_file,
            root=self.root, dataset_name=self.ds_name, source_name=source_name,
            resolution=self.resolution, scale_factors=self.scale_factors, chunks=self.chunks,
            method="affine", file_format=file_format, shape=self.shape,
            source_type="segmentation", add_default_table=False,
            tmp_folder=self.tmp_folder, target="local", max_jobs=self.n_jobs,
            bounding_box=bounding_box,
        )
        data_path, _ = mobie_utils.get_internal_paths(
            os.path.join(self.root, self.ds_name), file_format, source_name
        )
        with _open_data(data_path, "r") as f:
            s0 = f[get_scale_key(file_format, 0)][:]
            s1 = f[get_scale_key(file_format, 1)][:]
        return s0, s1

    def _check_source_in_dataset(self, source_name):
        import json
        ds_json = os.path.join(self.root, self.ds_name, "dataset.json")
        self.assertTrue(os.path.exists(ds_json))
        with open(ds_json) as f:
            meta = json.load(f)
        self.assertIn(source_name, meta["sources"])

    def test_affine_ome_zarr(self):
        # segmentation -> nearest-neighbor (order 0), so the result matches scipy exactly.
        ref = self._scipy_reference(order=0)
        s0, s1 = self._run_affine("ome.zarr", "reg-ome")
        self.assertEqual(s0.shape, self.shape)
        self.assertTrue(np.array_equal(s0, ref))
        # the pyramid level was created and is downsampled.
        self.assertEqual(s1.shape, (16, 24, 20))
        self._check_source_in_dataset("reg-ome")

    def test_affine_bdv_n5(self):
        # exercises the get_scale_key / metadata_format fix for the bdv.n5 layout.
        ref = self._scipy_reference(order=0)
        s0, s1 = self._run_affine("bdv.n5", "reg-bdv")
        self.assertEqual(s0.shape, self.shape)
        self.assertTrue(np.array_equal(s0, ref))
        self.assertEqual(s1.shape, (16, 24, 20))
        self._check_source_in_dataset("reg-bdv")

    def test_affine_bounding_box(self):
        # restrict to the corner block; everything outside the box must stay zero.
        ref = self._scipy_reference(order=0)
        bb = [[0, 0, 0], [16, 16, 16]]
        s0, _ = self._run_affine("ome.zarr", "reg-bb", bounding_box=bb)
        self.assertTrue(np.array_equal(s0[:16, :16, :16], ref[:16, :16, :16]))
        # all blocks outside the box are untouched (zero).
        self.assertTrue(np.all(s0[16:] == 0))
        self.assertTrue(np.all(s0[:, 16:] == 0))
        self.assertTrue(np.all(s0[:, :, 16:] == 0))

    def test_write_transformix_output(self):
        # the transformix output copy (formerly cluster_tools CopyVolume) is now a bp.copy.
        import imageio
        from mobie.import_data.registration.apply_registration import write_transformix_output

        result = np.random.RandomState(0).randint(0, 1000, size=self.shape).astype("uint16")
        tif_path = os.path.join(self.test_folder, "transformix_result.tif")
        imageio.volwrite(tif_path, result)

        out_path = os.path.join(self.test_folder, "out.ome.zarr")
        write_transformix_output(tif_path, out_path, get_scale_key("ome.zarr", 0), self.chunks,
                                 self.tmp_folder, "local", self.n_jobs, file_format="ome.zarr")
        with _open_data(out_path, "r") as f:
            written = f[get_scale_key("ome.zarr", 0)][:]
        self.assertEqual(written.shape, self.shape)
        self.assertTrue(np.array_equal(written, result))

    def test_registration_transformix_runner_map(self):
        # the fiji subprocess is dispatched via bp.runner.map; patch it out and assert the
        # transform-file rewriting + dispatch run and produce the expected output for the file.
        import imageio
        from mobie.import_data.registration import registration_impl

        result = np.random.RandomState(1).randint(0, 1000, size=self.shape).astype("uint16")

        def fake_apply(input_path, output_path, transformation_file, fiji_executable,
                       elastix_directory, working_dir, n_threads, output_format):
            # the rewritten transformation file must have been produced upstream.
            assert os.path.exists(transformation_file)
            imageio.volwrite(output_path + "-ch0.tif", result)

        orig = registration_impl._apply_transformix_for_file
        registration_impl._apply_transformix_for_file = fake_apply
        try:
            input_tif = os.path.join(self.test_folder, "input.tif")
            imageio.volwrite(input_tif, self.data)
            output_base = os.path.join(self.tmp_folder, "output")
            os.makedirs(self.tmp_folder, exist_ok=True)
            registration_impl.registration_transformix(
                input_tif, output_base, self.trafo_file,
                fiji_executable="/does/not/exist/fiji", elastix_directory=self.test_folder,
                tmp_folder=self.tmp_folder, shape=self.shape, resolution=self.resolution,
                interpolation="nearest", output_format="tif", result_dtype="unsigned short",
                n_threads=1, target="local", max_jobs=self.n_jobs,
            )
        finally:
            registration_impl._apply_transformix_for_file = orig

        produced = os.path.abspath(output_base) + "-ch0.tif"
        self.assertTrue(os.path.exists(produced))
        self.assertTrue(np.array_equal(np.asarray(imageio.volread(produced)), result))

    def test_parse_outputpoints(self):
        # the parser reads OutputIndexFixed (xyz), reverses to zyx, and reshapes C-order onto the block.
        from mobie.import_data.registration.registration_impl import _parse_outputpoints

        block_shape = (2, 2, 2)
        lines, expected = [], np.zeros((3,) + block_shape, dtype="float64")
        idx = 0
        for z in range(2):
            for y in range(2):
                for x in range(2):
                    sx, sy, sz = x + 10, y + 20, z + 30  # arbitrary source location (xyz)
                    lines.append(f"Point {idx} ; InputIndex = [ {x} {y} {z} ] ; "
                                 f"OutputIndexFixed = [ {sx} {sy} {sz} ] ; Deformation = [ 0 0 0 ]")
                    expected[0, z, y, x] = sz  # numpy zyx: axis 0 == z
                    expected[1, z, y, x] = sy
                    expected[2, z, y, x] = sx
                    idx += 1
        path = os.path.join(self.test_folder, "outputpoints.txt")
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")

        coords = _parse_outputpoints(path, 3, block_shape)
        self.assertEqual(coords.shape, (3,) + block_shape)
        np.testing.assert_array_equal(coords, expected)

    def test_coordinate_end_to_end(self):
        # exercise the full coordinate path with the transformix CLI monkeypatched to a known shift:
        # output voxel (z,y,x) samples source (z-dz, y-dy, x-dx); out-of-bounds -> 0.
        import functools
        from mobie.import_data.registration import registration_impl

        delta = (1, -2, 0)  # zyx shift

        def fake_transformix(in_coord_file, coord_folder, transformation_file, elastix_directory, delta):
            with open(in_coord_file) as f:
                pts = f.read().splitlines()[2:]  # skip "index" + count
            dz, dy, dx = delta
            out_file = os.path.join(coord_folder, "outputpoints.txt")
            with open(out_file, "w") as f:
                for i, ln in enumerate(pts):
                    x, y, z = map(int, ln.split())
                    sx, sy, sz = x - dx, y - dy, z - dz
                    f.write(f"Point {i} ; InputIndex = [ {x} {y} {z} ] ; "
                            f"OutputIndexFixed = [ {sx} {sy} {sz} ]\n")
            return out_file

        orig = registration_impl._run_transformix_coordinates
        registration_impl._run_transformix_coordinates = functools.partial(fake_transformix, delta=delta)
        try:
            s0, s1 = self._run_coordinate("ome.zarr", "reg-coord")
        finally:
            registration_impl._run_transformix_coordinates = orig

        # expected: integer-shift gather with zero fill outside the source (order 0 -> exact).
        dz, dy, dx = delta
        expected = np.zeros_like(self.data)
        Z, Y, X = self.data.shape
        for z in range(Z):
            for y in range(Y):
                for x in range(X):
                    sz, sy, sx = z - dz, y - dy, x - dx
                    if 0 <= sz < Z and 0 <= sy < Y and 0 <= sx < X:
                        expected[z, y, x] = self.data[sz, sy, sx]

        self.assertEqual(s0.shape, self.shape)
        self.assertTrue(np.array_equal(s0, expected))
        self.assertEqual(s1.shape, (16, 24, 20))
        self._check_source_in_dataset("reg-coord")

    def _run_coordinate(self, file_format, source_name):
        mobie.add_registered_source(
            input_path=self.in_path, input_key=self.in_key, transformation=self.trafo_file,
            root=self.root, dataset_name=self.ds_name, source_name=source_name,
            resolution=self.resolution, scale_factors=self.scale_factors, chunks=self.chunks,
            method="coordinate", file_format=file_format, shape=self.shape,
            source_type="segmentation", add_default_table=False,
            elastix_directory=self.test_folder,  # unused (transformix CLI is monkeypatched)
            tmp_folder=self.tmp_folder, target="local", max_jobs=self.n_jobs,
        )
        data_path, _ = mobie_utils.get_internal_paths(
            os.path.join(self.root, self.ds_name), file_format, source_name
        )
        with _open_data(data_path, "r") as f:
            s0 = f[get_scale_key(file_format, 0)][:]
            s1 = f[get_scale_key(file_format, 1)][:]
        return s0, s1


if __name__ == "__main__":
    unittest.main()
