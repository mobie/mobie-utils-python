import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file

from mobie import add_image
from mobie.validation import validate_project


class TestUtil(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    tmp_folder = './test-folder/tmp'
    dataset_name = 'test'

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        shape = (64,) * 3
        with open_file(data_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.rand(*shape))

        tmp_folder = os.path.join(self.test_folder, 'tmp-init')

        raw_name = 'test-raw'
        scales = [[2, 2, 2]]
        add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(32,)*3, scale_factors=scales,
                  tmp_folder=tmp_folder, file_format="bdv.n5")

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_clone_dataset(self):
        from mobie.utils import clone_dataset
        ds2 = 'test-clone'
        clone_dataset(self.root, self.dataset_name, ds2)
        validate_project(
            self.root, assert_true=self.assertTrue, assert_in=self.assertIn, assert_equal=self.assertEqual
        )


class TestParseFileFormat(unittest.TestCase):
    def test_parse_file_format(self):
        from mobie.utils import parse_file_format
        # no suffix -> default version 0.4 for ome.zarr, None for other formats
        self.assertEqual(parse_file_format("ome.zarr"), ("ome.zarr", "0.4"))
        self.assertEqual(parse_file_format("ome.zarr.s3"), ("ome.zarr.s3", "0.4"))
        self.assertEqual(parse_file_format("bdv.n5"), ("bdv.n5", None))
        self.assertEqual(parse_file_format("bdv.hdf5"), ("bdv.hdf5", None))
        self.assertEqual(parse_file_format(None), (None, None))
        # explicit version suffix
        self.assertEqual(parse_file_format("ome.zarr@0.4"), ("ome.zarr", "0.4"))
        self.assertEqual(parse_file_format("ome.zarr@0.5"), ("ome.zarr", "0.5"))
        self.assertEqual(parse_file_format("ome.zarr.s3@0.5"), ("ome.zarr.s3", "0.5"))

    def test_parse_file_format_errors(self):
        from mobie.utils import parse_file_format
        # version suffix on a non-ome.zarr format
        with self.assertRaises(ValueError):
            parse_file_format("bdv.n5@0.5")
        # unknown version
        with self.assertRaises(ValueError):
            parse_file_format("ome.zarr@0.6")
        with self.assertRaises(ValueError):
            parse_file_format("ome.zarr@2")

    def test_check_shards(self):
        from mobie.utils import check_shards
        # allowed: ome.zarr v0.5
        check_shards((64, 64, 64), "ome.zarr", "0.5")
        # a None shards is always fine
        check_shards(None, "ome.zarr", "0.4")
        check_shards(None, "bdv.n5", None)
        # not allowed: v0.4, s3, or bdv
        with self.assertRaises(ValueError):
            check_shards((64, 64, 64), "ome.zarr", "0.4")
        with self.assertRaises(ValueError):
            check_shards((64, 64, 64), "ome.zarr.s3", "0.5")
        with self.assertRaises(ValueError):
            check_shards((64, 64, 64), "bdv.n5", None)


class TestCliArgs(unittest.TestCase):
    def _parse(self, extra):
        from mobie.utils import get_base_parser
        parser = get_base_parser("test")
        argv = ["--input_path", "x", "--input_key", "k", "--root", "r", "--dataset_name", "d",
                "--name", "n", "--resolution", "[1,1,1]", "--scale_factors", "[[2,2,2]]",
                "--chunks", "[32,32,32]"]
        return parser.parse_args(argv + extra)

    def test_source_kwargs_present(self):
        from mobie.utils import get_source_kwargs
        args = self._parse(["--file_format", "ome.zarr@0.5", "--shards", "[64,64,64]"])
        self.assertEqual(get_source_kwargs(args), {"file_format": "ome.zarr@0.5", "shards": [64, 64, 64]})

    def test_source_kwargs_absent(self):
        # when neither flag is given, no kwargs are forwarded, so the add_* defaults apply.
        from mobie.utils import get_source_kwargs
        self.assertEqual(get_source_kwargs(self._parse([])), {})


class TestNgffHelpers(unittest.TestCase):
    # exercises the shared v2/v3 layout-aware readers used by both the local and s3 read paths.
    def test_group_attrs_v2_v3(self):
        from mobie.validation.utils import load_ngff_group_attrs, ngff_version, ngff_multiscales
        v2 = {"multiscales": [{"name": "a", "version": "0.4", "datasets": [{"path": "s0"}]}]}
        v3 = {"attributes": {"ome": {"version": "0.5", "multiscales": [{"name": "a", "datasets": [{"path": "s0"}]}]}},
              "zarr_format": 3, "node_type": "group"}

        attrs = load_ngff_group_attrs(lambda n: v2 if n == ".zattrs" else None)
        self.assertEqual(ngff_version(attrs), "0.4")
        self.assertEqual(ngff_multiscales(attrs)[0]["name"], "a")

        attrs = load_ngff_group_attrs(lambda n: v3 if n == "zarr.json" else None)
        self.assertEqual(ngff_version(attrs), "0.5")
        self.assertEqual(ngff_multiscales(attrs)[0]["name"], "a")

        self.assertIsNone(load_ngff_group_attrs(lambda n: None))

    def test_array_shape_v2_v3(self):
        from mobie.validation.utils import load_ngff_array_shape
        self.assertEqual(load_ngff_array_shape(lambda n: {"shape": [10, 10]} if n == ".zarray" else None), [10, 10])
        self.assertEqual(load_ngff_array_shape(lambda n: {"shape": [5, 5, 5]} if n == "zarr.json" else None), [5, 5, 5])
        self.assertIsNone(load_ngff_array_shape(lambda n: None))


if __name__ == '__main__':
    unittest.main()
