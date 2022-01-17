import os
import unittest
from shutil import rmtree
from subprocess import run
from sys import platform

import numpy as np

from elf.io import open_file
from mobie import add_image
from mobie.metadata import read_dataset_metadata, read_project_metadata
from mobie.xml_utils import parse_s3_xml
from mobie.validation.utils import validate_with_schema


class TestRemoteMetadata(unittest.TestCase):
    test_folder = "./test-folder"
    root = "./test-folder/data"
    shape = (64, 64, 64)
    dataset_name = "test"

    def init_dataset(self, file_format):
        data_path = os.path.join(self.test_folder, "data.h5")
        data_key = "data"
        with open_file(data_path, "a") as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        tmp_folder = os.path.join(self.test_folder, "tmp-init")

        raw_name = "test-raw"
        scales = [[2, 2, 2]]
        add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                  tmp_folder=tmp_folder, file_format=file_format)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def _check_remote_metadata(self, file_format, service_endpoint, bucket_name):
        dataset_folder = os.path.join(self.root, self.dataset_name)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        validate_with_schema(dataset_metadata, "dataset")

        new_file_format = file_format + ".s3"

        sources = dataset_metadata["sources"]
        for name, source in sources.items():
            source_type = list(source.keys())[0]
            storage = source[source_type]["imageData"]
            self.assertIn(new_file_format, storage)
            if new_file_format.startswith("bdv"):
                xml = storage[new_file_format]["relativePath"]
                xml_path = os.path.join(dataset_folder, xml)
                self.assertTrue(os.path.exists(xml_path))
                _, ep, bn, _ = parse_s3_xml(xml_path)
                self.assertEqual(ep, service_endpoint)
                self.assertEqual(bn, bucket_name)
            else:
                address = storage[new_file_format]["s3Address"]
                self.assertTrue(address.startswith(service_endpoint))

        proj_metadata = read_project_metadata(self.root)
        validate_with_schema(proj_metadata, "project")
        file_formats = proj_metadata["imageDataFormats"]
        self.assertIn(file_format, file_formats)
        self.assertIn(new_file_format, file_formats)

    def _test_remote_metadata(self, file_format):
        from mobie.metadata import add_remote_project_metadata
        self.init_dataset(file_format)
        bucket_name = "my-bucket"
        service_endpoint = "https://s3.embl.de"
        add_remote_project_metadata(self.root, bucket_name, service_endpoint)
        self._check_remote_metadata(file_format, service_endpoint, bucket_name)

    def test_remote_metadata_bdv_n5(self):
        self._test_remote_metadata("bdv.n5")

    def test_remote_metadata_bdv_ome_zarr(self):
        self._test_remote_metadata("bdv.ome.zarr")

    def test_remote_metadata_ome_zarr(self):
        self._test_remote_metadata("ome.zarr")

    @unittest.skipIf(platform == "win32", "CLI does not work on windows")
    def test_cli(self):
        file_format = "bdv.n5"
        self.init_dataset(file_format)
        bucket_name = "my-bucket"
        service_endpoint = "https://s3.embl.de"
        ret = run(["mobie.add_remote_metadata", "-i", self.root, "-b", bucket_name, "-s", service_endpoint])
        self.assertTrue(ret.returncode == 0)
        self._check_remote_metadata(file_format, service_endpoint, bucket_name)


if __name__ == "__main__":
    unittest.main()
