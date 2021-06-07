import os
import unittest
from shutil import rmtree

import numpy as np

from elf.io import open_file
from mobie import add_image
from mobie.metadata import read_dataset_metadata, read_project_metadata
from mobie.xml_utils import parse_s3_xml
from mobie.validation.utils import validate_with_schema


class TestRemoteMetadata(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    shape = (64, 64, 64)
    dataset_name = 'test'

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        with open_file(data_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        tmp_folder = os.path.join(self.test_folder, 'tmp-init')

        raw_name = 'test-raw'
        scales = [[2, 2, 2]]
        add_image(data_path, data_key, self.root, self.dataset_name, raw_name,
                  resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                  tmp_folder=tmp_folder)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_remote_metadata(self):
        from mobie.metadata import add_remote_project_metadata

        bucket_name = "my-bucket"
        service_endpoint = "https://s3.embl.de"
        add_remote_project_metadata(self.root, bucket_name, service_endpoint)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        validate_with_schema(dataset_metadata, "dataset")

        sources = dataset_metadata['sources']
        for name, source in sources.items():
            source_type = list(source.keys())[0]
            storage = source[source_type]["imageData"]
            self.assertIn("bdv.n5.s3", storage)
            xml = storage["bdv.n5.s3"]["relativePath"]
            xml_path = os.path.join(dataset_folder, xml)
            self.assertTrue(os.path.exists(xml_path))
            _, ep, bn, _ = parse_s3_xml(xml_path)
            self.assertEqual(ep, service_endpoint)
            self.assertEqual(bn, bucket_name)

        proj_metadata = read_project_metadata(self.root)
        validate_with_schema(proj_metadata, "project")
        file_formats = proj_metadata["imageDataFormats"]
        self.assertIn('bdv.n5', file_formats)
        self.assertIn('bdv.n5.s3', file_formats)


if __name__ == '__main__':
    unittest.main()
