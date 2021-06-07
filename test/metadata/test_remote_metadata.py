import os
import unittest
import xml.etree.ElementTree as ET
from shutil import rmtree

import numpy as np

from elf.io import open_file
from mobie import add_image
from mobie.metadata import read_dataset_metadata, read_project_metadata
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

    def get_path_in_bucket(self, xml):
        tree = ET.parse(xml)
        root = tree.getroot()
        img = root.find('SequenceDescription').find('S3ImageLoader')
        return img.find('Key').text

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
            xml = source[source_type]["imageData"]["relativePath"]
            xml_path = os.path.join(dataset_folder, xml)
            self.assertTrue(os.path.exists(xml_path))

        proj_metadata = read_project_metadata(self.root)
        validate_with_schema(proj_metadata, "project")
        s3_root = proj_metadata["s3Root"]
        self.assertEqual(len(s3_root), 1)
        s3_root = s3_root[0]
        self.assertEqual(s3_root["endpoint"], service_endpoint)
        self.assertEqual(s3_root["bucket"], bucket_name)


if __name__ == '__main__':
    unittest.main()
