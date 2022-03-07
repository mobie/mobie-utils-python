import multiprocessing
import os
import unittest
from shutil import rmtree

import numpy as np
import pybdv

from elf.io import open_file
from mobie import add_bdv_image
from mobie.metadata import read_dataset_metadata
from mobie.validation import validate_project


class TestBdvImporter(unittest.TestCase):
    test_folder = "./test-folder"
    tmp_folder = "./test-folder/tmp"
    root = "./test-folder/data"
    dataset_name = "test"
    shape = (128, 128, 128)
    max_jobs = min(8, multiprocessing.cpu_count())
    image_name = "image"

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.im_path = os.path.join(self.test_folder, "im.n5")
        self.xml_path = os.path.join(self.test_folder, "im.xml")
        self.data = np.random.rand(*self.shape)
        scale_factors = 2 * [[2, 2, 2]]
        pybdv.make_bdv(self.data, self.im_path,
                       downscale_factors=scale_factors,
                       resolution=[0.5, 0.5, 0.5], unit="micrometer",
                       setup_name=self.image_name)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_bdv_importer(self):
        add_bdv_image(self.xml_path, self.root, self.dataset_name, tmp_folder=self.tmp_folder)
        validate_project(self.root)
        meta = read_dataset_metadata(f"{self.root}/{self.dataset_name}")
        self.assertIn(self.image_name, meta["sources"])
        im_path = meta["sources"][self.image_name]["image"]["imageData"]["bdv.n5"]["relativePath"]
        im_path = os.path.join(self.root, self.dataset_name, im_path).replace("xml", "n5")
        self.assertTrue(os.path.exists(im_path))
        with open_file(im_path, "r") as f:
            data = f["setup0/timepoint0/s0"][:]
        self.assertTrue(np.allclose(data, self.data))


if __name__ == "__main__":
    unittest.main()
