import json
import os
import subprocess
import unittest
from shutil import rmtree
from sys import platform

import mobie
import numpy as np
import pandas as pd

from elf.io import open_file
from pybdv.util import get_key


class TestSegmentation(unittest.TestCase):
    test_folder = "./test-folder"
    root = "./test-folder/data"
    shape = (128, 128, 128)
    dataset_name = "test"

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.seg_path = os.path.join(self.test_folder, "seg.n5")
        self.seg_key = "seg"
        self.data = np.random.randint(0, 100, size=self.shape)
        with open_file(self.seg_path, "a") as f:
            ds = f.create_dataset(self.seg_key, data=self.data)
            ds.attrs["maxId"] = int(self.data.max())

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_segmentation(self, dataset_folder, name):
        self.assertTrue(os.path.exists(dataset_folder))
        exp_data = self.data

        # check the segmentation metadata
        metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(name, metadata["sources"])

        # check the segmentation data
        seg_path = os.path.join(dataset_folder, "images", "bdv-n5", f"{name}.n5")
        self.assertTrue(os.path.exists(seg_path))
        key = get_key(False, 0, 0, 0)
        with open_file(seg_path, "r") as f:
            data = f[key][:]
        self.assertTrue(np.array_equal(data, exp_data))

        # check the table
        table_path = os.path.join(dataset_folder, "tables", name, "default.tsv")
        self.assertTrue(os.path.exists(table_path)), table_path
        table = pd.read_csv(table_path, sep="\t")

        label_ids = table["label_id"].values
        exp_label_ids = np.unique(data)
        if 0 in exp_label_ids:
            exp_label_ids = exp_label_ids[1:]
        self.assertTrue(np.array_equal(label_ids, exp_label_ids))

        # check the full dataset metadata
        mobie.validation.validate_dataset(
            dataset_folder,
            assert_true=self.assertTrue, assert_equal=self.assertEqual, assert_in=self.assertIn
        )

    def test_add_segmentation(self):
        from mobie import add_segmentation
        dataset_folder = os.path.join(self.root, self.dataset_name)
        seg_name = "seg"

        tmp_folder = os.path.join(self.test_folder, "tmp-seg")

        scales = [[2, 2, 2]]
        add_segmentation(self.seg_path, self.seg_key,
                         self.root, self.dataset_name, seg_name,
                         resolution=(1, 1, 1), scale_factors=scales,
                         chunks=(64, 64, 64), tmp_folder=tmp_folder)
        self.check_segmentation(dataset_folder, seg_name)

    def test_add_segmentation_with_initial_table(self):
        from mobie import add_segmentation
        from mobie.tables import compute_default_table

        dataset_folder = os.path.join(self.root, self.dataset_name)
        seg_name = "seg"

        tmp_folder = os.path.join(self.test_folder, "tmp-seg")

        table_path = os.path.join(tmp_folder, "table.tsv")
        compute_default_table(self.seg_path, self.seg_key, table_path,
                              resolution=(1, 1, 1), tmp_folder=os.path.join(self.test_folder, "tmp-table"),
                              target="local", max_jobs=1)

        scales = [[2, 2, 2]]
        add_segmentation(self.seg_path, self.seg_key,
                         self.root, self.dataset_name, seg_name,
                         resolution=(1, 1, 1), scale_factors=scales,
                         chunks=(64, 64, 64), tmp_folder=tmp_folder,
                         add_default_table=table_path)
        self.check_segmentation(dataset_folder, seg_name)

    def test_add_segmentation_with_wrong_initial_table(self):
        from mobie import add_segmentation

        seg_name = "seg"
        tmp_folder = os.path.join(self.test_folder, "tmp-seg")
        os.makedirs(tmp_folder, exist_ok=True)

        table_path = os.path.join(tmp_folder, "table.tsv")
        table = pd.DataFrame(np.random.rand(128, 4), columns=["a", "b", "c", "d"])
        table.to_csv(table_path, index=False, sep="\t")

        scales = [[2, 2, 2]]
        with self.assertRaises(ValueError):
            add_segmentation(self.seg_path, self.seg_key,
                             self.root, self.dataset_name, seg_name,
                             resolution=(1, 1, 1), scale_factors=scales,
                             chunks=(64, 64, 64), tmp_folder=tmp_folder,
                             add_default_table=table_path)

    @unittest.skipIf(platform == "win32", "CLI does not work on windows")
    def test_cli(self):
        seg_name = "seg"

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        tmp_folder = os.path.join(self.test_folder, "tmp-seg")

        cmd = ["mobie.add_segmentation",
               "--input_path", self.seg_path,
               "--input_key", self.seg_key,
               "--root", self.root,
               "--dataset_name", self.dataset_name,
               "--name", seg_name,
               "--resolution", resolution,
               "--scale_factors", scales,
               "--chunks", chunks,
               "--tmp_folder", tmp_folder]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_segmentation(dataset_folder, seg_name)


if __name__ == "__main__":
    unittest.main()
