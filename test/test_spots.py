import os
import subprocess
import unittest
from shutil import rmtree
from sys import platform

import mobie
import numpy as np
import pandas as pd

from elf.io import open_file


class TestSpots(unittest.TestCase):
    test_folder = "./test-folder"
    root = "./test-folder/data"
    shape = (8, 64, 64)
    resolution = (5.0, 0.75, 0.75)
    dataset_name = "test"
    image_source_name = "my-image"
    table_path = "./test-folder/spot_table.tsv"
    n_spots = 1357

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

        data_path, data_key = os.path.join(self.test_folder, "data.h5"), "data"
        data = np.random.rand(*self.shape)
        with open_file(data_path, "a") as f:
            f.create_dataset(data_key, data=data)

        scale_factors = [[1, 2, 2]]
        chunks = (8, 32, 32)
        mobie.add_image(data_path, data_key, self.root, self.dataset_name, self.image_source_name,
                        resolution=self.resolution, scale_factors=scale_factors, chunks=chunks,
                        unit="nanometer", tmp_folder=os.path.join(self.test_folder, "tmp_image"))

        gene_names = ["aaa", "bbb", "ccc", "xyz", "123", "456"]
        table = {
            "x": np.random.rand(self.n_spots) * self.shape[2] * self.resolution[2],
            "y": np.random.rand(self.n_spots) * self.shape[1] * self.resolution[1],
            "z": np.random.rand(self.n_spots) * self.shape[0] * self.resolution[0],
            "gene": np.random.choice(gene_names, size=self.n_spots, replace=True),
        }
        table = pd.DataFrame.from_dict(table)
        table.to_csv(self.table_path, sep="\t")

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_spots(self, dataset_folder, name, expected_unit, extra_tables=None):
        metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(name, metadata["sources"])
        spot_metadata = metadata["sources"][name]["spots"]

        self.assertEqual(spot_metadata["unit"], expected_unit)
        bb_min = spot_metadata["boundingBoxMin"][::-1]
        self.assertEqual(len(bb_min), 3)
        bb_max = spot_metadata["boundingBoxMax"][::-1]
        self.assertEqual(len(bb_max), 3)
        for i, (mi, ma) in enumerate(zip(bb_min, bb_max)):
            max_len = self.shape[i] * self.resolution[i]
            self.assertLess(mi, ma)
            self.assertGreaterEqual(mi, 0)
            self.assertLessEqual(ma, max_len)

        if extra_tables is not None:
            table_folder = os.path.join(dataset_folder, spot_metadata["tableData"]["tsv"]["relativePath"])
            for tab_name in extra_tables:
                self.assertTrue(os.path.exists(os.path.join(table_folder, tab_name)))

        # check the full dataset metadata
        mobie.validation.validate_dataset(
            dataset_folder,
            assert_true=self.assertTrue, assert_equal=self.assertEqual, assert_in=self.assertIn
        )

    def test_add_spots(self):
        from mobie import add_spots
        dataset_folder = os.path.join(self.root, self.dataset_name)
        name = "my-spots"
        add_spots(self.table_path, self.root, self.dataset_name, name)
        self.check_spots(dataset_folder, name, expected_unit="micrometer")

    def test_add_spots_with_arguments(self):
        from mobie import add_spots
        dataset_folder = os.path.join(self.root, self.dataset_name)
        name = "my-spots"
        bb_min = [0.0, 0.0, 0.0]
        bb_max = [sh * res for sh, res in zip(self.shape, self.resolution)]
        add_spots(self.table_path, self.root, self.dataset_name, name,
                  bounding_box_min=bb_min, bounding_box_max=bb_max,
                  unit="nanometer")
        self.check_spots(dataset_folder, name, expected_unit="nanometer")

    def test_add_spots_with_reference(self):
        from mobie import add_spots
        dataset_folder = os.path.join(self.root, self.dataset_name)
        name = "my-spots"
        add_spots(self.table_path, self.root, self.dataset_name, name,
                  reference_source=self.image_source_name)
        self.check_spots(dataset_folder, name, expected_unit="nanometer")

    def test_add_spots_with_extra_tables(self):
        from mobie import add_spots

        def get_extra_table(col_name):
            n_spots = np.random.randint(10, self.n_spots)
            spot_ids = np.random.choice(np.arange(1, self.n_spots + 1), replace=False, size=n_spots).astype("uint64")
            new_gene_names = ["fu", "bar", "baz"]
            table = {"spot_id": spot_ids, col_name: np.random.choice(new_gene_names, size=n_spots, replace=True)}
            return pd.DataFrame.from_dict(table)

        tab_names = ["extra-tab1.tsv", "extra-tab2.tsv", "extra-tab3.tsv"]
        extra_tables = {
            name: get_extra_table(f"col_name{i}") for i, name in enumerate(tab_names)
        }

        dataset_folder = os.path.join(self.root, self.dataset_name)
        name = "my-spots"
        add_spots(self.table_path, self.root, self.dataset_name, name, additional_tables=extra_tables)
        mobie.create_view(dataset_folder, "extra-table-view",
                          sources=[[name]], display_settings=[{"additionalTables": tab_names, "spotRadius": 42.0}])
        self.check_spots(dataset_folder, name, expected_unit="micrometer", extra_tables=extra_tables)

    @unittest.skipIf(platform == "win32", "CLI does not work on windows")
    def test_cli(self):
        name = "my-spots"
        cmd = ["mobie.add_spots",
               "--input_table", self.table_path,
               "--root", self.root,
               "--dataset_name", self.dataset_name,
               "--name", name]
        subprocess.run(cmd)
        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_spots(dataset_folder, name, expected_unit="micrometer")


if __name__ == "__main__":
    unittest.main()
