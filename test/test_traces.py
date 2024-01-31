import json
import os
import subprocess
import unittest
from shutil import rmtree
from sys import platform

import mobie
import numpy as np
import pandas as pd
import elf.skeleton.io as skio

from elf.io import open_file


class TestTraces(unittest.TestCase):
    test_folder = "./test-folder"
    root = "./test-folder/data"
    trace_folder = "./test-folder/traces"
    shape = (128, 128, 128)
    dataset_name = "test"
    n_traces = 5

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
                        tmp_folder=tmp_folder, file_format="bdv.n5")

    def generate_trace(self, trace_id):
        path = os.path.join(self.trace_folder, f"trace_{trace_id}.swc")
        n_nodes = np.random.randint(50, 200)
        nodes = np.concatenate([np.random.randint(0, sh, size=n_nodes)[:, None] for sh in self.shape],
                               axis=1)
        edges = np.random.randint(0, n_nodes, size=(n_nodes, 2))
        skio.write_swc(path, nodes, edges)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()
        os.makedirs(self.trace_folder, exist_ok=True)
        for trace_id in range(1, self.n_traces + 1):
            self.generate_trace(trace_id)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def check_traces(self, dataset_folder, trace_name):
        self.assertTrue(os.path.exists(dataset_folder))

        # check the segmentation metadata
        metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(trace_name, metadata["sources"])

        # check the table
        table_path = os.path.join(dataset_folder, "tables", trace_name, "default.tsv")
        self.assertTrue(os.path.exists(table_path))
        table = pd.read_csv(table_path, sep="\t")

        label_ids = table["label_id"].values
        exp_label_ids = np.arange(1, self.n_traces + 1)
        self.assertTrue(np.array_equal(label_ids, exp_label_ids))

        # check the full dataset metadata
        mobie.validation.validate_dataset(
            dataset_folder,
            assert_true=self.assertTrue, assert_equal=self.assertEqual, assert_in=self.assertIn
        )

    def test_add_traces(self):
        from mobie import add_traces
        dataset_folder = os.path.join(self.root, self.dataset_name)
        traces_name = "traces"

        scales = [[2, 2, 2]]
        add_traces(self.trace_folder,
                   self.root, self.dataset_name, traces_name,
                   reference_name="test-raw", reference_scale=0,
                   resolution=(1, 1, 1), scale_factors=scales,
                   chunks=(64, 64, 64))
        self.check_traces(dataset_folder, traces_name)

    @unittest.skipIf(platform == "win32", "CLI does not work on windows")
    def test_cli(self):
        traces_name = "traces"
        ref_name = "test-raw"

        resolution = json.dumps([1., 1., 1.])
        scales = json.dumps([[2, 2, 2]])
        chunks = json.dumps([64, 64, 64])

        cmd = ["mobie.add_traces",
               "--input_path", self.trace_folder,
               "--input_key", "",
               "--root", self.root,
               "--dataset_name", self.dataset_name,
               "--name", traces_name,
               "--reference_name", ref_name,
               "--resolution", resolution,
               "--scale_factors", scales,
               "--chunks", chunks]
        subprocess.run(cmd)

        dataset_folder = os.path.join(self.root, self.dataset_name)
        self.check_traces(dataset_folder, traces_name)


if __name__ == "__main__":
    unittest.main()
