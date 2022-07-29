import multiprocessing as mp
import os
import unittest
from shutil import rmtree

import mobie
import numpy as np
from elf.io import open_file


class TestValidateProject(unittest.TestCase):
    tmp_folder = "./tmp"
    data_folder = "./tmp/data"

    def setUp(self):
        os.makedirs(self.tmp_folder, exist_ok=True)
        data_path = os.path.join(self.tmp_folder, "data.h5")
        data_key = "data"
        shape = (64, 64, 64)
        with open_file(data_path, "a") as f:
            f.create_dataset(data_key, data=np.random.rand(*shape))

        scales = [[2, 2, 2]]
        max_jobs = min(4, mp.cpu_count())

        tmp_folder = os.path.join(self.tmp_folder, "tmp-init-raw")
        mobie.add_image(
            data_path, data_key, self.data_folder, "test-ds", "raw",
            resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

    def tearDown(self):
        try:
            rmtree(self.tmp_folder)
        except OSError:
            pass

    def test_validate_project(self):
        from mobie.validation import validate_project
        validate_project(self.data_folder)


if __name__ == "__main__":
    unittest.main()
