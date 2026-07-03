import glob
import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file


class TestValidateData(unittest.TestCase):
    test_folder = "./test-folder"
    tmp_folder = "./test-folder/tmp"

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def _write_image(self, out_path, ome_zarr_version="0.4", shards=None):
        from mobie.import_data import import_image_data
        data = np.random.randint(0, 2000, size=(64, 96, 96)).astype("uint16")
        in_path = os.path.join(self.test_folder, "in.h5")
        with open_file(in_path, "a") as f:
            f.create_dataset("data", data=data)
        import_image_data(in_path, "data", out_path,
                          resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=[[2, 2, 2]],
                          tmp_folder=self.tmp_folder, target="local", max_jobs=1,
                          ome_zarr_version=ome_zarr_version, shards=shards)

    def test_validate_clean_v04(self):
        from mobie.validation.data import validate_local_dataset
        out_path = os.path.join(self.test_folder, "v04.ome.zarr")
        self._write_image(out_path, ome_zarr_version="0.4")
        self.assertEqual(validate_local_dataset(out_path, "s0", n_threads=2), [])

    def test_validate_clean_v05_sharded(self):
        from mobie.validation.data import validate_local_dataset
        out_path = os.path.join(self.test_folder, "v05.ome.zarr")
        self._write_image(out_path, ome_zarr_version="0.5", shards=(64, 64, 64))
        self.assertEqual(validate_local_dataset(out_path, "s0", n_threads=2), [])

    def test_detect_corruption_v05(self):
        from mobie.validation.data import validate_local_dataset
        out_path = os.path.join(self.test_folder, "v05.ome.zarr")
        self._write_image(out_path, ome_zarr_version="0.5", shards=(64, 64, 64))

        # corrupt one on-disk chunk / shard object of s0
        chunk_files = [f for f in glob.glob(os.path.join(out_path, "s0", "c", "**", "*"), recursive=True)
                       if os.path.isfile(f)]
        self.assertGreater(len(chunk_files), 0)
        with open(chunk_files[0], "r+b") as f:
            f.seek(0)
            f.write(b"\x00\x00garbage-corruption-bytes")

        corrupted = validate_local_dataset(out_path, "s0", n_threads=1)
        self.assertGreater(len(corrupted), 0)


if __name__ == "__main__":
    unittest.main()
