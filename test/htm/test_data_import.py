import os
import unittest
from shutil import rmtree

import h5py
import imageio
import numpy as np
import mobie.metadata as metadata
from elf.io import open_file


class TestDataImport(unittest.TestCase):
    test_folder = "./test_data"
    root = "./test_data/data"
    ds_name = "ds"
    shape = (32, 32)
    n_images = 4

    def setUp(self):
        os.makedirs(self.test_folder)
        self.images = [np.random.randint(0, 255, size=self.shape).astype("uint8")
                       for _ in range(self.n_images)]
        self.segs = [np.random.randint(0, 1000, size=self.shape).astype("uint32")
                     for _ in range(self.n_images)]

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def create_data(self, tif, is_seg=False):
        def write_tif(path, data):
            path = path + ".tif"
            imageio.imwrite(path, data)
            return path

        def write_h5(path, data):
            path = path + ".h5"
            with h5py.File(path, "w") as f:
                f.create_dataset("data", data=data)
            return path

        write = write_tif if tif else write_h5
        paths = []
        name = "seg" if is_seg else "im"
        for imid in range(self.n_images):
            path = f"{self.test_folder}/{name}{imid}"
            data = self.segs[imid] if is_seg else self.images[imid]
            p = write(path, data)
            paths.append(p)
        return paths

    def check_data(self, source_names, is_seg):
        ds_folder = os.path.join(self.root, self.ds_name)
        ds_meta = metadata.read_dataset_metadata(ds_folder)

        source_type = "segmentation" if is_seg else "image"
        sources = ds_meta["sources"]
        expected_sources = self.segs if is_seg else self.images
        self.assertEqual(len(source_names), len(expected_sources))

        for im_id, name in enumerate(source_names):
            self.assertIn(name, sources)
            source = sources[name]

            data_path = source[source_type]["imageData"]["ome.zarr"]["relativePath"]
            data_path = os.path.join(ds_folder, data_path)
            self.assertTrue(os.path.exists(data_path))

            with open_file(data_path, "r") as f:
                data = f["s0"][:]
            expected = expected_sources[im_id]
            self.assertTrue(np.allclose(expected, data))

    def test_add_images_from_tif(self):
        from mobie.htm import add_images
        files = self.create_data(tif=True)
        image_names = [f"im{ii}" for ii in range(self.n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_images(files, self.root, self.ds_name, image_names,
                   resolution=(1., 1.), scale_factors=[[2, 2]],
                   chunks=(16, 16), file_format="ome.zarr",
                   tmp_folder=tmp_folder)
        self.check_data(image_names, is_seg=False)

    def test_add_images_from_h5(self):
        from mobie.htm import add_images
        files = self.create_data(tif=False, is_seg=False)
        image_names = [f"im{ii}" for ii in range(self.n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_images(files, self.root, self.ds_name, image_names,
                   resolution=(1., 1.), scale_factors=[[2, 2]],
                   chunks=(16, 16), file_format="ome.zarr",
                   tmp_folder=tmp_folder, key="data")
        self.check_data(image_names, is_seg=False)

    def test_add_segmentation(self):
        from mobie.htm import add_segmentations
        files = self.create_data(tif=False, is_seg=True)
        seg_names = [f"seg{ii}" for ii in range(self.n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_segmentations(files, self.root, self.ds_name, seg_names,
                          resolution=(1., 1.), scale_factors=[[2, 2]],
                          chunks=(16, 16), file_format="ome.zarr",
                          tmp_folder=tmp_folder, key="data",
                          add_default_tables=True)
        self.check_data(seg_names, is_seg=True)


if __name__ == '__main__':
    unittest.main()
