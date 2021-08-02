import os
import unittest
from shutil import rmtree

import h5py
import imageio
import numpy as np
import mobie.metadata as metadata


class TestDataImport(unittest.TestCase):
    test_folder = "./test_data"
    root = "./test_data/data"
    ds_name = "ds"

    def setUp(self):
        os.makedirs(self.test_folder)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def create_data(self, n_images, tif, is_seg=False):
        shape = (32, 32)

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
        for imid in range(n_images):
            path = f"{self.test_folder}/{name}{imid}"
            if is_seg:
                data = np.random.randint(0, 1000, size=shape).astype("uint32")
            else:
                data = np.random.randint(0, 255, size=shape).astype("uint8")
            p = write(path, data)
            paths.append(p)
        return paths

    def check_data(self, source_names):
        ds_folder = os.path.join(self.root, self.ds_name)
        ds_meta = metadata.read_dataset_metadata(ds_folder)
        sources = ds_meta["sources"]
        for name in source_names:
            self.assertIn(name, sources)
            source = sources[name]
            data_path = source["imageData"]["ome.zarr"]["relativePath"]
            data_path = os.path.join(ds_folder, data_path)
            self.assertTrue(os.path.exists(data_path))

    def test_add_images_from_tif(self):
        from mobie.htm import add_images
        n_images = 4
        files = self.create_data(n_images, tif=True)
        image_names = [f"im{ii}" for ii in range(n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_images(files, self.root, self.ds_name, image_names,
                   resolution=(1., 1.), scale_factors=[[2, 2]],
                   chunks=(16, 16), file_format="ome.zarr",
                   tmp_folder=tmp_folder)

    def test_add_images_from_h5(self):
        from mobie.htm import add_images
        n_images = 4
        files = self.create_data(n_images, tif=False, is_seg=True)
        image_names = [f"im{ii}" for ii in range(n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_images(files, self.root, self.ds_name, image_names,
                   resolution=(1., 1.), scale_factors=[[2, 2]],
                   chunks=(16, 16), file_format="ome.zarr",
                   tmp_folder=tmp_folder, key="data")

    def test_add_segmentation(self):
        from mobie.htm import add_segmentations
        n_images = 4
        files = self.create_data(n_images, tif=False)
        seg_names = [f"seg{ii}" for ii in range(n_images)]
        tmp_folder = os.path.join(self.test_folder, "tmp")
        add_segmentations(files, self.root, self.ds_name, seg_names,
                          resolution=(1., 1.), scale_factors=[[2, 2]],
                          chunks=(16, 16), file_format="ome.zarr",
                          tmp_folder=tmp_folder, key="data",
                          add_default_tables=True)


if __name__ == '__main__':
    unittest.main()
