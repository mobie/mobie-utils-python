import os
import multiprocessing
import unittest
from shutil import rmtree

import h5py
import numpy as np
from mobie.htm import add_images
from mobie.metadata import read_dataset_metadata
from mobie.validation import validate_view_metadata


class TestGridViews(unittest.TestCase):
    test_folder = "./test_data"
    root = "./test_data/data"
    ds_name = "ds"

    def setUp(self):
        os.makedirs(self.test_folder)
        n_images = 4
        shape = (32, 32)

        files = []
        for ii in range(n_images):
            im_path = os.path.join(self.test_folder, f"im-{ii}.h5")
            with h5py.File(im_path, "w") as f:
                f.create_dataset("data", data=np.random.randint(0, 255, size=shape).astype("uint8"))
            files.append(im_path)

        max_jobs = min(4, multiprocessing.cpu_count())
        tmp_folder = os.path.join(self.test_folder, "tmp")
        image_names = ["aWell1-Im1", "aWell1-Im2", "aWell2-Im1", "aWell2-Im2"]
        add_images(files, self.root, self.ds_name, image_names,
                   resolution=(1., 1.), scale_factors=[[2, 2]],
                   chunks=(16, 16), file_format="ome.zarr",
                   tmp_folder=tmp_folder, key="data", max_jobs=max_jobs)

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def _test_plate_grid_view(self, get_view):
        from mobie.htm.grid_views import _get_default_site_table, _get_default_well_table

        ds_folder = os.path.join(self.root, self.ds_name)
        metadata = read_dataset_metadata(ds_folder)

        source_prefixes = ["a"]
        source_types = ["image"]
        source_settings = [{"color": "white"}]
        menu_name = "images"

        def to_site_name(source_name, prefix):
            return source_name[len(prefix):]

        def to_well_name(site_name):
            return site_name.split("-")[1]

        metadata, site_table = _get_default_site_table(ds_folder, metadata, source_prefixes,
                                                       to_site_name, to_well_name, None)
        metadata, well_table = _get_default_well_table(ds_folder, metadata, source_prefixes,
                                                       to_site_name, to_well_name, None)

        view = get_view(metadata, source_prefixes, source_types, source_settings,
                        menu_name, to_site_name, to_well_name,
                        site_table=site_table,
                        well_table=well_table)
        validate_view_metadata(view, dataset_folder=ds_folder, assert_true=self.assertTrue)

    def test_plate_merged_grid_view(self):
        from mobie.htm.grid_views import get_merged_plate_grid_view
        self._test_plate_grid_view(get_merged_plate_grid_view)

    def test_plate_transformed_grid_view(self):
        from mobie.htm.grid_views import get_transformed_plate_grid_view
        self._test_plate_grid_view(get_transformed_plate_grid_view)


if __name__ == '__main__':
    unittest.main()
