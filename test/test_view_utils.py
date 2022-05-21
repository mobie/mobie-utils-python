import json
import os
import multiprocessing as mp
import unittest
from shutil import rmtree

import h5py
import mobie
import numpy as np


class TestViewUtils(unittest.TestCase):
    root = "./data"
    ds_name = "ds"
    n_views = 3
    tmp_root = "./tmp"

    def setUp(self):
        os.makedirs(self.root)
        in_key = "data"
        resolution = (1.0, 1.0, 1.0)
        scale_factors = [[2, 2, 2]]
        chunks = (32, 32, 32)
        max_jobs = min(4, mp.cpu_count())
        for view_id in range(self.n_views):
            view_name = f"view-{view_id}"
            in_path = f"{self.root}/data-{view_id}.h5"
            with h5py.File(in_path, "w") as f:
                f.create_dataset(in_key, data=np.random.rand(64, 64, 64))
            tmp_folder = os.path.join(self.tmp_root, f"view-{view_id}")
            mobie.add_image(
                in_path, in_key, self.root, self.ds_name,
                view_name, resolution, scale_factors, chunks,
                tmp_folder=tmp_folder, max_jobs=max_jobs
            )

    def tearDown(self):
        try:
            rmtree(self.root)
        except OSError:
            pass
        try:
            rmtree(self.tmp_root)
        except OSError:
            pass

    def test_combine_views(self):
        ds_folder = os.path.join(self.root, self.ds_name)
        view_names = [f"view-{view_id}" for view_id in range(self.n_views)]
        new_view_name = "combined"
        menu_name = "combined"
        mobie.combine_views(ds_folder, view_names, new_view_name, menu_name)
        metadata = mobie.metadata.read_dataset_metadata(ds_folder)
        self.assertIn(new_view_name, metadata["views"])
        for vname in view_names:
            self.assertIn(vname, metadata["views"])

    def test_combine_views_dont_keep(self):
        ds_folder = os.path.join(self.root, self.ds_name)
        view_names = [f"view-{view_id}" for view_id in range(self.n_views)]
        new_view_name = "combined"
        menu_name = "combined"
        mobie.combine_views(ds_folder, view_names, new_view_name, menu_name, keep_original_views=False)
        metadata = mobie.metadata.read_dataset_metadata(ds_folder)
        self.assertIn(new_view_name, metadata["views"])
        for vname in view_names:
            self.assertNotIn(vname, metadata["views"])

    def test_merge_view_file(self):
        ds_folder = os.path.join(self.root, self.ds_name)

        view_name = "new-view"
        view = {
            "isExclusive": True, "uiSelectionGroup": "test-views", "viewerTransform": {"position": [0.1, 1.1, 2.3]}
        }
        views = {view_name: view}
        view_file = os.path.join(self.tmp_root, "views.json")
        with open(view_file, "w") as f:
            json.dump({"views": views}, f)

        mobie.merge_view_file(ds_folder, view_file)
        metadata = mobie.metadata.read_dataset_metadata(ds_folder)
        self.assertIn(view_name, metadata["views"])


if __name__ == "__main__":
    unittest.main()
