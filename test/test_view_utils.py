import json
import os
import multiprocessing as mp
import unittest
from shutil import rmtree

import h5py
import mobie
import numpy as np

from elf.io import open_file
from mobie.metadata.utils import read_metadata


class TestViewCreation(unittest.TestCase):
    test_folder = "./test-folder"
    root = "./test-folder/data"
    dataset_name = "test"
    raw_name = "test-raw"
    extra_name = "extra-im"
    seg_name = "test-seg"
    extra_seg_name = "extra-seg"
    shape = (16, 32, 32)
    chunks = (8, 16, 16)

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, "data.h5")
        data_key = "data"
        with open_file(data_path, "a") as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        seg_path = os.path.join(self.test_folder, "seg.h5")
        with open_file(seg_path, "a") as f:
            f.create_dataset(data_key, data=np.random.randint(0, 100, size=self.shape))

        scales = [[2, 2, 2]]
        max_jobs = min(4, mp.cpu_count())

        tmp_folder = os.path.join(self.test_folder, "tmp-init-raw")
        mobie.add_image(
            data_path, data_key, self.root, self.dataset_name, self.raw_name,
            resolution=(1, 1, 1), chunks=self.chunks, scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

        tmp_folder = os.path.join(self.test_folder, "tmp-init-extra")
        mobie.add_image(
            data_path, data_key, self.root, self.dataset_name, self.extra_name,
            resolution=(1, 1, 1), chunks=self.chunks, scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

        tmp_folder = os.path.join(self.test_folder, "tmp-init-seg")
        mobie.add_segmentation(
            seg_path, data_key, self.root, self.dataset_name, self.seg_name,
            resolution=(1, 1, 1), chunks=self.chunks, scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

        tmp_folder = os.path.join(self.test_folder, "tmp-init-extra_seg")
        mobie.add_segmentation(
            seg_path, data_key, self.root, self.dataset_name, self.extra_seg_name,
            resolution=(1, 1, 1), chunks=self.chunks, scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_create_view(self):
        from mobie import create_view

        dataset_folder = os.path.join(self.root, self.dataset_name)
        bookmark_name = "my-bookmark"

        sources = [[self.raw_name], [self.seg_name]]
        display_settings = [
            {"color": "white", "contrastLimits": [0., 1000.]},
            {"opacity": 0.8, "lut": "viridis", "colorByColumn": "n_pixels", "valueLimits": [0, 1000]}
        ]

        create_view(dataset_folder, bookmark_name, sources, display_settings)
        dataset_metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

    def test_create_external_view(self):
        from mobie import create_view

        dataset_folder = os.path.join(self.root, self.dataset_name)
        bookmark_file_name = "more-bookmarks.json"
        bookmark_name = "my-bookmark"

        sources = [[self.raw_name], [self.seg_name]]
        display_settings = [
            {"color": "white", "contrastLimits": [0., 1000.]},
            {"opacity": 0.8, "lut": "viridis", "colorByColumn": "n_pixels", "valueLimits": [0, 1000]}
        ]

        view_file = os.path.join(dataset_folder, "misc", "views", bookmark_file_name)
        create_view(
            dataset_folder, bookmark_name, sources, display_settings, view_file=view_file)

        self.assertTrue(os.path.exists(view_file))
        bookmarks = read_metadata(view_file)["views"]
        self.assertIn(bookmark_name, bookmarks)

    def test_create_grid_view(self):
        from mobie import create_grid_view
        dataset_folder = os.path.join(self.root, self.dataset_name)

        # test vanilla grid bookmark
        bookmark_name = "simple-grid"
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        create_grid_view(dataset_folder, bookmark_name, sources)
        dataset_metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

        # test bookmark with positions
        bookmark_name = "grid-with-pos"
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        positions = [[0, 0], [1, 1]]
        create_grid_view(dataset_folder, bookmark_name, sources, positions=positions)
        dataset_metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

        # test bookmark with custom settings
        bookmark_name = "custom-setting-grid"
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        display_groups = {
            self.raw_name: "ims1",
            self.extra_name: "ims2",
            self.seg_name: "segs",
            self.extra_seg_name: "segs"
        }
        display_group_settings = {
            "ims1": {"color": "white", "opacity": 1.},
            "ims2": {"color": "green", "opacity": 0.75},
            "segs": {"lut": "glasbey", "opacity": 0.6}
        }
        create_grid_view(
            dataset_folder, bookmark_name, sources,
            display_groups=display_groups,
            display_group_settings=display_group_settings
        )
        dataset_metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])


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
