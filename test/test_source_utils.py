import os
import multiprocessing as mp
import unittest
from shutil import rmtree

import mobie
import numpy as np
from elf.io import open_file
from mobie.validation import validate_dataset


class TestSourceUtils(unittest.TestCase):
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

        tmp_folder = os.path.join(self.test_folder, "tmp-init-seg")
        mobie.add_segmentation(
            seg_path, data_key, self.root, self.dataset_name, self.seg_name,
            resolution=(1, 1, 1), chunks=self.chunks, scale_factors=scales,
            tmp_folder=tmp_folder, max_jobs=max_jobs
        )

        display_settings = [
            mobie.metadata.get_image_display("image-group-0", [self.raw_name]),
            mobie.metadata.get_segmentation_display("segmentation-group-1", [self.seg_name]),
        ]
        source_transforms = [
            mobie.metadata.get_affine_source_transform([self.raw_name, self.seg_name], np.random.rand(12))
        ]
        mobie.create_view(
            os.path.join(self.root, self.dataset_name), "my-view", [[self.raw_name], [self.seg_name]],
            display_settings=display_settings, source_transforms=source_transforms
        )

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def _test_rename(self, old_name, new_name):
        from mobie import rename_source
        ds_folder = os.path.join(self.root, self.dataset_name)
        rename_source(ds_folder, old_name, new_name)
        ds_meta = mobie.metadata.read_dataset_metadata(ds_folder)

        self.assertIn(new_name, ds_meta["sources"])
        self.assertNotIn(old_name, ds_meta["sources"])

        views = ds_meta["views"]
        self.assertIn(new_name, views)
        self.assertNotIn(old_name, views)

        view_sources = next(iter(views[new_name]["sourceDisplays"][0].values()))["sources"]
        self.assertIn(new_name, view_sources)
        self.assertNotIn(old_name, view_sources)

        combined_view = views["my-view"]
        trafo_sources = next(iter(combined_view["sourceTransforms"][0].values()))["sources"]
        self.assertIn(new_name, trafo_sources)
        self.assertNotIn(old_name, trafo_sources)

        disp_id = 0 if old_name == self.raw_name else 1
        view_sources = next(iter(combined_view["sourceDisplays"][disp_id].values()))["sources"]
        self.assertIn(new_name, view_sources)
        self.assertNotIn(old_name, view_sources)
        validate_dataset(ds_folder, assert_true=self.assertTrue, assert_in=self.assertIn, assert_equal=self.assertEqual)

    def test_rename_image_source(self):
        self._test_rename(self.raw_name, "new-raw-data")

    def test_rename_segmentation_source(self):
        self._test_rename(self.seg_name, "new-seg-data")

    def _test_remove(self, name):
        from mobie import remove_source
        ds_folder = os.path.join(self.root, self.dataset_name)
        remove_source(ds_folder, name)

        ds_meta = mobie.metadata.read_dataset_metadata(ds_folder)
        self.assertNotIn(name, ds_meta["sources"])

        views = ds_meta["views"]
        self.assertNotIn(name, views)

        combined_view = views["my-view"]
        self.assertEqual(len(combined_view["sourceDisplays"]), 1)
        self.assertNotIn(
            name, combined_view["sourceTransforms"][0]["affine"]["sources"]
        )
        # name == raw_name will remove the default view, which is required for a valid MoBIE project
        if name != self.raw_name:
            validate_dataset(
                ds_folder, assert_true=self.assertTrue, assert_in=self.assertIn, assert_equal=self.assertEqual
            )

    def test_remove_image_source(self):
        self._test_remove(self.raw_name)

    def test_remove_segmentation_source(self):
        self._test_remove(self.seg_name)


if __name__ == "__main__":
    unittest.main()
