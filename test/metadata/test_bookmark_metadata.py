import os
import unittest
from shutil import rmtree

import numpy as np
from elf.io import open_file

from mobie import add_image, add_segmentation
from mobie.metadata import read_dataset_metadata
from mobie.metadata.utils import read_metadata


# TODO add tests for source and viewer transformations
class TestBookmarkMetadata(unittest.TestCase):
    test_folder = './test-folder'
    root = './test-folder/data'
    shape = (32, 64, 64)
    dataset_name = 'test'
    raw_name = 'test-raw'
    extra_name = 'extra-im'
    seg_name = 'test-seg'
    extra_seg_name = 'extra-seg'

    def init_dataset(self):
        data_path = os.path.join(self.test_folder, 'data.h5')
        data_key = 'data'
        with open_file(data_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.rand(*self.shape))

        seg_path = os.path.join(self.test_folder, 'seg.h5')
        with open_file(seg_path, 'a') as f:
            f.create_dataset(data_key, data=np.random.randint(0, 100, size=self.shape))

        scales = [[2, 2, 2]]

        tmp_folder = os.path.join(self.test_folder, 'tmp-init-raw')
        add_image(data_path, data_key, self.root, self.dataset_name, self.raw_name,
                  resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                  tmp_folder=tmp_folder)

        tmp_folder = os.path.join(self.test_folder, 'tmp-init-extra')
        add_image(data_path, data_key, self.root, self.dataset_name, self.extra_name,
                  resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                  tmp_folder=tmp_folder)

        tmp_folder = os.path.join(self.test_folder, 'tmp-init-seg')
        add_segmentation(seg_path, data_key, self.root, self.dataset_name, self.seg_name,
                         resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                         tmp_folder=tmp_folder)

        tmp_folder = os.path.join(self.test_folder, 'tmp-init-extra_seg')
        add_segmentation(seg_path, data_key, self.root, self.dataset_name, self.extra_seg_name,
                         resolution=(1, 1, 1), chunks=(32, 32, 32), scale_factors=scales,
                         tmp_folder=tmp_folder)

    def setUp(self):
        os.makedirs(self.test_folder, exist_ok=True)
        self.init_dataset()

    def tearDown(self):
        try:
            rmtree(self.test_folder)
        except OSError:
            pass

    def test_add_dataset_bookmark(self):
        from mobie.metadata import add_dataset_bookmark

        dataset_folder = os.path.join(self.root, self.dataset_name)
        bookmark_name = 'my-bookmark'

        sources = [[self.raw_name], [self.seg_name]]
        display_settings = [
            {"color": "white", "contrastLimits": [0., 1000.]},
            {"opacity": 0.8, "lut": "viridis", "colorByColumn": "n_pixels"}
        ]

        add_dataset_bookmark(dataset_folder, bookmark_name,
                             sources, display_settings)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

    def test_add_additional_bookmark(self):
        from mobie.metadata import add_additional_bookmark

        dataset_folder = os.path.join(self.root, self.dataset_name)
        bookmark_file_name = "more-bookmarks.json"
        bookmark_name = 'my-bookmark'

        sources = [[self.raw_name], [self.seg_name]]
        display_settings = [
            {"color": "white", "contrastLimits": [0., 1000.]},
            {"opacity": 0.8, "lut": "viridis", "colorByColumn": "n_pixels"}
        ]

        add_additional_bookmark(dataset_folder, bookmark_file_name, bookmark_name,
                                sources, display_settings)

        bookmark_file = os.path.join(dataset_folder, "misc", "views", bookmark_file_name)
        self.assertTrue(os.path.exists(bookmark_file))
        bookmarks = read_metadata(bookmark_file)["views"]
        self.assertIn(bookmark_name, bookmarks)

    def test_add_grid_bookmark(self):
        from mobie.metadata import add_grid_bookmark
        dataset_folder = os.path.join(self.root, self.dataset_name)

        # test vanilla grid bookmark
        bookmark_name = 'simple-grid'
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        add_grid_bookmark(dataset_folder, bookmark_name, sources)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

        # test bookmark with positions
        bookmark_name = 'grid-with-pos'
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        positions = [[0, 0], [1, 1]]
        add_grid_bookmark(dataset_folder, bookmark_name, sources,
                          positions=positions)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])

        # test bookmark with custom settings
        bookmark_name = 'custom-setting-grid'
        sources = [[self.raw_name, self.seg_name], [self.extra_name, self.extra_seg_name]]
        display_groups = {
            self.raw_name: 'ims1',
            self.extra_name: 'ims2',
            self.seg_name: 'segs',
            self.extra_seg_name: 'segs'
        }
        display_group_settings = {
            'ims1': {'color': 'white', 'opacity': 1.},
            'ims2': {'color': 'green', 'opacity': 0.75},
            'segs': {'lut': 'glasbey', 'opacity': 0.6}
        }
        add_grid_bookmark(dataset_folder, bookmark_name, sources,
                          display_groups=display_groups,
                          display_group_settings=display_group_settings)
        dataset_metadata = read_dataset_metadata(dataset_folder)
        self.assertIn(bookmark_name, dataset_metadata["views"])


if __name__ == '__main__':
    unittest.main()
