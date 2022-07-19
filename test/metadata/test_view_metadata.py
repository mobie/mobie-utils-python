import multiprocessing
import os
import unittest
from shutil import rmtree

import numpy as np
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestViewMetadata(unittest.TestCase):
    root = "./data"
    ds_folder = "./data/ds"

    def tearDown(self):
        if os.path.exists(self.root):
            rmtree(self.root)

    def test_image_view(self):
        from mobie.metadata import get_default_view

        # test the default view
        view = get_default_view("image", "my-image")
        validate_with_schema(view, "view")

        # test custom image settings
        custom_kwargs = [
            {"contrastLimits": [0., 255.], "color": "white"},
            {"contrastLimits": [0., 2000.], "color": "red"},
            {"contrastLimits": [-10., 20000000.]},
            {"showImagesIn3d": True},
            {"showImagesIn3d": True, "resolution3dView": [10., 10., 12.]},
            {"blendingMode": "sumOccluding"},
        ]
        for kwargs in custom_kwargs:
            view = get_default_view("image", "my-image", **kwargs)
            validate_with_schema(view, "view")

        # test missing fields
        view = get_default_view("image", "my-image")
        view["sourceDisplays"][0]["imageDisplay"].pop("color")
        with self.assertRaises(ValidationError):
            validate_with_schema(view, "view")

        # test invalid fields
        view = get_default_view("image", "my-image")
        view["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, "view")

        view = get_default_view("image", "my-image")
        view["sourceDisplays"][0]["imageDisplay"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, "view")

        # test invalid values
        invalid_kwargs = [
            {"uiSelectionGroup": "abc a"},
            {"uiSelectionGroup": "abc/a"},
            {"uiSelectionGroup": "abc;"},
            {"color": "foobar"},
            {"color": "r=1,g=2,b=3,a=4,z=5"},
            {"contrastLimits": [1., 2., 3.]},
            {"showImagesIn3d": "foobar"},
            {"resolution3dView": [1., 2., 3., 4.]},
            {"blendingMode": "summe"}
        ]
        for kwargs in invalid_kwargs:
            if "uiSelectionGroup" in kwargs:
                menu_name = kwargs.pop("uiSelectionGroup")
            view = get_default_view("image", "my-image", menu_name=menu_name, **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

    def test_segmentation_view(self):
        from mobie.metadata import get_default_view

        # test the default view
        view = get_default_view("segmentation", "my-seg")
        validate_with_schema(view, "view")

        # test custom segmentation settings
        custom_kwargs = [
            {"opacity": 0.5, "lut": "glasbey"},
            {"opacity": 0.9, "lut": "viridis",
             "colorByColumn": "colname", "showSelectedSegmentsIn3d": True, "tables": ["a.csv", "b.tsv"],
             "valueLimits": [0, 2500]},
            {"selectedSegmentIds": ["my-seg;0;1", "my-seg;0;2", "my-seg;1;10"]},
            {"showAsBoundaries": True, "boundaryThickness": 12}
        ]
        for kwargs in custom_kwargs:
            view = get_default_view("segmentation", "my-seg", **kwargs)
            validate_with_schema(view, "view")

        # test missing fields
        view = get_default_view("segmentation", "my-seg")
        view["sourceDisplays"][0]["segmentationDisplay"].pop("opacity")
        with self.assertRaises(ValidationError):
            validate_with_schema(view, "view")

        # test invalid fields
        view = get_default_view("segmentation", "my-seg")
        view["sourceDisplays"][0]["segmentationDisplay"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, "view")

        # test invalid values
        invalid_kwargs = [
            {"opacity": 10},
            {"lut": "red"},
            {"lut": "foobar"},
            {"selectedSegmentIds": ["my-seg,0,2"]},
            {"selectedSegmentIds": ["my-seg/abc;0;2"]},
            {"selectedSegmentIds": ["my-segc;abba;2"]}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view("segmentation", "my-seg", **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

        # test view with source transformations
        trafos = [
            {"parameters": np.random.rand(12).tolist()},
            {"parameters": np.random.rand(12).tolist(), "timepoints": [0]},
            {"parameters": np.random.rand(12).tolist(), "timepoints": [1, 2, 3]}
        ]
        for trafo in trafos:
            view = get_default_view("image", "my-image", source_transform=trafo)
            validate_with_schema(view, "view")

        # test invalid values
        invalid_trafos = [
            {"parameters": np.random.rand(12).tolist(), "timepoints": [-1]},
            {"parameters": np.random.rand(12).tolist(), "timepoints": [-1, 2, 3]}
        ]
        for trafo in invalid_trafos:
            view = get_default_view("image", "my-image", source_transform=trafo)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

    def test_viewer_transform(self):
        from mobie.metadata import get_default_view

        trafos = [
            {"timepoint": 0},
            {"affine": np.random.rand(12).tolist()},
            {"affine": np.random.rand(12).tolist(), "timepoint": 0},
            {"normalizedAffine": np.random.rand(12).tolist()},
            {"normalizedAffine": np.random.rand(12).tolist(), "timepoint": 1},
            {"position": np.random.rand(3).tolist()},
            {"position": np.random.rand(3).tolist(), "timepoint": 2},
            {"normalVector": np.random.rand(3).tolist()},
        ]
        for trafo in trafos:
            view = get_default_view("image", "my-image", viewer_transform=trafo)
            validate_with_schema(view, "view")

        # test missing fields
        invalid_trafos = [
            {"foo": "bar"}
        ]
        for trafo in invalid_trafos:
            view = get_default_view("image", "my-image", viewer_transform=trafo)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

        # test invalid fields
        invalid_trafos = [
            {"timepoint": 0, "foo": "bar"},
            {"affine": np.random.rand(12).tolist(), "foo": "bar"},
            {"normalizedAffine": np.random.rand(12).tolist(), "x": "y"},
            {"position": np.random.rand(3).tolist(), "a": 3}
        ]
        for trafo in invalid_trafos:
            view = get_default_view("image", "my-image", viewer_transform=trafo)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

        # test invalid values
        invalid_trafos = [
            {"timepoint": -1},
            {"affine": np.random.rand(11).tolist()},
            {"normalizedAffine": np.random.rand(13).tolist()},
            {"position": np.random.rand(4).tolist()}
        ]
        for trafo in invalid_trafos:
            view = get_default_view("image", "my-image", viewer_transform=trafo)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, "view")

    def test_get_view(self):
        from mobie.metadata import get_view, get_image_display, get_segmentation_display

        # single image views
        # with get_image_display arguments
        display_args = {"contrastLimits": [0.0, 1.0], "opacity": 1.0}
        view = get_view(["my-view"], ["image"], [["my-image"]], [display_args],
                        is_exclusive=True, menu_name="bookmark")
        validate_with_schema(view, "view")

        # with get_image_display return value
        display = get_image_display("my-view", ["my-image"], **display_args)
        view = get_view(["my-view"], ["image"], [["my-image"]], [display],
                        is_exclusive=True, menu_name="bookmark")
        validate_with_schema(view, "view")

        # single segmentation view
        # with get_segmentation_display arguments
        display_args = {"lut": "glasbey", "opacity": 1.0}
        view = get_view(["my-view"], ["segmentation"], [["my-segmentation"]], [display_args],
                        is_exclusive=True, menu_name="bookmark")
        validate_with_schema(view, "view")

        # with get_image_display return value
        display = get_segmentation_display("my-view", ["my-segmentation"], **display_args)
        view = get_view(["my-view"], ["segmentation"], [["my-segmentation"]], [display],
                        is_exclusive=True, menu_name="bookmark")
        validate_with_schema(view, "view")

        # multi source view
        image_display = get_image_display("images", ["my-image1", "my-image2"], contrastLimits=[0.0, 2.4], opacity=1.0)
        seg_display = get_segmentation_display("segmentations", ["my-seg"], opacity=0.6, lut="glasbey")
        view = get_view(["images", "segmentations"], ["image", "segmentation"],
                        [["my-image1", "my-image2"], ["my-seg"]], [image_display, seg_display],
                        is_exclusive=True, menu_name="bookmark")
        validate_with_schema(view, "view")

    def test_source_transforms(self):
        from mobie.metadata import (get_affine_source_transform, get_crop_source_transform,
                                    get_image_display,
                                    get_merged_grid_source_transform, get_transformed_grid_source_transform,
                                    get_view)
        settings = {"contrastLimits": [0.0, 1.0], "opacity": 1.0}

        # affine trafo
        affine = get_affine_source_transform(["my-image"], np.random.rand(12),
                                             timepoints=[0, 1], source_names_after_transform=["my-transformed-image"])
        view = get_view(names=["image-view"],
                        source_types=["image"],
                        sources=[["my-transformed-image"]],
                        display_settings=[settings],
                        is_exclusive=True,
                        menu_name="bookmark",
                        source_transforms=[affine])
        validate_with_schema(view, "view")

        # crop trafo
        crop = get_crop_source_transform(["my-image"], np.random.rand(3), np.random.rand(3),
                                         timepoints=[0, 1], source_names_after_transform=["my-cropped-image"],
                                         center_at_origin=True, rectify=True)
        view = get_view(names=["image-view"],
                        source_types=["image"],
                        sources=[["my-cropped-image"]],
                        display_settings=[settings],
                        is_exclusive=True, menu_name="bookmark",
                        source_transforms=[crop])
        validate_with_schema(view, "view")

        # grid trafo
        grid = get_merged_grid_source_transform(["my-image1", "my-image2", "my-image3", "my-image4"], "merged-images")
        view = get_view(names=["image-grid"],
                        source_types=["image"],
                        sources=[["my-image1", "my-image2", "my-image3", "my-image4"]],
                        display_settings=[settings],
                        is_exclusive=True, menu_name="bookmark",
                        source_transforms=[grid])
        validate_with_schema(view, "view")

        # transform grid trafo from list
        grid = get_transformed_grid_source_transform([["my-image1", "my-image2"], ["my-image3", "my-image4"]],
                                                     positions=[[0, 0], [1, 1]],
                                                     center_at_origin=True)
        view = get_view(names=["image-grid"],
                        source_types=["image"],
                        sources=[["my-image1", "my-image2", "my-image3", "my-image4"]],
                        display_settings=[settings],
                        is_exclusive=True, menu_name="bookmark",
                        source_transforms=[grid])
        validate_with_schema(view, "view")

        # combined transformations and new display settings
        crop = get_crop_source_transform(["my-transformed-image"], np.random.rand(3), np.random.rand(3),
                                         timepoints=[0, 1], source_names_after_transform=["my-cropped-image"])
        settings = get_image_display("image-view", ["my-cropped-image"], **settings)
        view = get_view(names=["image-view"],
                        source_types=["image"],
                        sources=[["my-cropped-image"]],
                        display_settings=[settings],
                        is_exclusive=True,
                        menu_name="bookmark",
                        source_transforms=[affine, crop])
        validate_with_schema(view, "view")

    def init_ds(self):
        import h5py
        from mobie import add_image
        os.makedirs(self.root, exist_ok=True)
        path = os.path.join(self.root, "data.h5")
        with h5py.File(path, "w") as f:
            f.create_dataset("data", data=np.random.rand(2, 64, 64))

        sources = []
        for ii in range(4):
            im_name = f"image-{ii}"
            add_image(path, "data", self.root, "ds", im_name,
                      resolution=(1, 1, 1), scale_factors=[[2, 2, 2]], chunks=(1, 32, 32),
                      tmp_folder=os.path.join(self.root, f"tmp-{ii}"),
                      max_jobs=min(4, multiprocessing.cpu_count()))
            sources.append(im_name)
        return sources

    def test_transform_grid_view(self):
        from mobie.metadata import get_grid_view, get_affine_source_transform

        # we need an initial dataset for the grid view
        sources = self.init_ds()
        grid_sources = [[source] for source in sources]

        # only grid transform
        view = get_grid_view(self.ds_folder, "grid-view", grid_sources, use_transformed_grid=False, menu_name="grid")
        validate_with_schema(view, "view")

        view = get_grid_view(self.ds_folder, "grid-view", grid_sources, use_transformed_grid=True, menu_name="grid")
        validate_with_schema(view, "view")

        # additional transforms
        trafos = [
            get_affine_source_transform([source], np.random.rand(12)) for source in sources
        ]
        view = get_grid_view(self.ds_folder, "grid-view", grid_sources,
                             additional_source_transforms=trafos, menu_name="grid")
        validate_with_schema(view, "view")

        # additional transforms, changed source names and grid_sources
        trafos = [
            get_affine_source_transform([source], np.random.rand(12),
                                        source_names_after_transform=[f"transformed-{ii}"])
            for ii, source in enumerate(sources)
        ]
        transformed_sources = [[f"transformed-{ii}"] for ii in range(len(sources))]
        all_transformed_sources = [trafo for trafos in transformed_sources for trafo in trafos]

        view = get_grid_view(self.ds_folder, "grid-view", grid_sources,
                             additional_source_transforms=trafos,
                             grid_sources=transformed_sources,
                             use_transformed_grid=False,
                             menu_name="grid")
        # check that all source displays list the names in transformed sources
        for disp in view["sourceDisplays"]:
            disp_sources = disp[list(disp.keys())[0]]["sources"]
            if isinstance(disp_sources, dict):
                disp_sources = list(disp_sources.values())
                disp_sources = [sname for srcs in disp_sources for sname in srcs]
            self.assertTrue(all(sname in all_transformed_sources for sname in disp_sources))
        validate_with_schema(view, "view")

        view = get_grid_view(self.ds_folder, "grid-view", grid_sources,
                             additional_source_transforms=trafos,
                             grid_sources=transformed_sources,
                             use_transformed_grid=True,
                             menu_name="grid")
        # check that all source displays list the names in transformed sources
        for disp in view["sourceDisplays"]:
            disp_sources = disp[list(disp.keys())[0]]["sources"]
            if isinstance(disp_sources, dict):
                disp_sources = list(disp_sources.values())
                disp_sources = [sname for srcs in disp_sources for sname in srcs]
            self.assertTrue(all(sname in all_transformed_sources for sname in disp_sources))
        validate_with_schema(view, "view")


if __name__ == "__main__":
    unittest.main()
