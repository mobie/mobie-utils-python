import unittest
import numpy as np
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestViewMetadata(unittest.TestCase):
    def test_image_view(self):
        from mobie.metadata import get_default_view

        # test the default view
        view = get_default_view('image', 'my-image')
        validate_with_schema(view, 'view')

        # test custom image settings
        custom_kwargs = [
            {'contrastLimits': [0., 255.], 'color': 'white'},
            {'contrastLimits': [0., 2000.], 'color': 'red'},
            {'showImagesIn3d': True},
            {'showImagesIn3d': True, 'resolution3dView': [10., 10., 12.]}
        ]
        for kwargs in custom_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            validate_with_schema(view, 'view')

        # test missing fields
        view = get_default_view('image', 'my-image')
        view['sourceDisplays'][0]['imageDisplays'].pop('color')
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        # test invalid fields
        view = get_default_view('image', 'my-image')
        view['foo'] = 'bar'
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        view = get_default_view('image', 'my-image')
        view['sourceDisplays'][0]['imageDisplays']["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        # test invalid values
        invalid_kwargs = [
            {'color': "foobar"},
            {'color': "r=1,g=2,b=3,a=4,z=5"},
            {'contrastLimits': [-10., 5.]},
            {'contrastLimits': [1., 2., 3.]},
            {'showImagesIn3d': "foobar"},
            {'resolution3dView': [1., 2., 3., 4.]}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')

    def test_segmentation_view(self):
        from mobie.metadata import get_default_view

        # test the default view
        view = get_default_view('segmentation', 'my-seg')
        validate_with_schema(view, 'view')

        # test custom segmentation settings
        custom_kwargs = [
            {'alpha': 0.5, 'color': 'glasbey'},
            {'alpha': 0.9, 'color': 'viridis',
             'colorByColumn': 'colname', 'showSelectedSegmentsIn3d': True, "tables": ["a", "b"]},
            {'selectedSegmentIds': ['my-seg,0,1', 'my-seg,0,2', 'my-seg,1,10']}
        ]
        for kwargs in custom_kwargs:
            view = get_default_view('segmentation', 'my-seg', **kwargs)
            validate_with_schema(view, 'view')

        # test missing fields
        view = get_default_view('segmentation', 'my-seg')
        view['sourceDisplays'][0]['segmentationDisplays'].pop('alpha')
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        # test invalid fields
        view = get_default_view('segmentation', 'my-seg')
        view['sourceDisplays'][0]['segmentationDisplays']["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        # test invalid values
        invalid_kwargs = [
            {'alpha': 10},
            {'color': "red"},
            {'color': "foobar"},
            {'selectedSegmentIds': ['my-seg/abc,0,2']},
            {'selectedSegmentIds': ['my-segc,alpha,2']}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('segmentation', 'my-seg', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')

    def test_source_transforms(self):
        from mobie.metadata import get_default_view

        # test view with source transformations
        trafo_kwargs = [
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist(),
                                   'name': 'my-affine'}]},
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist()}
                                  for _ in range(3)]},
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist(),
                                   'timepoints': [0]}]},
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist(),
                                   'timepoints': [1, 2, 3]}]}
        ]
        for kwargs in trafo_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            validate_with_schema(view, 'view')

        # test invalid fields
        view = get_default_view('image', 'my-image', **trafo_kwargs[0])
        view["sourceTransforms"].append({"invalidTransform": {"name": "invalid", "parameters": 0}})
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        view = get_default_view('image', 'my-image', **trafo_kwargs[0])
        view["sourceTransforms"][0]["affine"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(view, 'view')

        # test invalid values
        invalid_kwargs = [
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist(),
                                   'timepoints': [-1]}]},
            {'sourceTransforms': [{'parameters': np.random.rand(12).tolist(),
                                   'timepoints': [-1, 2, 3]}]}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')

    def test_viewer_transform(self):
        from mobie.metadata import get_default_view

        trafo_kwargs = [
            {'viewerTransform': {"timepoint": 0}},
            {'viewerTransform': {'affine': np.random.rand(12).tolist()}},
            {'viewerTransform': {'affine': np.random.rand(12).tolist(), "timepoint": 0}},
            {'viewerTransform': {'normalizedAffine': np.random.rand(12).tolist()}},
            {'viewerTransform': {'normalizedAffine': np.random.rand(12).tolist(), "timepoint": 1}},
            {'viewerTransform': {'position': np.random.rand(3).tolist()}},
            {'viewerTransform': {'position': np.random.rand(3).tolist(), "timepoint": 2}}
        ]
        for kwargs in trafo_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            validate_with_schema(view, 'view')

        # test missing fields
        invalid_kwargs = [
            {'viewerTransform': {}},
            {'viewerTransform': {"foo": "bar"}}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')

        # test invalid fields
        invalid_kwargs = [
            {'viewerTransform': {'timepoint': 0, "foo": "bar"}},
            {'viewerTransform': {'affine': np.random.rand(12).tolist(), "foo": "bar"}},
            {'viewerTransform': {'normalizedAffine': np.random.rand(12).tolist(), "x": "y"}},
            {'viewerTransform': {'position': np.random.rand(3).tolist(), "a": 3}}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')

        # test invalid values
        invalid_kwargs = [
            {'viewerTransform': {'timepoint': -1}},
            {'viewerTransform': {'affine': np.random.rand(11).tolist()}},
            {'viewerTransform': {'normalizedAffine': np.random.rand(13).tolist()}},
            {'viewerTransform': {'position': np.random.rand(4).tolist()}}
        ]
        for kwargs in invalid_kwargs:
            view = get_default_view('image', 'my-image', **kwargs)
            with self.assertRaises(ValidationError):
                validate_with_schema(view, 'view')


if __name__ == '__main__':
    unittest.main()
