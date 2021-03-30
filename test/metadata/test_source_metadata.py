import unittest
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestSourceMetadata(unittest.TestCase):
    def test_image_source(self):
        from mobie.metadata import get_image_metadata

        name = 'my-image'
        source = get_image_metadata(name, "/path/to/bdv.xml")
        validate_with_schema(source, 'source')

        # check missing fields
        source = get_image_metadata(name, "/path/to/bdv.xml")
        source["image"].pop("imageDataLocations")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        # check invalid fields
        source = get_image_metadata(name, "/path/to/bdv.xml")
        source["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        source = get_image_metadata(name, "/path/to/bdv.xml")
        source["image"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        source = get_image_metadata(name, "/path/to/bdv.xml")
        source["image"]["imageDataLocations"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        # check invalid values
        source = get_image_metadata(name, "/path/to/bdv.xxl")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        source = get_image_metadata(name, "/path/to/bdv xyz.xml")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

    def test_segmentation_source(self):
        from mobie.metadata import get_segmentation_metadata

        name = 'my-segmentation'
        source = get_segmentation_metadata(name, "/path/to/bdv.xml")
        validate_with_schema(source, 'source')

        source = get_segmentation_metadata(name, "/path/to/bdv.xml",
                                           table_location="/path/to/tables")
        validate_with_schema(source, 'source')

        # check missing fields
        source = get_segmentation_metadata(name, "/path/to/bdv.xml")
        source["segmentation"].pop("imageDataLocations")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        # check invalid fields
        source = get_segmentation_metadata(name, "/path/to/bdv.xml")
        source["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        source = get_segmentation_metadata(name, "/path/to/bdv.xml")
        source["segmentation"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        source = get_segmentation_metadata(name, "/path/to/bdv.xml")
        source["segmentation"]["imageDataLocations"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')

        # check invalid values
        source = get_segmentation_metadata(name, "/path/to/bdv.xml",
                                           table_location="/path/to /table")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, 'source')


if __name__ == '__main__':
    unittest.main()
