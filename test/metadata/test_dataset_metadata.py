import unittest
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestDatasetMetadata(unittest.TestCase):
    def get_dataset_metadata(self):
        from mobie.metadata import get_default_view, get_image_metadata, get_segmentation_metadata
        dataset_metadata = {
            "dataset": {"is2d": False, "description": "My dataset."},
            "sources": {
                "image1": get_image_metadata("image1", "/images/image1.xml",
                                             menu_item="images/image1"),
                "seg1": get_segmentation_metadata("seg1", "/images/seg1.xml",
                                                  menu_item="segmentations/seg1"),
            },
            "views": {
                "default": get_default_view("image", "image1")
            }
        }
        return dataset_metadata

    def test_dataset_metadata(self):
        metadata = self.get_dataset_metadata()
        validate_with_schema(metadata, "dataset")

        # check missing fields
        metadata = self.get_dataset_metadata()
        metadata.pop("sources")
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "dataset")

        # check invalid fields
        metadata = self.get_dataset_metadata()
        metadata["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "dataset")

        metadata = self.get_dataset_metadata()
        metadata["sources"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "dataset")

        metadata = self.get_dataset_metadata()
        metadata["views"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "dataset")


if __name__ == '__main__':
    unittest.main()
