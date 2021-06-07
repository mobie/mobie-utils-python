import unittest
from jsonschema import ValidationError

import mobie.metadata as metadata
from mobie.validation.utils import validate_with_schema


class TestDatasetMetadata(unittest.TestCase):
    def get_dataset_metadata(self):
        dataset_metadata = {
            "is2D": False,
            "description": "My dataset.",
            "sources": {
                "image1": metadata.get_image_metadata("image1", "/images/image1.xml", file_format="bdv.n5"),
                "seg1": metadata.get_segmentation_metadata("seg1", "/images/seg1.xml", file_format="bdv.n5")
            },
            "views": {
                "default": metadata.get_default_view("image", "image1")
            }
        }
        return dataset_metadata

    def test_dataset_metadata(self):
        ds_metadata = self.get_dataset_metadata()
        validate_with_schema(ds_metadata, "dataset")

        # check missing fields
        ds_metadata = self.get_dataset_metadata()
        ds_metadata.pop("sources")
        with self.assertRaises(ValidationError):
            validate_with_schema(ds_metadata, "dataset")

        # check invalid fields
        ds_metadata = self.get_dataset_metadata()
        ds_metadata["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(ds_metadata, "dataset")

        ds_metadata = self.get_dataset_metadata()
        ds_metadata["sources"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(ds_metadata, "dataset")

        ds_metadata = self.get_dataset_metadata()
        ds_metadata["views"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(ds_metadata, "dataset")

        # check invalid names
        ds_metadata = self.get_dataset_metadata()
        ds_metadata["sources"]["foo bar"] = metadata.get_image_metadata("image2", '/images/image2.xml',
                                                                        file_format="bdv.n5")
        with self.assertRaises(ValidationError):
            validate_with_schema(ds_metadata, "dataset")


if __name__ == '__main__':
    unittest.main()
