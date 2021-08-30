import unittest
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestSourceMetadata(unittest.TestCase):
    def test_image_source(self):
        from mobie.metadata import get_image_metadata
        ds_folder = "/path"
        xml_path = "/path/to/bdv.xml"

        # check valid
        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5",
                                    description="My shiny image")
        validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["imageData"] = {"bdv.n5": {"relativePath": "path/to/bdv.xml"},
                                        "bdv.n5.s3": {"relativePath": "path/to/some-other.xml"}}
        validate_with_schema(source, "source")

        # check missing fields
        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"].pop("imageData")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["imageData"].pop("bdv.n5")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["imageData"]["bdv.n5"].pop("relativePath")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        # check invalid fields
        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["imageData"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_image_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["image"]["imageData"]["tiff"] = {"path": "my-tiff.tiff"}
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

    def test_segmentation_source(self):
        from mobie.metadata import get_segmentation_metadata
        ds_folder = "/path"
        xml_path = "/path/to/bdv.xml"

        # check valid
        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5")
        validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5",
                                           description="My shiny segmentation")
        validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path,
                                           table_location="/path/to/tables",
                                           file_format="bdv.n5")
        validate_with_schema(source, "source")

        # check missing fields
        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["segmentation"].pop("imageData")
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        # check invalid fields
        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["segmentation"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5")
        source["segmentation"]["imageData"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5",
                                           table_location="/path/to/tables")
        source["segmentation"]["tableData"]["excel"] = {"relative_path": "/path/to/tables"}
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")

        source = get_segmentation_metadata(ds_folder, xml_path, file_format="bdv.n5",
                                           table_location="/path/to/tables")
        source["segmentation"]["tableData"]["foo"] = "bar"
        with self.assertRaises(ValidationError):
            validate_with_schema(source, "source")


if __name__ == "__main__":
    unittest.main()
