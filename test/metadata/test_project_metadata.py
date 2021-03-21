import unittest
from jsonschema import ValidationError
from mobie.validation.utils import validate_with_schema


class TestProjectMetadata(unittest.TestCase):
    def get_project_metadata(self):
        project_metadata = {
            "datasets": ["alpha", "beta", "gamma"],
            "defaultDataset": "alpha",
            "specVersion": "0.2.0"
        }
        return project_metadata

    def test_dataset_metadata(self):
        metadata = self.get_project_metadata()
        validate_with_schema(metadata, "project")

        # check missing fields
        metadata = self.get_project_metadata()
        metadata.pop("datasets")
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "project")

        # check invalid fields
        metadata = self.get_project_metadata()
        metadata["abc"] = "def"
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "project")

        # TODO need the regexps
        # check invalid values
        # metadata = self.get_project_metadata()
        # metadata["specVersion"] = "0.3.3"
        # with self.assertRaises(ValidationError):
        #     validate_with_schema(metadata, "project")


if __name__ == '__main__':
    unittest.main()
