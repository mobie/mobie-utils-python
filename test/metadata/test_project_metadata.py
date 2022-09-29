import json
import unittest

from jsonschema import ValidationError
from mobie import SPEC_VERSION
from mobie.validation.utils import validate_with_schema


class TestProjectMetadata(unittest.TestCase):
    def get_project_metadata(self):
        project_metadata = {
            "datasets": ["alpha", "beta", "gamma"],
            "defaultDataset": "alpha",
            "specVersion": SPEC_VERSION,
        }
        return project_metadata

    # local schema option for testing local changes
    def get_schema(self, local_schema=False):
        if local_schema:
            schema_path = "/home/pape/Work/mobie/mobie.github.io/schema/project.schema.json"
            with open(schema_path) as f:
                return json.load(f)
        else:
            return "project"

    def test_project_metadata(self):
        schema = self.get_schema()

        metadata = self.get_project_metadata()
        validate_with_schema(metadata, schema)

        # check optional fields and additional values
        metadata = self.get_project_metadata()
        metadata["description"] = "Lorem ipsum."
        metadata["references"] = ["https://my-publication.com"]
        validate_with_schema(metadata, schema)

        metadata = self.get_project_metadata()
        validate_with_schema(metadata, schema)

        # check missing fields
        metadata = self.get_project_metadata()
        metadata.pop("datasets")
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, schema)

        # check invalid fields
        metadata = self.get_project_metadata()
        metadata["abc"] = "def"
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, schema)

        # check invalid values
        metadata = self.get_project_metadata()
        invalid_version = SPEC_VERSION.split(".")
        invalid_version = invalid_version[0:1] + [str(int(invalid_version[1]) + 1)] + invalid_version[2:]
        invalid_version = ".".join(invalid_version)
        metadata["specVersion"] = invalid_version
        with self.assertRaises(ValidationError):
            validate_with_schema(metadata, "project")


if __name__ == '__main__':
    unittest.main()
