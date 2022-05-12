import argparse
import json
from .metadata import validate_view_metadata


def validate_views(view_file):
    with open(view_file, "r") as f:
        views = json.load(f)["views"]
    for name, view in views.items():
        validate_view_metadata(view)


def main():
    parser = argparse.ArgumentParser("Validate MoBIE view metadata")
    parser.add_argument("--input", "-i", type=str, required=True, help="the input json file with view definitions")
    args = parser.parse_args()
    validate_views(args.input)
