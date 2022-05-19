import json
import os
from glob import glob

import mobie.metadata as metadata
import pandas as pd


def _update_region_tables(ds_folder, table_path):
    table_folder = os.path.join(ds_folder, table_path)
    assert os.path.exists(table_folder), table_folder
    tables = glob(os.path.join(table_folder, "*.tsv"))
    for tab_path in tables:
        table = pd.read_csv(tab_path, sep="\t")
        table.rename(columns={"annotation_id": "region_id"}, inplace=True)
        table.to_csv(tab_path, sep="\t", index=False, na_rep="nan")


def _update_views(views, dataset_folder):
    new_views = {}
    for name, view in views.items():
        displays = view.get("sourceDisplays", [])
        new_displays = []

        for display in displays:
            display_type = list(display.keys())[0]

            if display_type == "sourceAnnotationDisplay":
                display_settings = display["sourceAnnotationDisplay"]
                _update_region_tables(dataset_folder, display_settings["tableData"]["tsv"]["relativePath"])
                selected_ids = display_settings.pop("selectedAnnotationIds", [])
                if selected_ids:
                    display_settings["selectedRegionIds"] = selected_ids
                display = {"regionDisplay": display_settings}

            new_displays.append(display)

        if new_displays:
            view["sourceDisplays"] = new_displays

        transforms = view.get("sourceTransforms", [])
        new_transforms = []
        for trafo in transforms:
            trafo_type = list(trafo.keys())[0]

            if trafo_type == "transformedGrid":
                trafo_settings = trafo["transformedGrid"]
                sources = trafo_settings.pop("sources")
                trafo_settings["nestedSources"] = sources
                trafo = {trafo_type: trafo_settings}

            new_transforms.append(trafo)

        if new_transforms:
            view["sourceTransforms"] = new_transforms

        new_views[name] = view
    return new_views


def migrate_name_spec(dataset_folder):
    """Update to name changes in https://github.com/mobie/mobie.github.io/pull/74.
    """
    ds_metadata = metadata.read_dataset_metadata(dataset_folder)
    views = ds_metadata["views"]
    new_views = _update_views(views, dataset_folder)
    ds_metadata["views"] = new_views
    metadata.write_dataset_metadata(dataset_folder, ds_metadata)

    extra_view_files = glob(os.path.join(dataset_folder, "misc", "views", "*.json"))
    for view_file in extra_view_files:
        with open(view_file) as f:
            views = json.load(f)["views"]
        new_views = _update_views(views, dataset_folder)
        with open(view_file, "w") as f:
            json.dump({"views": new_views}, f)
