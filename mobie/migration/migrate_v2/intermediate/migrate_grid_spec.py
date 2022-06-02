import json
import os
from copy import deepcopy
from glob import glob

import mobie.metadata as metadata
import pandas as pd


def update_grid_view(trafo, name):
    params = trafo["grid"]

    # update sources
    sources = {
        source_id: source_list
        for source_id, source_list in enumerate(params["sources"])
    }
    grid_params = {"sources": sources}
    additional_fields = ["name", "sourceNamesAfterTransformation", "timepoints"]
    for add_field_name in additional_fields:
        if add_field_name in params:
            grid_params[add_field_name] = params[add_field_name]
    if "positions" in params:
        grid_params["positions"] = {source_id: pos for source_id, pos in enumerate(params["positions"])}

    table_data = params["tableData"]
    tables = ["default.tsv"]
    annotation_display = metadata.view_metadata.get_region_display(
        name, sources, table_data, tables
    )

    return {"grid": grid_params}, annotation_display


def update_views(views):
    new_views = {}
    for name, view in views.items():
        has_source_trafo = "sourceTransforms" in view
        if has_source_trafo:
            trafos = view["sourceTransforms"]
            trafo_types = [list(trafo.keys())[0] for trafo in trafos]
            has_grid_trafo = "grid" in trafo_types
            if has_grid_trafo:
                new_view = deepcopy(view)
                new_trafos = []
                for trafo in trafos:
                    if list(trafo.keys())[0] == "grid":
                        trafo, annotation_display = update_grid_view(trafo, name)
                        new_view["sourceDisplays"].append(annotation_display)
                    new_trafos.append(trafo)
                new_view["sourceTransforms"] = new_trafos
                new_views[name] = new_view
                continue

        new_views[name] = view
    return new_views


def update_tables(views, dataset_folder):
    for name, view in views.items():
        displays = view["sourceDisplays"]
        for disp in displays:
            if list(disp.keys())[0] == "sourceAnnotationDisplay":
                props = disp["sourceAnnotationDisplay"]
                table_folder = os.path.join(
                    dataset_folder, props["tableData"]["tsv"]["relativePath"]
                )
                tables = glob(os.path.join(table_folder, "*.tsv"))
                for table_path in tables:
                    table = pd.read_csv(table_path, sep="\t")
                    table = table.rename(columns={"grid_id": "annotation_id"})
                    table.to_csv(table_path, sep="\t", index=False)


def migrate_grid_spec(dataset_folder):
    """ Update to the new grid and sourceAnnotationDisplay spec.

    See https://github.com/mobie/mobie-viewer-fiji/issues/343 for details
    """
    ds_meta = metadata.read_dataset_metadata(dataset_folder)
    views = ds_meta["views"]
    new_views = update_views(views)
    update_tables(new_views, dataset_folder)
    ds_meta["views"] = new_views
    metadata.write_dataset_metadata(dataset_folder, ds_meta)

    views_folder = os.path.join(dataset_folder, "misc", "views")
    view_files = glob(os.path.join(views_folder, "*.json"))
    for view_file in view_files:
        with open(view_file, "r") as f:
            views = json.load(f)["views"]
            new_views = update_views(views)
            update_tables(new_views, dataset_folder)
            metadata.utils.write_metadata(view_file, {"views": new_views})
