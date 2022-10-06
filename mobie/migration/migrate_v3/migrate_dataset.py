import os
from glob import glob

import mobie.metadata as metadata
from mobie.validation import validate_dataset


# go through the views and:
# - rename 'tables' -> 'additionalTables', drop 'default.tsv' from the tables
#   and drop 'additionalTables' altogether if nothing is left
# - for regionDisplay: move 'tableData' to 'sources' and add reference to it
def migrate_table_spec(views, sources):

    def _update_tables_in_display(display):
        tables = display.pop("tables")
        assert tables[0] == "default.tsv"
        tables = tables[1:]
        if tables:
            display["additionalTables"] = tables
        return display

    region_table_sources = {}
    new_views = {}
    for name, view in views.items():

        if "sourceDisplays" in view:
            source_displays = view["sourceDisplays"]
            new_source_displays = []

            for display in source_displays:
                display_type, display = next(iter(display.items()))

                if display_type == "segmentationDisplay" and "tables" in display:
                    display = _update_tables_in_display(display)

                elif display_type == "regionDisplay":
                    display = _update_tables_in_display(display)
                    table_data = display.pop("tableData")
                    table_source_name = os.path.split(table_data["tsv"]["relativePath"])[1]
                    if table_source_name not in region_table_sources:
                        region_table_sources[table_source_name] = table_data
                    display["tableSource"] = table_source_name

                new_source_displays.append({display_type: display})

            view["sourceDisplays"] = new_source_displays

        new_views[name] = view

    for name, table_data in region_table_sources.items():
        sources[name] = {"regions": {"tableData": table_data}}

    return new_views, sources


# for merged grids the source names are automatically suffixed with "_{gridName}"
# so if we have a merged grid with a region display we assume the regionDisplay
# refers to the sources arranged in the grid and update the source names in there
def migrate_merged_grid_spec(views):
    new_views = {}

    # check if we have grid views
    for name, view in views.items():
        updated_names = {}

        if "sourceTransforms" in view:
            transforms = view["sourceTransforms"]
            for transform in transforms:
                trafo_type, trafo = next(iter(transform.items()))
                if trafo_type == "mergedGrid":
                    grid_name = trafo["mergedGridSourceName"]
                    updated_names.update({source: f"{source}_{grid_name}" for source in trafo["sources"]})

        if "sourceDisplays" in view:
            displays = view["sourceDisplays"]
            new_displays = []
            for display in displays:
                display_type, disp = next(iter(display.items()))
                if display_type == "regionDisplay" and updated_names:
                    sources = disp["sources"]
                    sources = {position: [updated_names[source] for source in position_sources]
                               for position, position_sources in sources.items()}
                    disp["sources"] = sources
                new_displays.append({display_type: disp})
            view["sourceDisplays"] = new_displays

        new_views[name] = view

    return new_views


def migrate_view_file(view_file, sources):
    views = metadata.utils.read_metadata(view_file)["views"]
    new_views, new_sources = migrate_table_spec(views, sources)
    new_views = migrate_merged_grid_spec(new_views)
    metadata.utils.write_metadata(view_file, {"views": new_views})
    return new_sources


def migrate_views(folder):
    ds_meta = metadata.read_dataset_metadata(folder)
    views, sources = ds_meta["views"], ds_meta["sources"]

    new_views, new_sources = migrate_table_spec(views, sources)
    new_views = migrate_merged_grid_spec(new_views)

    additional_view_files = glob(os.path.join(folder, "misc", "views", "*.json"))
    for view_file in additional_view_files:
        new_sources = migrate_view_file(view_file, new_sources)

    ds_meta["views"] = new_views
    ds_meta["sources"] = new_sources
    metadata.write_dataset_metadata(folder, ds_meta)


def migrate_dataset(folder):
    """Migrate dataset from spec version 0.2.0 to 0.3.0

    Arguments:
        folder [str] - dataset folder
    """
    migrate_views(folder)
    validate_dataset(folder, require_local_data=False, require_remote_data=False)
