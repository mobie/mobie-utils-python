import os
import mobie.metadata as metadata
from mobie.validation import validate_dataset


def migrate_table_spec(folder):
    ds_meta = metadata.read_dataset_metadata(folder)

    # go through the views and:
    # - rename 'tables' -> 'additionalTables', drop 'default.tsv' from the tables
    #   and drop 'additionalTables' altogether if nothing is left
    # - for regionDisplay: move 'tableData' to 'sources' and add reference to it
    views = ds_meta["views"]
    region_table_sources = {}

    def _update_tables_in_display(display):
        tables = display.pop("tables")
        assert tables[0] == "default.tsv"
        tables = tables[1:]
        if tables:
            display["additionalTables"] = tables
        return display

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

    if region_table_sources:
        sources = ds_meta["sources"]
        for name, table_data in region_table_sources.items():
            sources[name] = {"regionTable": {"tableData": table_data}}
        ds_meta["sources"] = sources

    ds_meta["views"] = new_views
    metadata.write_dataset_metadata(folder, ds_meta)


def migrate_merged_grid_spec(folder):
    pass


def migrate_dataset(folder):
    """Migrate dataset from spec version 0.2.0 to 0.3.0

    Arguments:
        folder [str] - dataset folder
    """
    migrate_table_spec(folder)
    migrate_merged_grid_spec(folder)
    validate_dataset(folder, require_local_data=False, require_remote_data=False)
