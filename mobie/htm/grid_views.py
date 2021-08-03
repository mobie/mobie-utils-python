import os

import numpy as np
import mobie
from ..tables.grid_view_table import compute_grid_view_table


def _get_display(name, source_type, sources, settings):
    assert isinstance(settings, dict)
    if source_type == "image":
        if list(settings.keys())[0] == "imageDisplay":
            settings["imageDisplay"]["sources"] = sources
            settings["imageDisplay"]["name"] = sources
            return settings
        else:
            return mobie.metadata.get_image_display(
                name, sources, **settings
            )

    elif source_type == "segmentation":
        if list(settings.keys())[0] == "segmentationDisplay":
            settings["segmentationDisplay"]["sources"] = sources
            settings["segmentationDisplay"]["name"] = sources
            return settings
        else:
            return mobie.metadata.get_segmentation_display(
                name, sources, **settings
            )

    else:
        raise ValueError(f"Invalid source type {source_type}")


def _get_sources_and_site_names(metadata, source_prefixes, source_name_to_site_name, name_filter):
    all_sources = metadata["sources"]

    # get the sources for each of the surce prefixes
    this_sources = {
        prefix: [name for name in all_sources if name.startswith(prefix)]
        for prefix in source_prefixes
    }
    if name_filter is not None:
        this_sources = {prefix: [name for name in sources if name_filter(name)]
                        for prefix, sources in this_sources.items()}

    # make sure that the number of sites (= number of sources) is the same for each prefix
    n_sites = len(this_sources[source_prefixes[0]])
    assert all(len(sources) == n_sites for sources in this_sources.values())

    # get the site names and make sure that they are identical for all prefixes
    all_site_names = {
        source_prefix: [source_name_to_site_name(name, source_prefix) for name in sources]
        for source_prefix, sources in this_sources.items()
    }
    site_names = all_site_names[source_prefixes[0]]
    assert all(snames == site_names for snames in all_site_names.values())

    return this_sources, site_names


def get_plate_grid_view(metadata, source_prefixes,
                        source_types, source_settings, menu_name,
                        source_name_to_site_name,
                        site_name_to_well_name,
                        site_table, well_table,
                        well_to_position=None, name_filter=None,
                        sites_visible=True, wells_visible=True):
    assert len(source_prefixes) == len(source_types) == len(source_settings)
    this_sources, site_names = _get_sources_and_site_names(metadata, source_prefixes,
                                                           source_name_to_site_name, name_filter)

    # create the source displays
    source_displays = []
    for prefix, source_type, settings in zip(source_prefixes, source_types, source_settings):
        display = _get_display(prefix, source_type, this_sources[prefix], settings)
        source_displays.append(display)

    # get the mapping from sites to names and the unique well names
    sites_to_wells = np.array([
        site_name_to_well_name(site_name) for site_name in site_names
    ])
    well_names = np.unique(sites_to_wells)

    # create the grid transforms for aranging sites to wells
    source_transforms = []
    sources_per_well = {}  # keep track of the sources for each well
    all_site_sources = {}  # keep track of the mapping from sites to sources
    for well in well_names:
        site_ids = np.where(sites_to_wells == well)[0]
        this_site_names = [site_names[sid] for sid in site_ids]

        well_sources = {
            site_name: [sources[sid] for sources in this_sources.values()]
            for sid, site_name in zip(site_ids, this_site_names)
        }
        well_trafo = mobie.metadata.get_grid_source_transform(well_sources)
        source_transforms.append(well_trafo)

        sources_per_well[well] = [source for sources in well_sources.values()
                                  for source in sources]
        all_site_sources.update(well_sources)

    # create the annotation display for the sites
    site_display = mobie.metadata.get_source_annotation_display(
        "sites", all_site_sources,
        table_data={"tsv": {"relativePath": site_table}},
        tables=["default.tsv"],
        lut="glasbey",
        opacity=0.5,
        visible=sites_visible
    )
    source_displays.append(site_display)

    # create the grid transform for aranging wells to the plate
    plate_sources = {well: sources_per_well[well] for well in well_names}
    if well_to_position is None:
        well_positions = None
    else:
        well_positions = {well: well_to_position(well) for well in well_names}
    plate_trafo = mobie.metadata.get_grid_source_transform(
        plate_sources, positions=well_positions
    )
    source_transforms.append(plate_trafo)

    # create the annotation display for wells to plate
    well_display = mobie.metadata.get_source_annotation_display(
        "wells", plate_sources,
        table_data={"tsv": {"relativePath": well_table}},
        tables=["default.tsv"],
        lut="glasbey",
        opacity=0.5,
        visible=wells_visible
    )
    source_displays.append(well_display)

    view = {
        "isExclusive": True,
        "sourceDisplays": source_displays,
        "sourceTransforms": source_transforms,
        "uiSelectionGroup": menu_name
    }
    return view


def _get_default_site_table(ds_folder, metadata, source_prefixes,
                            source_name_to_site_name,
                            site_name_to_well_name,
                            name_filter):

    rel_table_folder = "tables/sites"
    table_path = os.path.join(ds_folder, rel_table_folder, "default.tsv")

    this_sources, site_names = _get_sources_and_site_names(metadata, source_prefixes,
                                                           source_name_to_site_name, name_filter)
    wells = [site_name_to_well_name(name) for name in site_names]
    sources = {name: source_prefixes for name in site_names}

    compute_grid_view_table(sources, table_path, wells=wells)
    return rel_table_folder


def _get_default_well_table(ds_folder, metadata, source_prefixes,
                            source_name_to_site_name,
                            site_name_to_well_name,
                            name_filter):

    rel_table_folder = "tables/wells"
    table_path = os.path.join(ds_folder, rel_table_folder, "default.tsv")

    this_sources, site_names = _get_sources_and_site_names(metadata, source_prefixes,
                                                           source_name_to_site_name, name_filter)
    wells = list(set([site_name_to_well_name(name) for name in site_names]))
    sources = {well: source_prefixes for well in wells}

    compute_grid_view_table(sources, table_path)
    return rel_table_folder


def add_plate_grid_view(ds_folder, view_name, menu_name,
                        source_prefixes, source_types, source_settings,
                        source_name_to_site_name,
                        site_name_to_well_name,
                        site_table=None, well_table=None,
                        well_to_position=None, name_filter=None,
                        sites_visible=True, wells_visible=True):
    metadata = mobie.metadata.read_dataset_metadata(ds_folder)

    if site_table is None:
        site_table = _get_default_site_table(ds_folder, metadata, source_prefixes,
                                             source_name_to_site_name,
                                             site_name_to_well_name,
                                             name_filter)
    if well_table is None:
        well_table = _get_default_well_table(ds_folder, metadata, source_prefixes,
                                             source_name_to_site_name,
                                             site_name_to_well_name,
                                             name_filter)

    view = get_plate_grid_view(metadata, source_prefixes, source_types,
                               source_settings, menu_name,
                               source_name_to_site_name=source_name_to_site_name,
                               site_name_to_well_name=site_name_to_well_name,
                               well_to_position=well_to_position,
                               site_table=site_table, well_table=well_table,
                               name_filter=name_filter,
                               sites_visible=sites_visible, wells_visible=wells_visible)
    metadata["views"][view_name] = view
    mobie.metadata.write_dataset_metadata(ds_folder, metadata)
