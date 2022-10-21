import argparse
import os

import mobie
from mobie.tables import process_spot_table, read_table


def _get_spot_metadata_from_source(dataset_folder, source, is_2d):
    source_type, source = next(iter(source.items()))
    if source_type in ("image", "segmentation"):
        image_data = source["imageData"]
        shape = mobie.metadata.source_metadata.get_shape(image_data, dataset_folder)
        resolution = mobie.metadata.source_metadata.get_resolution(image_data, dataset_folder)
        bounding_box_min = [0, 0] if is_2d else [0, 0, 0]
        bounding_box_max = [sh * res for sh, res in zip(shape, resolution)]
        if is_2d and len(bounding_box_max) == 3:  # can happen for bdv formats
            bounding_box_max = bounding_box_max[:2]
        unit = mobie.metadata.source_metadata.get_unit(image_data, dataset_folder)
    elif source_type == "spots":
        bounding_box_min = source["boundingBoxMin"][::-1]
        bounding_box_max = source["boundingBoxMax"][::-1]
        unit = source["unit"]
    else:
        raise ValueError(f"Expected one of 'image', 'segmentation' or 'spots', got {source_type}.")

    return bounding_box_min, bounding_box_max, unit


def _process_spot_metadata(
    dataset_folder, metadata, table_folder, reference_source,
    bounding_box_min, bounding_box_max, unit, is_2d,
):
    # if we have a reference source, determine the source metadata from it
    if reference_source is not None:
        sources = metadata["sources"]
        assert reference_source in sources
        bounding_box_min, bounding_box_max, unit = _get_spot_metadata_from_source(
            dataset_folder, sources[reference_source], is_2d,
        )

    # if bounding_box_min or max are not passed determine it from the table
    if bounding_box_min is None or bounding_box_max is None:
        table = read_table(os.path.join(table_folder, "default.tsv"))
        coordinates = table[["y", "x"]] if is_2d else table[["z", "y", "x"]]
        if bounding_box_min is None:
            bounding_box_min = coordinates.min(axis=0).values.tolist()
        if bounding_box_max is None:
            bounding_box_max = coordinates.max(axis=0).values.tolist()

    # we assume that bounding box min / max are given in zyx, but need to be returned as xyz
    bounding_box_min = bounding_box_min[::-1]
    bounding_box_max = bounding_box_max[::-1]
    return bounding_box_min, bounding_box_max, unit


def add_spots(input_table, root, dataset_name, spot_name,
              additional_tables=None, bounding_box_min=None, bounding_box_max=None,
              menu_name=None, view=None, unit="micrometer",
              reference_source=None, description=None):
    """Add spot source to MoBIE dataset.

    Arguments:
        input_table [str or pandas.DataFrame] - the main table for the spots data.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the spots should be added to.
        spot_name [str] - name of the spots.
        additional_tables [dict[str, str] or dict[str, pandas.DataFrame]] - list of additional tables. (default: None)
        bounding_box_min [list[float]] - the minimum bounding box coordinates for the spot data (default: None)
        bounding_box_max [list[float]] - the maximum bounding box coordiants for the spot data (default: None)
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        view [dict] - default view settings for this source (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        reference_source [str] - reference source with image data that will be used to determine
            the bounding box and the unit (default: None)
        description [str] - description for this spots (default: None)
    """
    view = mobie.utils.require_dataset_and_view(root, dataset_name, file_format=None,
                                                source_type="spots",
                                                source_name=spot_name,
                                                menu_name=menu_name, view=view,
                                                is_default_dataset=False)
    dataset_folder = os.path.join(root, dataset_name)

    metadata = mobie.metadata.read_dataset_metadata(dataset_folder)
    is_2d = metadata.get("is2D", False)

    # process the table data
    table_folder = os.path.join(dataset_folder, "tables", spot_name)
    process_spot_table(table_folder, input_table, is_2d, additional_tables)

    # determine other source metadata
    bounding_box_min, bounding_box_max, unit = _process_spot_metadata(
        dataset_folder, metadata, table_folder, reference_source,
        bounding_box_min, bounding_box_max, unit, is_2d,
    )

    # add the spot source to the dataset metadata
    mobie.metadata.add_source_to_dataset(dataset_folder, "spots", spot_name,
                                         image_metadata_path=None,
                                         table_folder=table_folder, view=view,
                                         description=description, unit=unit,
                                         bounding_box_min=bounding_box_min,
                                         bounding_box_max=bounding_box_max)


def main():
    description = "Add spot source to MoBIE dataset."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--input_table", type=str,
                        help="the main table for the spots data",
                        required=True)
    parser.add_argument("--root", type=str,
                        help="root folder under which the MoBIE project is saved",
                        required=True)
    parser.add_argument("--dataset_name", type=str,
                        help="name of the dataset to which the source is added",
                        required=True)
    parser.add_argument("--name", type=str,
                        help="name of the source to be added",
                        required=True)
    parser.add_argument("--reference_source",
                        help="reference source used to derive additional spot source metadata")
    parser.add_argument("--unit", type=str, default="micrometer",
                        help="physical unit of the source data")
    parser.add_argument("--menu_name", type=str, default=None,
                        help="the menu name which will be used when grouping this source in the UI")
    args = parser.parse_args()

    add_spots(args.input_table, args.root, args.dataset_name, args.name,
              unit=args.unit, menu_name=args.menu_name,
              reference_source=args.reference_source)
