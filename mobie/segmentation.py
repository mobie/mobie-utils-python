"""Import segmentation label data into a MoBIE project.
"""

import multiprocessing
import os
from typing import Dict, List, Optional, Sequence, Union

import mobie
import numpy as np
import pandas as pd

from mobie.import_data import (import_segmentation,
                               import_segmentation_from_node_labels)
from mobie.tables import check_and_copy_default_table, compute_default_table


# TODO support transformation
# TODO support default arguments for scale factors and chunks
def add_segmentation(
    input_path: str,
    input_key: str,
    root: str,
    dataset_name: str,
    segmentation_name: str,
    resolution: Sequence[float],
    scale_factors: List[List[int]],
    chunks: Sequence[int],
    menu_name: Optional[str] = None,
    file_format: str = "ome.zarr",
    node_label_path: Optional[str] = None,
    node_label_key: Optional[str] = None,
    tmp_folder: Optional[str] = None,
    target: str = "local",
    max_jobs: int = multiprocessing.cpu_count(),
    add_default_table: Union[bool, str, pd.DataFrame] = True,
    view: Optional[Dict] = None,
    unit: str = "micrometer",
    is_default_dataset: bool = False,
    description: Optional[str] = None,
    is_2d: Optional[bool] = None,
) -> None:
    """Add segmentation source to MoBIE dataset.

    Args:
        input_path: The path to the segmentation to add.
        input_key: The key to the segmentation to add.
        root: The data root folder.
        dataset_name: The name of the dataset the segmentation should be added to.
        segmentation_name: The name of the segmentation.
        resolution: The resolution of the segmentation in micrometer.
        scale_factors: The scale factors used for down-sampling.
        chunks: The chunks for the data.
        menu_name: The menu name for this source.
            If none is given will be created based on the sourec name.
        file_format: The file format used to store the data internally.
        node_label_path: The path to a node-label assignment (a fragment -> segment id mapping).
            If given, these will be used to re-assign the label ids of the input segmentation.
        node_label_key: The key to the node labels, for the internal path where the node labels are stored.
        tmp_folder: Folder for temporary files.
        target: The computation target.
        max_jobs: The maximum number of jobs for parallelization.
        add_default_table: Whether to add the default segmentation table.
            Can also be a filepath to a table in tsv format or a pandas DataFrame.
            In the two latter cases the default table will be initialized from the passed data.
        view: The default view settings for this source.
        unit: The physical unit of the coordinate system.
        is_default_dataset: Whether to set new dataset as default dataset. Only applies if the dataset is being created.
        description: The description for this segmentation source.
        is_2d: Whether this is a 2d segmentation.
    """
    if isinstance(input_path, np.ndarray):
        input_path, input_key = mobie.utils.save_temp_input(input_path, tmp_folder, segmentation_name)

    view = mobie.utils.require_dataset_and_view(root, dataset_name, file_format,
                                                source_type="segmentation",
                                                source_name=segmentation_name,
                                                menu_name=menu_name, view=view,
                                                is_default_dataset=is_default_dataset,
                                                description=description)

    dataset_folder = os.path.join(root, dataset_name)
    tmp_folder = f"tmp_{dataset_name}_{segmentation_name}" if tmp_folder is None else tmp_folder

    # import the segmentation data
    data_path, image_metadata_path = mobie.utils.get_internal_paths(dataset_folder, file_format,
                                                                    segmentation_name)
    if node_label_path is not None:
        if node_label_key is None:
            raise ValueError("Expect node_label_key if node_label_path is given")
        import_segmentation_from_node_labels(input_path, input_key, data_path,
                                             node_label_path, node_label_key,
                                             resolution, scale_factors, chunks,
                                             tmp_folder=tmp_folder, target=target,
                                             max_jobs=max_jobs, unit=unit,
                                             source_name=segmentation_name,
                                             file_format=file_format)
    else:
        import_segmentation(input_path, input_key, data_path,
                            resolution, scale_factors, chunks,
                            tmp_folder=tmp_folder, target=target,
                            max_jobs=max_jobs, unit=unit,
                            source_name=segmentation_name,
                            file_format=file_format)

    if is_2d is None:
        is_2d = mobie.metadata.read_dataset_metadata(dataset_folder).get("is2D", False)
    # we initialize with an already computed default table
    if isinstance(add_default_table, (str, pd.DataFrame)):
        table_folder = os.path.join(dataset_folder, "tables", segmentation_name)
        table_path = os.path.join(table_folder, "default.tsv")
        os.makedirs(table_folder, exist_ok=True)
        input_table = add_default_table
        check_and_copy_default_table(input_table, table_path, is_2d)

    # compute the default segmentation table
    elif add_default_table:
        table_folder = os.path.join(dataset_folder, "tables", segmentation_name)
        table_path = os.path.join(table_folder, "default.tsv")
        os.makedirs(table_folder, exist_ok=True)
        key = mobie.utils.get_data_key(file_format, scale=0, path=data_path)
        compute_default_table(data_path, key, table_path, resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the dataset metadata
    mobie.metadata.add_source_to_dataset(dataset_folder, "segmentation",
                                         segmentation_name, image_metadata_path,
                                         table_folder=table_folder, view=view, is_2d=is_2d)


def main():
    """@private
    """
    description = "Add segmentation source to MoBIE dataset."
    parser = mobie.utils.get_base_parser(description)
    parser.add_argument("--node_label_path", type=str, default=None,
                        help="path to the node_labels for the segmentation")
    parser.add_argument("--node_label_key", type=str, default=None,
                        help="key for the node labels for segmentation")
    parser.add_argument("--add_default_table", type=int, default=1,
                        help="whether to add the default table")
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = mobie.utils.parse_spatial_args(args)
    view = mobie.utils.parse_view(args)

    if transformation is not None:
        raise NotImplementedError("Transformation is currently not supported")

    add_segmentation(args.input_path, args.input_key,
                     args.root, args.dataset_name, args.name,
                     node_label_path=args.node_label_path, node_label_key=args.node_label_key,
                     resolution=resolution, scale_factors=scale_factors, chunks=chunks,
                     add_default_table=bool(args.add_default_table),
                     view=view, unit=args.unit, menu_name=args.menu_name,
                     tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
                     is_default_dataset=bool(args.is_default_dataset))
