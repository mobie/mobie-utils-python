import multiprocessing
import os
from copy import deepcopy

import mobie.metadata as metadata
from mobie.import_data import import_image_data
from mobie.utils import get_base_parser, parse_spatial_args, parse_view
from mobie.xml_utils import update_transformation_parameter
from mobie.validation import validate_view_metadata


# TODO support default arguments for scale factors and chunks
def add_image(input_path, input_key,
              root, dataset_name, image_name,
              resolution, scale_factors, chunks,
              menu_name=None,
              tmp_folder=None, target='local',
              max_jobs=multiprocessing.cpu_count(),
              view=None, transformation=None,
              unit='micrometer',
              is_default_dataset=False):
    """ Add an image source to a MoBIE dataset.

    Will create the dataset if it does not exist.

    Arguments:
        input_path [str] - path to the data that should be added.
        input_key [str] - key to the data that should be added.
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the image data should be added to.
        image_name [str] - name of the image data.
        resolution [list[float]] - resolution of the segmentation in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        view [dict] - default view settings for this source (default: None)
        transformation [list or np.ndarray] - parameter for affine transformation
            applied to the data on the fly (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        is_default_dataset [bool] - whether to set new dataset as default dataset.
            Only applies if the dataset is created. (default: False)
    """
    dataset_folder = os.path.join(root, dataset_name)
    # check if we have the project and dataset already
    proj_exists = metadata.project_exists(root)
    if proj_exists:
        ds_exists = metadata.dataset_exists(root, dataset_name)
    else:
        metadata.create_project_metadata(root)
        ds_exists = False

    if view is None:
        view = metadata.get_default_view('image', image_name, menu_name=menu_name)
    elif view is not None and menu_name is not None:
        view.update({"uiSelectionGroup": menu_name})
    validate_view_metadata(view, sources=[image_name])

    if not ds_exists:
        metadata.create_dataset_structure(root, dataset_name)
        default_view = deepcopy(view)
        default_view.update({"uiSelectionGroup": "bookmark"})
        metadata.create_dataset_metadata(dataset_folder, views={'default': default_view})

    tmp_folder = f'tmp_{dataset_name}_{image_name}' if tmp_folder is None else tmp_folder

    # import the image data and add the metadata
    data_path = os.path.join(dataset_folder, 'images', 'local', f'{image_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', 'local', f'{image_name}.xml')
    import_image_data(input_path, input_key, data_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=tmp_folder, target=target,
                      max_jobs=max_jobs, unit=unit,
                      source_name=image_name)
    metadata.add_source_metadata(dataset_folder, 'image', image_name, xml_path, view=view)

    if transformation is not None:
        update_transformation_parameter(xml_path, transformation)

    # need to add the dataset to datasets.json and create the default bookmark
    # if we have just created it
    if not ds_exists:
        metadata.add_dataset(root, dataset_name, is_default_dataset)


def main():
    description = """Add image data to MoBIE dataset.
                     Initialize the dataset if it does not exist."""
    parser = get_base_parser(description)
    parser.add_argument("--is_default_dataset", type=int, default=0,
                        help="")
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = parse_spatial_args(args)
    view = parse_view(args)
    add_image(args.input_path, args.input_key,
              args.root, args.dataset_name, args.name,
              resolution=resolution, scale_factors=scale_factors, chunks=chunks,
              view=view, menu_name=args.menu_name,
              tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
              is_default_dataset=bool(args.is_default_dataset),
              transformation=transformation, unit=args.unit)
