import multiprocessing
import os

import mobie.metadata as metadata
import mobie.utils as utils
from mobie.import_data import import_image_data
from mobie.xml_utils import update_transformation_parameter


# TODO support default arguments for scale factors and chunks
def add_image(input_path, input_key,
              root, dataset_name, image_name,
              resolution, scale_factors, chunks,
              file_format="bdv.n5", menu_name=None,
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
        file_format [str] - the file format used to store the data internally (default: bdv.n5)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        view [dict] - default view settings for this source (default: None)
        transformation [list or np.ndarray] - parameter for affine transformation
            applied to the data on the fly (default: None)
        unit [str] - physical unit of the coordinate system (default: micrometer)
        is_default_dataset [bool] - whether to set new dataset as default dataset.
            Only applies if the dataset is being created. (default: False)
    """
    view = utils.require_dataset_and_view(root, dataset_name, file_format,
                                          source_type="image", source_name=image_name,
                                          menu_name=menu_name, view=view,
                                          is_default_dataset=is_default_dataset)

    dataset_folder = os.path.join(root, dataset_name)
    tmp_folder = f'tmp_{dataset_name}_{image_name}' if tmp_folder is None else tmp_folder

    # import the image data and add the metadata
    data_path, image_metadata_path = utils.get_internal_paths(dataset_folder, file_format, image_name)
    import_image_data(input_path, input_key, data_path,
                      resolution, scale_factors, chunks,
                      tmp_folder=tmp_folder, target=target,
                      max_jobs=max_jobs, unit=unit,
                      source_name=image_name,
                      file_format=file_format)
    metadata.add_source_to_dataset(dataset_folder, 'image', image_name, image_metadata_path, view=view)

    if transformation is not None:
        update_transformation_parameter(image_metadata_path, transformation)


def main():
    description = """Add image data to MoBIE dataset.
                     Initialize the dataset if it does not exist."""
    parser = utils.get_base_parser(description)
    args = parser.parse_args()

    resolution, scale_factors, chunks, transformation = utils.parse_spatial_args(args)
    view = utils.parse_view(args)
    add_image(args.input_path, args.input_key,
              args.root, args.dataset_name, args.name,
              resolution=resolution, scale_factors=scale_factors, chunks=chunks,
              view=view, menu_name=args.menu_name,
              tmp_folder=args.tmp_folder, target=args.target, max_jobs=args.max_jobs,
              is_default_dataset=bool(args.is_default_dataset),
              transformation=transformation, unit=args.unit)
