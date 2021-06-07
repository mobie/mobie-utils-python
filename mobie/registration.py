import multiprocessing
import os
import warnings

import mobie.metadata as metadata
from mobie.import_data.utils import downscale, add_max_id
from mobie.tables import compute_default_table
from mobie.utils import get_base_parser, parse_spatial_args, parse_view
from mobie.validation import validate_view_metadata

try:
    from mobie.import_data.registration import apply_registration
except ImportError as e:
    warnings.warn(f"Could not import 'apply_registration' due to {e}.\n 'add_registered_volume' will not be available.")
    apply_registration = None


def add_registered_source(input_path, input_key, transformation,
                          root, dataset_name, source_name,
                          resolution, scale_factors, chunks, method,
                          menu_name=None, shape=None, source_type='image',
                          view=None, add_default_table=True,
                          fiji_executable=None, elastix_directory=None,
                          tmp_folder=None, target='local',
                          max_jobs=multiprocessing.cpu_count(),
                          bounding_box=None):
    """ Add a volume after registration in elastix format.

    Arguments:
        input_path [str] - path to the data that should be added.
        input_key [str] - key to the data that should be added.
        transformation [str] - file defining elastix transformation to be applied
        root [str] - data root folder.
        dataset_name [str] - name of the dataset the data should be added to.
        source_name [str] - name of the data.
        resolution [list[float]] - resolution of the data in micrometer.
        scale_factors [list[list[int]]] - scale factors used for down-sampling.
        chunks [list[int]] - chunks for the data.
        method [str] - method used to apply the registration transformation:
            'affine': apply transformation using elf/nifty functionality.
                only works for affine transformations or simpler.
            'coordinate': apply transformation based on coordinate transformation of transformix
            'bdv': write transformation to bdv metadata so that it's applied on the fly.
                only works for affine transformations or simpler.
            'transformix': apply transformation using transformix
        menu_name [str] - menu name for this source.
            If none is given will be created based on the image name. (default: None)
        shape [tuple[int]] - shape of the output volume. If None, the shape specified in
            the elastix transformation file will be used. (default: None)
        source_type [str] - type of the data, can be either 'image', 'segmentation' or 'mask'
            (default: 'image')
        add_default_table [bool] - whether to add the default table (default: True)
        tmp_folder [str] - folder for temporary files (default: None)
        target [str] - computation target (default: 'local')
        max_jobs [int] - number of jobs (default: number of cores)
        bounding_box [list[list[int]]] - bounding box where the registration is applied.
            needs to be specified in the output dataset space (default: None)
    """
    if apply_registration is None:
        raise ValueError("Could not import 'apply_registration' functionality")

    # check that we have this dataset
    if not metadata.dataset_exists(root, dataset_name):
        raise ValueError(f"Dataset {dataset_name} not found in {root}")
    tmp_folder = f'tmp_{source_name}' if tmp_folder is None else tmp_folder

    if view is None:
        view = metadata.get_default_view(source_type, source_name, menu_name=menu_name)
    elif view is not None and menu_name is not None:
        view.update({"uiSelectionGroup": menu_name})
    validate_view_metadata(view, sources=[source_name])

    dataset_folder = os.path.join(root, dataset_name)
    data_path = os.path.join(dataset_folder, 'images', f'{source_name}.n5')
    xml_path = os.path.join(dataset_folder, 'images', f'{source_name}.xml')
    data_key = 'setup0/timepoint0/s0'

    interpolation = 'linear' if source_type == 'image' else 'nearest'
    # the resolution might be changed after the registration, which we need to take into account
    # in the subsequent downscaling step
    effective_resolution = apply_registration(input_path, input_key, data_path, data_key,
                                              transformation, method, interpolation,
                                              fiji_executable=fiji_executable, elastix_directory=elastix_directory,
                                              shape=shape, resolution=resolution, chunks=chunks,
                                              tmp_folder=tmp_folder, target=target, max_jobs=max_jobs,
                                              bounding_box=bounding_box)

    data_key = 'setup0/timepoint0/s0'
    # we don't need to downscale the data if the transformation is applied on the fly by bdv
    if method != 'bdv':
        if source_type == 'image':
            ds_library = 'skimage'
            ds_library_kwargs = {}
        else:
            ds_library = 'vigra'
            ds_library_kwargs = {'order': 0}
        downscale(data_path, data_key, data_path,
                  effective_resolution, scale_factors, chunks,
                  tmp_folder, target, max_jobs, block_shape=chunks,
                  library=ds_library, library_kwargs=ds_library_kwargs,
                  source_name=source_name)
        add_max_id(input_path, input_key, data_path, data_key,
                   tmp_folder, target, max_jobs)

    # compute the default segmentation table
    if source_type == 'segmentation' and add_default_table:

        if method == 'bdv':
            msg = ("Cannot compute table for segmentation registered via bdv on-the-fly transformations."
                   "Use methd 'affine' instead to support this feature.")
            raise NotImplementedError(msg)

        table_folder = os.path.join(dataset_folder, 'tables', source_name)
        table_path = os.path.join(table_folder, 'default.tsv')
        os.makedirs(table_folder, exist_ok=True)
        compute_default_table(data_path, data_key, table_path, effective_resolution,
                              tmp_folder=tmp_folder, target=target,
                              max_jobs=max_jobs)
    else:
        table_folder = None

    # add the segmentation to the image dict
    metadata.add_source_metadata(dataset_folder, source_type,
                                 source_name, xml_path,
                                 view=view, table_folder=table_folder)


if __name__ == '__main__':
    description = "Apply transformation defined in elastix format to source and add it to MoBIE dataset."
    parser = get_base_parser(description, transformation_file=True)

    descr = """method used to apply the registration transformation:
            'affine': apply transformation using elf/nifty functionality.
                only works for affine transformations or simpler.
            'bdv': write transformation to bdv metadata so that it's applied on the fly.
                only works for affine transformations or simpler.
            'transformix': apply transformation using transformix
            'coordinate': apply transformation based on coordinate transformation of transformix
            """
    parser.add_argument('--method', type=str, required=True, help=descr)
    descr = """shape of the output volume. If None, the shape specified in
            the elastix transformation file will be used.
            """
    parser.add_argument('--shape', type=int, default=None, nargs=3,
                        help="")
    parser.add_argument('--source_type', type=str, default='image',
                        help="Type of the source, either 'image' or 'segmentation'")
    parser.add_argument('--add_default_table', type=int, default=1,
                        help="Add the default table for segmentations.")

    args = parser.parse_args()

    resolution, scale_factors, chunks = parse_spatial_args(args, parse_transformation=False)
    view = parse_view(args)
    add_registered_source(args.input_path, args.input_key, args.transformation,
                          args.root, args.dataset_name, args.name,
                          resolution, scale_factors, chunks,
                          method=args.method, menu_name=args.menu_name,
                          shape=args.shape, source_type=args.source_type,
                          add_default_table=bool(args.add_default_table),
                          tmp_folder=args.tmp_folder, targer=args.target, max_jobs=args.max_jobs)
