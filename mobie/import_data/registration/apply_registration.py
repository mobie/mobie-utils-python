import os
import subprocess

import bioimage_py as bp
import imageio
import numpy as np

from elf.transformation import elastix_parser
from mobie.utils import get_run_config
from mobie.import_data.utils import _create_level, _open_storage, _remove_output
from .registration_impl import (registration_affine,
                                registration_bdv,
                                registration_coordinate,
                                registration_transformix)


DATA_EXTENSIONS = ['.n5', '.h5', '.hdf', '.hdf5']


def data_path_to_xml_path(data_path, pass_exist_check=False):
    for ext in DATA_EXTENSIONS:
        xml_path = data_path.replace(ext, '.xml')
        if xml_path.endswith('.xml') and (pass_exist_check or os.path.exists(xml_path)):
            return xml_path
    return None


def save_tif(data, path, fiji_executable, resolution):
    script_root = os.path.split(__file__)[0]
    # write initial tif with imageio
    if data.ndim == 3:
        n_slices = data.shape[0]
        imageio.volwrite(path, data)
        script = os.path.join(script_root, 'set_voxel_size_3d.ijm')

        # encode the arguments for the imagej macro:
        arguments = "%s,%i,%f,%f,%f" % (os.path.abspath(path), n_slices,
                                        resolution[0], resolution[1], resolution[2])
    else:
        n_channels = data.shape[0]
        n_slices = data.shape[1]
        imageio.mvolwrite(path, data)
        script = os.path.join(script_root, 'set_voxel_size_4d.ijm')

        # encode the arguments for the imagej macro:
        arguments = "%s,%i,%i,%f,%f,%f" % (os.path.abspath(path), n_slices, n_channels,
                                           resolution[0], resolution[1], resolution[2])

    # imagej macros can only take a single string as argument, so we need
    # to comma seperate the individual arguments
    assert "," not in path, "Can't encode pathname containing a comma"

    # call the imagej macro
    cmd = [fiji_executable, '-batch', '--headless', script, arguments]
    subprocess.run(cmd)


def write_transformix_input(in_path, in_key, out_path,
                            fiji_executable, resolution,
                            tmp_folder, target, max_jobs,
                            cast_to=None):
    src = bp.open_source(in_path, in_key) if in_key else bp.open_source(in_path)
    data = src[:]
    if cast_to is not None:
        data = data.astype(cast_to)

    save_tif(data, out_path, fiji_executable, resolution=resolution)


def write_transformix_output(in_path, out_path, out_key, chunks, tmp_folder, target, max_jobs,
                             file_format="ome.zarr"):
    # the transformix result tif is materialized into the scale-0 dataset via a block-wise copy
    # (replacing the former cluster_tools CopyVolume task).
    src = bp.open_source(in_path)
    job_type, job_config, num_workers = get_run_config(target, max_jobs, tmp_folder)

    _remove_output(out_path)
    with _open_storage(out_path, file_format, mode="a") as f:
        ds = _create_level(f, file_format, 0, src.shape, chunks, src.dtype)
        bp.copy(src, output=ds, block_shape=tuple(int(c) for c in ds.chunks),
                job_type=job_type, job_config=job_config, num_workers=num_workers)


def apply_affine(input_path, input_key,
                 output_path, output_key,
                 transformation, interpolation,
                 shape, resolution, chunks,
                 tmp_folder, target, max_jobs,
                 bounding_box, file_format):
    os.makedirs(tmp_folder, exist_ok=True)
    registration_affine(input_path, input_key,
                        output_path, output_key,
                        transformation, interpolation,
                        shape=shape, resolution=resolution,
                        chunks=chunks, tmp_folder=tmp_folder,
                        target=target, max_jobs=max_jobs,
                        bounding_box=bounding_box, file_format=file_format)


def apply_bdv(input_path, output_path, transformation, resolution):
    xml_path = data_path_to_xml_path(input_path)
    if xml_path is None:
        raise ValueError(f"Could not find xml path for {input_path}")
    xml_out_path = data_path_to_xml_path(output_path, pass_exist_check=True)
    if xml_out_path is None:
        msg = (
            f"Output path {output_path} for xml file format has invalid extension;"
            f" expected one of {DATA_EXTENSIONS}"
        )
        raise ValueError(msg)
    registration_bdv(xml_path, xml_out_path, transformation, resolution)


def apply_transformix(input_path, input_key, output_path, output_key,
                      transformation, interpolation,
                      shape, resolution, chunks,
                      fiji_executable, elastix_directory,
                      tmp_folder, target, max_jobs,
                      file_format):
    os.makedirs(tmp_folder, exist_ok=True)

    # get the data type of the input dataset
    src = bp.open_source(input_path, input_key) if input_key else bp.open_source(input_path)
    dtype = src.dtype

    # make sure it's in the supported datatypes and set the correct
    # datatype name for elastix
    cast_to = None
    if dtype == np.dtype('uint8'):
        dtype = 'unsigned char'
    elif dtype == np.dtype('uint16'):
        dtype = 'unsigned short'
    elif np.issubdtype(np.dtype(dtype), np.integer):
        cast_to = 'uint16'
        dtype = 'unsigned short'
    else:
        msg = f"Invalid dtype {dtype} encountered, expected integer dtype"
        raise RuntimeError(msg)

    input_tmp_path = os.path.join(tmp_folder, 'input.tif')
    output_tmp_path = os.path.join(tmp_folder, 'output')

    write_transformix_input(input_path, input_key, input_tmp_path,
                            fiji_executable,
                            resolution=resolution,
                            tmp_folder=tmp_folder,
                            target=target,
                            max_jobs=max_jobs,
                            cast_to=cast_to)

    registration_transformix(input_tmp_path, output_tmp_path,
                             transformation, fiji_executable,
                             elastix_directory, tmp_folder,
                             interpolation=interpolation, output_format='tif',
                             shape=shape, resolution=resolution,
                             result_dtype=dtype, target=target,
                             n_threads=max_jobs, max_jobs=max_jobs)

    output_tmp_path += '-ch0.tif'
    write_transformix_output(output_tmp_path, output_path, output_key,
                             chunks, tmp_folder, target, max_jobs,
                             file_format=file_format)


def apply_coordinate(input_path, input_key,
                     output_path, output_key,
                     transformation, interpolation, elastix_directory,
                     shape, resolution, chunks,
                     tmp_folder, target, max_jobs,
                     bounding_box, file_format):
    os.makedirs(tmp_folder, exist_ok=True)
    registration_coordinate(input_path, input_key,
                            output_path, output_key,
                            transformation=transformation,
                            elastix_directory=elastix_directory,
                            shape=shape, resolution=resolution, chunks=chunks,
                            tmp_folder=tmp_folder, target=target, max_jobs=max_jobs,
                            interpolation=interpolation, bounding_box=bounding_box,
                            file_format=file_format)


def _validate_bounding_box(bounding_box):
    # bounding box should have len 2, start and stop
    # and then should have len 3 or be None
    if len(bounding_box) != 2:
        raise ValueError("Invalid bounding box")
    for bb in bounding_box:
        if bb is None:
            continue
        if len(bb) != 3:
            raise ValueError("Invalid bounding box")


def apply_registration(input_path, input_key,
                       output_path, output_key,
                       transformation, method, interpolation,
                       fiji_executable, elastix_directory,
                       shape, resolution, chunks,
                       tmp_folder, target, max_jobs,
                       bounding_box=None, file_format="ome.zarr"):
    if elastix_parser.get_transformation_type(transformation) is None:
        raise ValueError(f"{transformation} is not an elastix transformation")

    if bounding_box is not None:
        _validate_bounding_box(bounding_box)

    # transform via fiji transformix plugin
    if method == 'transformix':
        if not isinstance(transformation, str):
            msg = f"Transformix expects path to transformation of type str, got {type(transformation)} instead"
            raise ValueError(msg)
        if fiji_executable is None or not os.path.exists(fiji_executable):
            msg = f"Path to fiji {fiji_executable} is not valid"
        if elastix_directory is None or os.path.exists(elastix_directory):
            msg = f"Path to elastix directory {elastix_directory} is not valid"
        apply_transformix(input_path, input_key, output_path, output_key,
                          transformation, interpolation,
                          shape=shape,
                          resolution=resolution,
                          chunks=chunks,
                          fiji_executable=fiji_executable,
                          elastix_directory=elastix_directory,
                          tmp_folder=tmp_folder,
                          target=target,
                          max_jobs=max_jobs,
                          file_format=file_format)
    # write on the fly-transformation to bdv xml metadata
    elif method == 'bdv':
        apply_bdv(input_path, output_path, transformation, resolution)
    # transform via bioimage-py's affine source wrapper
    elif method == 'affine':
        apply_affine(input_path, input_key,
                     output_path, output_key,
                     transformation, interpolation,
                     shape, resolution, chunks,
                     tmp_folder, target, max_jobs,
                     bounding_box, file_format)
    # transform via transformix coordinate mapping, resampled with map_coordinates
    elif method == 'coordinate':
        apply_coordinate(input_path, input_key,
                         output_path, output_key,
                         transformation=transformation,
                         interpolation=interpolation,
                         elastix_directory=elastix_directory,
                         shape=shape, resolution=resolution,
                         chunks=chunks, tmp_folder=tmp_folder,
                         target=target, max_jobs=max_jobs,
                         bounding_box=bounding_box, file_format=file_format)
    else:
        msg = (
            f"Invalid registration method {method} provided."
            "Choose one of ('transformix', 'bdv', 'affine', 'coordinate')"
        )
        raise ValueError(msg)

    return resolution
