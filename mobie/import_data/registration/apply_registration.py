import json
import os
import subprocess

import imageio
import luigi
import numpy as np

from elf.io import open_file
from elf.transformation import elastix_parser
from cluster_tools.copy_volume import CopyVolumeLocal, CopyVolumeSlurm
from mobie.config import write_global_config
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


# TODO implement this as cluster task to make it run safely on login nodes
def write_transformix_input(in_path, in_key, out_path,
                            fiji_executable, resolution,
                            tmp_folder, target, max_jobs,
                            cast_to=None):
    with open_file(in_path, 'r') as f:
        ds = f[in_key]
        ds.n_threads = max_jobs
        data = ds[:]
    if cast_to is not None:
        data = data.astype(cast_to)

    save_tif(data, out_path, fiji_executable, resolution=resolution)


def write_transformix_output(in_path, out_path, out_key, chunks, tmp_folder, target, max_jobs):
    task = CopyVolumeSlurm if target == 'slurm' else CopyVolumeLocal
    config_dir = os.path.join(tmp_folder, 'configs')

    task_config = task.default_task_config()
    task_config.update({'chunks': chunks})
    with open(os.path.join(config_dir, 'copy_volume.config'), 'w') as f:
        json.dump(task_config, f)

    t = task(tmp_folder=tmp_folder, config_dir=config_dir, max_jobs=max_jobs,
             input_path=in_path, input_key='',
             output_path=out_path, output_key=out_key,
             prefix='transformix-output')
    ret = luigi.build([t], local_scheduler=True)
    if not ret:
        raise RuntimeError("Copying transformix output failed")


def apply_affine(input_path, input_key,
                 output_path, output_key,
                 transformation, interpolation,
                 shape, resolution, chunks,
                 tmp_folder, target, max_jobs,
                 bounding_box):
    os.makedirs(tmp_folder, exist_ok=True)
    if bounding_box is None:
        write_global_config(os.path.join(tmp_folder, 'configs'), block_shape=chunks)
    else:
        write_global_config(os.path.join(tmp_folder, 'configs'), block_shape=chunks,
                            roi_begin=bounding_box[0], roi_end=bounding_box[1])
    registration_affine(input_path, input_key,
                        output_path, output_key,
                        transformation, interpolation,
                        shape=shape, resolution=resolution,
                        chunks=chunks, tmp_folder=tmp_folder,
                        target=target, max_jobs=max_jobs)


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
                      tmp_folder, target, max_jobs):
    os.makedirs(tmp_folder, exist_ok=True)
    write_global_config(os.path.join(tmp_folder, 'configs'), block_shape=chunks)

    # get the data type of the input dataset
    with open_file(input_path, 'r') as f:
        ds = f[input_key]
        dtype = ds.dtype

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
                             n_threads=max_jobs)

    output_tmp_path += '-ch0.tif'
    write_transformix_output(output_tmp_path, output_path, output_key,
                             chunks, tmp_folder, target, max_jobs)


def apply_coordinate(input_path, input_key,
                     output_path, output_key,
                     transformation, elastix_directory,
                     shape, resolution, chunks,
                     tmp_folder, target, max_jobs,
                     bounding_box):
    os.makedirs(tmp_folder, exist_ok=True)
    if bounding_box is None:
        write_global_config(os.path.join(tmp_folder, 'configs'), block_shape=chunks)
    else:
        write_global_config(os.path.join(tmp_folder, 'configs'), block_shape=chunks,
                            roi_begin=bounding_box[0], roi_end=bounding_box[1])
    registration_coordinate(input_path, input_key,
                            output_path, output_key,
                            transformation=transformation,
                            elastix_directory=elastix_directory,
                            shape=shape, resolution=resolution,
                            tmp_folder=tmp_folder, target=target, max_jobs=max_jobs)


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
                       bounding_box=None):
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
                          max_jobs=max_jobs)
    # write on the fly-transformation to bdv xml metadata
    elif method == 'bdv':
        apply_bdv(input_path, output_path, transformation, resolution)
    # transform via affine transformation functionality of nifty
    elif method == 'affine':
        apply_affine(input_path, input_key,
                     output_path, output_key,
                     transformation, interpolation,
                     shape, resolution, chunks,
                     tmp_folder, target, max_jobs,
                     bounding_box)
    # transform via coordinate transformation functionality of transformix and nifty
    elif method == 'coordinate':
        apply_coordinate(input_path, input_key,
                         output_path, output_key,
                         transformation=transformation,
                         elastix_directory=elastix_directory,
                         shape=shape, resolution=resolution,
                         chunks=chunks, tmp_folder=tmp_folder,
                         target=target, max_jobs=max_jobs,
                         bounding_box=bounding_box)
    else:
        msg = (
            f"Invalid registration method {method} provided."
            "Choose one of ('transformix', 'bdv', 'affine', 'coordinate')"
        )
        raise ValueError(msg)

    return resolution
