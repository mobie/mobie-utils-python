import json
import os
import subprocess

import imageio
import luigi
import numpy as np

from elf.io import open_file
from elf.transformation import elastix_parser as trafo_parser
from cluster_tools.copy_volume import CopyVolumeLocal, CopyVolumeSlurm
from mobie.config import write_global_config
from pybdv.metadata import get_resolution
from .registration_impl import (registration_affine,
                                registration_bdv,
                                registration_transformix)


DATA_EXTENSIONS = ['.n5', '.h5', '.hdf', '.hdf5']


def data_path_to_xml_path(data_path, pass_exist_check=False):
    for ext in DATA_EXTENSIONS:
        xml_path = data_path.replace(ext, '.xml')
        if xml_path.endswith('.xml') and (pass_exist_check or os.path.exists(xml_path)):
            return xml_path
    return None


def read_resolution(in_path, resolution):
    xml_path = data_path_to_xml_path(in_path)
    if xml_path is None:
        return resolution
    else:
        return get_resolution(xml_path, setup_id=0)


def save_tif(data, path, fiji_executable, resolution):
    # write initial tif with imageio
    imageio.volwrite(path, data)

    # encode the arguments for the imagej macro:
    # imagej macros can only take a single string as argument, so we need
    # to comma seperate the individual arguments
    assert "," not in path, "Can't encode pathname containing a comma"
    arguments = "%s,%i,%f,%f,%f" % (os.path.abspath(path), data.shape[0],
                                    resolution[0], resolution[1], resolution[2])

    # call the imagej macro
    script = os.path.split(__file__)[0]
    script = os.path.join(script, 'set_voxel_size.ijm')
    cmd = [fiji_executable, '-batch', '--headless', script, arguments]
    subprocess.run(cmd)


# TODO implement this as cluster task to make it run safely on login nodes
def write_transformix_input(in_path, in_key, out_path,
                            fiji_executable, resolution,
                            tmp_folder, target, max_jobs):
    # try to read the resolution from the input dataset,
    # otherwise fall back to the resolution that was given
    res = read_resolution(in_path, resolution)

    with open_file(in_path, 'r') as f:
        ds = f[in_key]
        ds.n_threads = max_jobs
        data = ds[:]

    save_tif(data, out_path, fiji_executable, resolution=res)


def write_transformix_output(in_path, out_path, out_key, chunks, tmp_folder, target, max_jobs):
    task = CopyVolumeSlurm if target == 'slurm' else CopyVolumeLocal

    config_dir = os.path.join(tmp_folder, 'configs')
    write_global_config(config_dir, block_shape=chunks)

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


def apply_registration(input_path, input_key,
                       output_path, output_key,
                       transformation_file, method, interpolation,
                       fiji_executable, elastix_directory,
                       resolution, chunks,
                       tmp_folder, target, max_jobs):

    trafo_type = trafo_parser.get_transformation_type(transformation_file)
    if trafo_type is None:
        raise ValueError(f"{transformation_file} is not an elastix transformation")

    os.makedirs(tmp_folder, exist_ok=True)

    if method == 'transformix':
        assert fiji_executable is not None and os.path.exists(fiji_executable)
        assert elastix_directory is not None and os.path.exists(elastix_directory)

        # get the data type of the input dataset
        with open_file(input_path, 'r') as f:
            ds = f[input_key]
            dtype = ds.dtype

        # make sure it's in the supported datatypes and set the correct
        # datatype name for elastix
        if dtype == np.dtype('uint8'):
            dtype = 'unsigned char'
        elif dtype == np.dtype('uint16'):
            dtype = 'unsigned short'
        else:
            msg = f"Invalid dtype {dtype} encountered, expected either uint8 or uint16"
            raise RuntimeError(msg)

        input_tmp_path = os.path.join(tmp_folder, 'input.tif')
        output_tmp_path = os.path.join(tmp_folder, 'output')
        write_transformix_input(input_path, input_key, input_tmp_path,
                                fiji_executable, resolution,
                                tmp_folder, target, max_jobs)

        registration_transformix(input_tmp_path, output_tmp_path,
                                 transformation_file, fiji_executable,
                                 elastix_directory, tmp_folder,
                                 interpolation=interpolation, output_format='tif',
                                 result_dtype=dtype, target='target',
                                 n_threads=max_jobs)

        output_tmp_path += '-ch0.tif'
        write_transformix_output(output_tmp_path, output_path, output_key,
                                 chunks, tmp_folder, target, max_jobs)

    elif method == 'bdv':
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
        registration_bdv(xml_path, xml_out_path, transformation_file)

    elif method == 'affine':
        registration_affine()

    else:
        msg = (
            f"Invalid registration method {method} provided."
            "Choose one of ('transformix', 'bdv', 'affine')"
        )
        raise ValueError(msg)
