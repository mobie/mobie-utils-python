import os
import shutil
import warnings
from glob import glob

from pybdv.metadata import get_data_path, get_bdv_format
from .utils import read_metadata, write_metadata
from ..validation import validate_view_metadata
from ..xml_utils import copy_xml_with_newpath


#
# functionality for reading / writing dataset.shema.json
#


def write_dataset_metadata(dataset_folder, dataset_metadata):
    path = os.path.join(dataset_folder, 'dataset.json')
    write_metadata(path, dataset_metadata)


def read_dataset_metadata(dataset_folder):
    path = os.path.join(dataset_folder, 'dataset.json')
    return read_metadata(path)


#
# functionality for creating datasets
#


def create_dataset_metadata(dataset_folder,
                            description=None,
                            is2d=False,
                            views=None,
                            sources=None,
                            n_timepoints=1):
    path = os.path.join(dataset_folder, 'dataset.json')
    if os.path.exists(path):
        raise RuntimeError(f"Dataset metadata at {path} already exists")
    metadata = {
        "is2D": is2d,
        "timepoints": n_timepoints,
        # we assume sources are already validated
        "sources": {} if sources is None else sources,
        # we assume views are already validated
        "views": {} if views is None else views
    }
    if description is not None:
        metadata["description"] = description
    write_dataset_metadata(dataset_folder, metadata)


def add_view_to_dataset(dataset_folder, view_name, view, overwrite=True):
    validate_view_metadata(view)
    metadata = read_dataset_metadata(dataset_folder)
    if view_name in metadata["views"]:
        msg = f"A view with name {view_name} already exists for the dataset {dataset_folder}"
        if overwrite:
            warnings.warn(msg)
        else:
            raise ValueError(msg)
    metadata["views"][view_name] = view
    write_dataset_metadata(dataset_folder, metadata)


def create_dataset_structure(root, dataset_name, file_formats):
    """ Make the folder structure for a new dataset.

    Arguments:
        root [str] - the root data directory
        dataset_name [str] - name of the dataset
    """
    dataset_folder = os.path.join(root, dataset_name)
    os.makedirs(os.path.join(dataset_folder, 'tables'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'misc', 'views'), exist_ok=True)
    for file_format in file_formats:
        os.makedirs(os.path.join(dataset_folder, 'images', file_format.replace('.', '-')), exist_ok=True)
    return dataset_folder


#
# functionality to copy a dataset folder
#

def make_squashed_link(src_file, dst_file, override=False):

    if os.path.exists(dst_file):
        if override and os.path.islink(dst_file):
            os.unlink(dst_file)
        elif override and not os.path.islink(dst_file):
            raise RuntimeError("Cannot override an actual file!")
        elif not override:
            return

    if os.path.islink(src_file):
        src_file = os.path.realpath(src_file)
    dst_folder = os.path.split(dst_file)[0]
    rel_path = os.path.relpath(src_file, dst_folder)
    os.symlink(rel_path, dst_file)


def link_id_lut(src_folder, dst_folder, name):
    # for local storage:
    # make link to the previous id look-up-table (if present)
    lut_name = 'new_id_lut_%s.json' % name
    lut_in = os.path.join(src_folder, 'misc', lut_name)
    if not os.path.exists(lut_in):
        return
    lut_out = os.path.join(dst_folder, 'misc', lut_name)
    if not os.path.exists(lut_out):
        rel_path = os.path.relpath(lut_in, os.path.split(lut_out)[0])
        os.symlink(rel_path, lut_out)


def copy_tables(src_folder, dst_folder, table_folder=None):
    if table_folder is None:
        table_in = src_folder
        table_out = dst_folder
    else:
        table_in = os.path.join(src_folder, table_folder)
        table_out = os.path.join(dst_folder, table_folder)
    os.makedirs(table_out, exist_ok=True)

    table_files = os.listdir(table_in)
    table_files = [ff for ff in table_files if os.path.splitext(ff)[1] in ('.csv', '.tsv')]

    for ff in table_files:
        src_file = os.path.join(table_in, ff)
        dst_file = os.path.join(table_out, ff)
        make_squashed_link(src_file, dst_file)


def copy_xml_file(xml_in, xml_out, file_format):
    if file_format in ('bdv.hdf5', 'bdv.n5'):
        data_path = get_data_path(xml_in, return_absolute_path=True)
        bdv_format = get_bdv_format(xml_in)
        xml_dir = os.path.split(xml_out)[0]
        data_path = os.path.relpath(data_path, start=xml_dir)
        copy_xml_with_newpath(xml_in, xml_out, data_path,
                              path_type='relative', data_format=bdv_format)
    elif file_format == 'bdv.n5.s3':
        shutil.copyfile(xml_in, xml_out)
    else:
        raise ValueError(f"Invalid file format {file_format}")


def copy_sources(src_folder, dst_folder, exclude_sources=[]):
    dataset_metadata = read_dataset_metadata(src_folder)
    sources = dataset_metadata["sources"]

    new_sources = {}
    for name, metadata in sources.items():
        # don't copy exclude sources
        if name in exclude_sources:
            continue

        source_type = list(metadata.keys())[0]
        metadata = metadata[source_type]

        # copy the xml file (if we have a bdv format)
        storage = metadata['imageData']
        for file_format, storage in metadata['imageData'].items():
            if file_format.startswith('bdv'):
                rel_path = storage['relativePath']
                in_path = os.path.join(src_folder, rel_path)
                out_path = os.path.join(dst_folder, rel_path)
                copy_xml_file(in_path, out_path, file_format)
            else:
                print(f"Image data for source {name} in format {format} cannot be copied.")

        # copy table if we have it
        if source_type == 'segmentation':
            if 'tableData' in metadata:
                copy_tables(src_folder, dst_folder, metadata['tableData']['source'])
            # link the id look-up-table (platybrowser specific functionality)
            link_id_lut(src_folder, dst_folder, name)

        new_sources[name] = {source_type: metadata}

    dataset_metadata["sources"] = new_sources
    write_dataset_metadata(dst_folder, dataset_metadata)


def copy_misc_data(src_folder, dst_folder, copy_misc=None):
    misc_src = os.path.join(src_folder, 'misc')
    misc_dst = os.path.join(dst_folder, 'misc')

    # copy the views
    view_src = os.path.join(misc_src, 'views')
    view_dst = os.path.join(misc_dst, 'views')

    for bkmrk in glob(os.path.join(view_src, '*.json')):
        bkmrk_out = os.path.join(view_dst, os.path.split(bkmrk)[1])
        shutil.copyfile(bkmrk, bkmrk_out)

    # copy the leveling.json file
    leveling_src = os.path.join(misc_src, 'leveling.json')
    if os.path.exists(leveling_src):
        shutil.copyfile(leveling_src, os.path.join(misc_dst, 'leveling.json'))

    # copy additional data in the misc folder if copy_misc function is given
    if copy_misc is not None:
        copy_misc(src_folder, dst_folder)


def copy_dataset_folder(src_folder, dst_folder, exclude_sources=[], copy_misc=None):
    copy_misc_data(src_folder, dst_folder, copy_misc)
    copy_sources(src_folder, dst_folder, exclude_sources)
