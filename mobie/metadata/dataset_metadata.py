import json
import os
import shutil
import warnings
from glob import glob

from pybdv.metadata import get_data_path, get_bdv_format
from ..__version__ import SPEC_VERSION
from ..xml_utils import copy_xml_with_newpath
from .source_metadata import read_sources_metadata, write_sources_metadata


def _load_datasets(path):
    try:
        with open(path) as f:
            datasets = json.load(f)
    except (FileNotFoundError, ValueError):
        datasets = {}
        datasets['datasets'] = []
        datasets['specVersion'] = SPEC_VERSION
    return datasets


def have_dataset(root, dataset_name):
    path = os.path.join(root, 'datasets.json')
    datasets = _load_datasets(path)
    return dataset_name in datasets['datasets']


def add_dataset(root, dataset_name, is_default):
    path = os.path.join(root, 'datasets.json')
    datasets = _load_datasets(path)

    if dataset_name in datasets['datasets']:
        warnings.warn(f"Dataset {dataset_name} is already present!")
    else:
        datasets['datasets'].append(dataset_name)

    # if this is the only dataset we set it as default
    if is_default or len(datasets['datasets']) == 1:
        datasets['defaultDataset'] = dataset_name

    with open(path, 'w') as f:
        json.dump(datasets, f, sort_keys=True, indent=2)


def get_datasets(root):
    path = os.path.join(root, 'datasets.json')
    datasets = _load_datasets(path)
    return datasets['datasets']


def create_dataset_structure(root, dataset_name):
    """ Make the folder structure for a new dataset.

    Arguments:
        root [str] - the root data directory
        dataset_name [str] - name of the dataset
    """
    dataset_folder = os.path.join(root, dataset_name)
    os.makedirs(os.path.join(dataset_folder, 'tables'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'images', 'local'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'images', 'remote'), exist_ok=True)
    os.makedirs(os.path.join(dataset_folder, 'misc', 'bookmarks'), exist_ok=True)
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


def copy_xml_file(xml_in, xml_out, storage='local'):
    if storage == 'local':
        data_path = get_data_path(xml_in, return_absolute_path=True)
        bdv_format = get_bdv_format(xml_in)
        xml_dir = os.path.split(xml_out)[0]
        data_path = os.path.relpath(data_path, start=xml_dir)
        copy_xml_with_newpath(xml_in, xml_out, data_path,
                              path_type='relative', data_format=bdv_format)
    elif storage == 'remote':
        shutil.copyfile(xml_in, xml_out)
    else:
        raise ValueError("Invalid storage spec %s" % storage)


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


def copy_sources(src_folder, dst_folder, exclude_sources=[]):
    sources = read_sources_metadata(src_folder)
    new_sources = {}
    for name, metadata in sources.items():
        source_type = metadata['type']
        # don't copy exclude sources
        if name in exclude_sources:
            continue
        # copy the xmls for the different storages
        for storage, relative_xml in metadata['imageLocation'].items():
            in_path = os.path.join(src_folder, relative_xml)
            out_path = os.path.join(dst_folder, relative_xml)
            # copy the xml
            copy_xml_file(in_path, out_path, storage)
        if source_type == 'segmentation':
            if 'tableRootLocation' in metadata:
                copy_tables(src_folder, dst_folder, metadata['tableRootLocation'])
            # link the id look-up-table (platybrowser specific functionality)
            link_id_lut(src_folder, dst_folder, name)
        new_sources[name] = metadata
    write_sources_metadata(dst_folder, new_sources)


def copy_misc_data(src_folder, dst_folder, copy_misc=None):
    misc_src = os.path.join(src_folder, 'misc')
    misc_dst = os.path.join(dst_folder, 'misc')

    # copy the bookmarks
    bookmark_src = os.path.join(misc_src, 'bookmarks')
    bookmark_dst = os.path.join(misc_dst, 'bookmarks')

    for bkmrk in glob(os.path.join(bookmark_src, '*.json')):
        bkmrk_out = os.path.join(bookmark_dst, os.path.split(bkmrk)[1])
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
