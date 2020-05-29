import os
import json
import shutil
import numpy as np
import numbers

from elf.io import open_file
from pybdv.converter import copy_dataset
from pybdv.metadata import write_n5_metadata, get_data_path, get_bdv_format
from pybdv.util import get_key, get_number_of_scales, get_scale_factors

from .xml_utils import copy_xml_with_newpath
from .util import write_additional_table_file
from ..validation import IMAGE_DICT_KEYS


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


def copy_file(xml_in, xml_out, storage='local'):
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


def copy_tables(src_folder, dst_folder, table_folder=None):
    if table_folder is None:
        table_in = src_folder
        table_out = dst_folder
    else:
        table_in = os.path.join(src_folder, table_folder)
        table_out = os.path.join(dst_folder, table_folder)
    os.makedirs(table_out, exist_ok=True)

    table_files = os.listdir(table_in)
    table_files = [ff for ff in table_files if os.path.splitext(ff)[1] == '.csv']

    for ff in table_files:
        src_file = os.path.join(table_in, ff)
        dst_file = os.path.join(table_out, ff)
        make_squashed_link(src_file, dst_file)

    # write the txt file for additional tables
    write_additional_table_file(table_out)


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


def copy_image_data(src_folder, dst_folder, exclude_prefixes=[]):
    # load all image properties from the image dict
    image_dict = os.path.join(src_folder, 'images', 'images.json')
    with open(image_dict, 'r') as f:
        image_dict = json.load(f)

    for name, properties in image_dict.items():
        type_ = properties['Type']
        # don't copy segmentations
        if type_ not in ('Image', 'Mask'):
            continue
        # check if we exclude this prefix
        prefix = '-'.join(name.split('-')[:4])
        if prefix in exclude_prefixes:
            continue
        # copy the xmls for the different storages
        for storage, relative_xml in properties['Storage'].items():
            in_path = os.path.join(src_folder, 'images', relative_xml)
            out_path = os.path.join(dst_folder, 'images', relative_xml)
            # copy the xml
            copy_file(in_path, out_path, storage)


def copy_misc_data(src_folder, dst_folder):
    # copy the bookmarks
    bkmrk_in = os.path.join(src_folder, 'misc', 'bookmarks.json')
    if os.path.exists(bkmrk_in):
        shutil.copyfile(bkmrk_in,
                        os.path.join(dst_folder, 'misc', 'bookmarks.json'))

    # TODO take care of additional data in the misc folder
    # # copy the dynamic segmentation dict
    # shutil.copyfile(os.path.join(src_folder, 'misc', 'dynamic_segmentations.json'),
    #                 os.path.join(dst_folder, 'misc', 'dynamic_segmentations.json'))

    # # copy the aux gene data
    # prospr_prefix = 'prospr-6dpf-1-whole'
    # aux_name = '%s_meds_all_genes.xml' % prospr_prefix
    # copy_file(os.path.join(src_folder, 'misc', aux_name),
    #           os.path.join(dst_folder, 'misc', aux_name))


def copy_segmentation(src_folder, dst_folder, name, properties):
    # copy the xmls for the different storages
    for storage, relative_xml in properties['Storage'].items():
        in_path = os.path.join(src_folder, 'images', relative_xml)
        out_path = os.path.join(dst_folder, 'images', relative_xml)
        # copy the xml
        copy_file(in_path, out_path, storage)
    # link the id look-up-table
    link_id_lut(src_folder, dst_folder, name)


def copy_segmentations(src_folder, dst_folder, exclude_prefixes=[]):
    # load all image properties from the image dict
    image_dict = os.path.join(src_folder, 'images', 'images.json')
    with open(image_dict, 'r') as f:
        image_dict = json.load(f)

    for name, properties in image_dict.items():
        type_ = properties['Type']
        # only copy segmentations
        if type_ != 'Segmentation':
            continue
        # check if we exclude this prefix
        prefix = '-'.join(name.split('-')[:4])
        if prefix in exclude_prefixes:
            continue
        copy_segmentation(src_folder, dst_folder, name, properties)


def copy_all_tables(src_folder, dst_folder):
    image_dict = os.path.join(src_folder, 'images', 'images.json')
    with open(image_dict) as f:
        image_dict = json.load(f)

    for name, properties in image_dict.items():
        table_folder = properties.get('TableFolder', None)
        if table_folder is None:
            continue
        copy_tables(src_folder, dst_folder, table_folder)


def normalize_scale_factors(scale_factors, start_scale):
    # we expect scale_factors[0] == [1 1 1]
    assert np.prod(scale_factors[0]) == 1

    # convert to relative scale factors
    rel_scales = [scale_factors[0]]
    for scale in range(1, len(scale_factors)):
        rel_factor = [sf / prev_sf for sf, prev_sf in zip(scale_factors[scale],
                                                          scale_factors[scale - 1])]
        rel_scales.append(rel_factor)

    # return the relative scales starting at the new scale
    new_factors = [[1., 1., 1.]] + rel_scales[(start_scale + 1):]
    return new_factors


def copy_attributes(in_file, in_key, out_file, out_key):
    with open_file(in_file, 'r') as fin, open_file(out_file) as fout:
        ds_in = fin[in_key]
        ds_out = fout[out_key]
        for k, v in ds_in.attrs.items():
            if isinstance(v, numbers.Real):
                v = float(v)
            elif isinstance(v, numbers.Integral):
                v = int(v)
            elif isinstance(v, np.ndarray):
                v = v.tolist()
            ds_out.attrs[k] = v


def copy_to_bdv_n5(in_file, out_file, chunks, resolution,
                   n_threads=32, start_scale=0):

    n_scales = get_number_of_scales(in_file, 0, 0)
    scale_factors = get_scale_factors(in_file, 0)
    # double check newly implemented functions in pybdv
    assert n_scales == len(scale_factors)

    scale_factors = normalize_scale_factors(scale_factors, start_scale)

    for out_scale, in_scale in enumerate(range(start_scale, n_scales)):
        in_key = get_key(True, 0, 0, in_scale)
        out_key = get_key(False, 0, 0, out_scale)

        if chunks is None:
            with open_file(in_file, 'r') as f:
                chunks_ = f[in_key].chunks
        else:
            chunks_ = chunks

        copy_dataset(in_file, in_key, out_file, out_key,
                     convert_dtype=False,
                     chunks=chunks_,
                     n_threads=n_threads)
        copy_attributes(in_file, in_key, out_file, out_key)

    write_n5_metadata(out_file, scale_factors, resolution, setup_id=0)


def copy_and_check_image_dict(src_folder, dst_folder, exclude_prefixes=[]):
    image_dict_in = os.path.join(src_folder, 'images', 'images.json')
    image_dict_out = os.path.join(dst_folder, 'images', 'images.json')
    with open(image_dict_in) as f:
        image_dict = json.load(f)

    for name, properties in image_dict.items():

        # TODO if we have exclude prefixes check if the name is in it.

        intersection = set(properties.keys()) - IMAGE_DICT_KEYS
        if len(intersection) > 0:
            raise RuntimeError("Validating image dict: invalid keys %s" % str(intersection))

        storage = properties['Storage']
        # validate local xml location
        xml = storage['local']
        xml = os.path.join(dst_folder, 'images', xml)
        if not os.path.exists(xml):
            raise RuntimeError("Validating image dict: could not find %s" % xml)

        # validate data location
        data_path = get_data_path(xml, return_absolute_path=True)
        if not os.path.exists(data_path):
            raise RuntimeError("Validating image dict: could not find %s" % data_path)

        # validate remote xml location
        if 'remote' in storage:
            xml = storage['remote']
            xml = os.path.join(dst_folder, 'images', xml)
            if not os.path.exists(xml):
                raise RuntimeError("Validating image dict: could not find %s" % xml)

        # validate tables
        if 'TableFolder' in properties:
            # check that we have the table folder
            table_folder = os.path.join(dst_folder, properties['TableFolder'])
            if not os.path.exists(table_folder):
                raise RuntimeError("Validating image dict: could not find %s" % table_folder)
            default_table = os.path.join(table_folder, 'default.csv')

            # check that we have the default table
            if not os.path.exists(default_table):
                raise RuntimeError("Validating image dict: could not find %s" % default_table)

            # if we have an additional table file, check that the additional tables exist
            additional_table_file = os.path.join(table_folder, 'additional_tables.txt')
            if os.path.exists(additional_table_file):
                with open(additional_table_file, 'r') as f:
                    for fname in f:
                        additional_table = os.path.join(table_folder, fname.rstrip('\n'))
                        if not os.path.exists(additional_table):
                            raise RuntimeError("Validating image dict: could not find %s" % additional_table)

    with open(image_dict_out, 'w') as f:
        json.dump(image_dict, f)


def copy_version_folder_helper(src_folder, dst_folder, exclude_prefixes=[]):
    # copy static image and misc data
    copy_image_data(src_folder, dst_folder, exclude_prefixes)
    copy_misc_data(src_folder, dst_folder)
    copy_segmentations(src_folder, dst_folder, exclude_prefixes)
    copy_all_tables(src_folder, dst_folder)
    copy_and_check_image_dict(src_folder, dst_folder,
                              exclude_prefixes=exclude_prefixes)
