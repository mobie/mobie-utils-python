import json
import os
import xml.etree.ElementTree as ET
from glob import glob

import pandas as pd
from pybdv.metadata import indent_xml

import mobie.metadata as metadata
from mobie.tables.util import remove_background_label_row


#
# migrate source and bookmark metadata
#


# TODO
def migrate_source_metadata(name, source, dataset_folder):
    source_type = source['type']
    xml_locations = source['storage']
    local_xml = os.path.join('images', xml_locations['local'])
    assert os.path.exists(os.path.join(dataset_folder, local_xml))

    if source_type in ('image', 'mask'):
        menu_item = ''  # TODO name to menu item parsing

        view = metadata.get_default_view(
            "image", name,
            color=source['color'], contrastLimits=source['contrastLimits']
        )
        new_source = metadata.get_image_metadata(
            name, local_xml, menu_item,
            view=view
        )
        source_type = 'image'

    else:
        assert source_type == 'segmentation'
        menu_item = ''  # TODO name to menu item parsing

        seg_color = source['color']
        seg_color = 'glasbey' if seg_color == 'randomFromGlasbey' else seg_color
        view = metadata.get_default_view(
            "segmentation", name, color=seg_color
        )

        if 'tableFolder' in source:
            table_location = source['tableFolder']
            assert os.path.exists(os.path.join(dataset_folder, table_location))
        else:
            table_location = None

        new_source = metadata.get_segmentation_metadata(
            name, local_xml, menu_item,
            view=view, table_location=table_location
        )

    if 'remote' in xml_locations:
        remote_xml = os.path.join('images', xml_locations['remote'])
        assert os.path.exists(os.path.join(dataset_folder, remote_xml))
        new_source[source_type]['imageDataLocations']['remote'] = remote_xml

    return new_source


def migrate_dataset_metadata(folder):
    in_file = os.path.join(folder, 'images', 'images.json')
    assert os.path.exists(in_file), in_file
    with open(in_file, 'r') as f:
        sources_in = json.load(f)

    new_sources = {}
    for name, source in sources_in.items():
        new_sources[name] = migrate_source_metadata(name, source, folder)

    dataset_metadata = {
        "is2d": False,
        "sources": new_sources,
        "views": {}
    }
    metadata.write_dataset_metadata(folder, dataset_metadata)
    os.remove(in_file)


# TODO
def migrate_bookmark(bookmark):
    pass


def migrate_bookmark_file(bookmark_file):
    with open(bookmark_file) as f:
        bookmarks = json.load(f)

    new_bookmarks = {}
    for name, bookmark in bookmarks.items():
        new_bookmarks[name] = migrate_bookmark(bookmark)

    with open(bookmark_file, 'w') as f:
        json.dump(new_bookmarks, f, indent=2, sort_keys=True)


# TODO bookmarks from default.json go to dataset.json:views
def migrate_bookmarks(folder):
    bookmark_dir = os.path.join(folder, 'misc', 'bookmarks')
    assert os.path.exists(bookmark_dir)
    bookmark_files = glob(os.path.join(bookmark_dir, '*.json'))
    for bookmark_file in bookmark_files:
        migrate_bookmark_file(bookmark_file)


#
# migrate table functionality
#

def migrate_table(table_path):
    table = pd.read_csv(table_path, sep='\t')
    table = remove_background_label_row(table)
    out_path = table_path.replace('.csv', '.tsv')
    table.to_csv(out_path, sep='\t', index=False)
    os.remove(table_path)


def migrate_tables(folder):
    table_root = os.path.join(folder, 'tables')
    assert os.path.exists(table_root)
    table_names = os.listdir(table_root)
    for table_name in table_names:
        table_folder = os.path.join(table_root, table_name)
        table_paths = glob(os.path.join(table_folder, "*.csv"))
        for table_path in table_paths:
            migrate_table(table_path)


#
# migrate source xmls
#


def remove_authentication_field(xml):
    root = ET.parse(xml).getroot()
    # get the image loader node
    imgload = root.find('SequenceDescription').find('ImageLoader')
    # remove the field
    el = imgload.find("Authentication")
    imgload.remove(el)

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(xml)


# only need to remove "Authentication" from the remote xmls
def migrate_sources(folder):
    remote_folder = os.path.join(folder, 'images', 'remote')
    # dataset might not have remote sources
    if not os.path.exists(remote_folder):
        return
    xmls = os.path.join(remote_folder, '*.xml')
    for xml in xmls:
        remove_authentication_field(xml)


def migrate_dataset(folder):
    migrate_dataset_metadata(folder)
    return
    migrate_bookmarks(folder)
    migrate_tables(folder)
    migrate_sources(folder)
    # TODO validate
