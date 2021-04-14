import json
import os
import xml.etree.ElementTree as ET
from glob import glob

import pandas as pd
from pybdv.metadata import indent_xml, write_name

import mobie.metadata as metadata
from mobie.metadata.utils import write_metadata
from mobie.tables.utils import remove_background_label_row
from mobie.validation import validate_dataset


#
# migrate source and bookmark metadata
#


def migrate_source_metadata(name, source, dataset_folder, menu_name):
    source_type = source['type']
    xml_locations = source['storage']
    local_xml = os.path.join('images', xml_locations['local'])
    assert os.path.exists(os.path.join(dataset_folder, local_xml))

    if source_type in ('image', 'mask'):
        view = metadata.get_default_view(
            "image", name, menu_name=menu_name,
            color=source['color'], contrastLimits=source['contrastLimits']
        )
        new_source = metadata.get_image_metadata(
            name, local_xml, view=view
        )
        source_type = 'image'

    else:
        assert source_type == 'segmentation'

        seg_color = source['color']
        seg_color = 'glasbey' if seg_color == 'randomFromGlasbey' else seg_color
        view = metadata.get_default_view(
            "segmentation", name, menu_name=menu_name, lut=seg_color
        )

        if 'tableFolder' in source:
            table_location = source['tableFolder']
            assert os.path.exists(os.path.join(dataset_folder, table_location))
        else:
            table_location = None

        new_source = metadata.get_segmentation_metadata(
            name, local_xml,
            view=view, table_location=table_location
        )

    if 'remote' in xml_locations:
        remote_xml = os.path.join('images', xml_locations['remote'])
        assert os.path.exists(os.path.join(dataset_folder, remote_xml))
        new_source[source_type]['imageDataLocations']['s3store'] = remote_xml

    return new_source


def migrate_dataset_metadata(folder, parse_menu_name, parse_source_name):
    in_file = os.path.join(folder, 'images', 'images.json')
    assert os.path.exists(in_file), in_file
    with open(in_file, 'r') as f:
        sources_in = json.load(f)

    new_sources = {}
    for name, source in sources_in.items():
        # NOTE parse meu name needs to be called before  parse source name
        menu_name = parse_menu_name(source['type'], name)
        if parse_source_name is not None:
            name = parse_source_name(name)
        new_sources[name] = migrate_source_metadata(name, source, folder, menu_name)

    dataset_metadata = {
        "is2d": False,
        "sources": new_sources,
        "views": {}
    }
    metadata.write_dataset_metadata(folder, dataset_metadata)
    os.remove(in_file)


def migrate_bookmark(name, bookmark, all_sources, parse_source_name=None):
    menu_name = "bookmark"

    # check if we have a viewer transform in this bookmark
    affine = bookmark.pop('view', None)
    normalized_affine = bookmark.pop('normView', None)
    position = bookmark.pop('position', None)
    if normalized_affine is not None:
        # get rid of the leading "n" character in normView
        normalized_affine = [float(param[1:]) for param in normalized_affine]

    # old views allow to specify both position and view / normView, but the position doesn't
    # have any effect in that case, so we don't allow it any more.
    # hence get rid of the position if it's specified alongside a view or normView
    if position is not None and (affine is not None or normalized_affine is not None):
        position = None

    if any(trafo is not None for trafo in (affine, normalized_affine, position)):
        viewer_transform = metadata.get_viewer_transform(affine=affine,
                                                         normalized_affine=normalized_affine,
                                                         position=position)
    else:
        viewer_transform = None

    # layers is in bookmark -> we need to add sourceDisplays
    if "layers" in bookmark:
        layers = bookmark.pop("layers")
        names, source_types, sources, display_settings = [], [], [], []

        for source_name, settings in layers.items():

            if parse_source_name is not None:
                source_name = parse_source_name(source_name)

            this_source = all_sources[source_name]
            source_type = list(this_source.keys())[0]
            this_default_settings = this_source[source_type]['view']['sourceDisplays']

            if source_type == 'image':
                this_default_settings = this_default_settings[0]['imageDisplay']
                this_settings = {
                    'opacity': this_default_settings['opacity'],  # did not have opacity equivalent in old spec
                    'color': settings.pop('color', this_default_settings['color']),
                    'contrastLimits': settings.pop('contrastLimits', this_default_settings['contrastLimits'])
                }

            else:  # source_type == 'segmentation'
                this_default_settings = this_default_settings[0]['segmentationDisplay']

                lut = settings.pop('color', this_default_settings['lut'])
                if lut == 'randomFromGlasbey':
                    lut = 'glasbey'

                this_settings = {
                    'opacity': this_default_settings['opacity'],  # did not have opacity equivalent in old spec
                    'lut': lut
                }

                # optional keys that don't need any translation
                optional_keys = ("colorByColumn", "showSelectedSegmentsIn3d", "tables")
                for key in optional_keys:
                    val = settings.pop(key, None)
                    if val is not None:
                        this_settings[key] = val

                # selected segment id entry format is "<source-name>;<timepoint>;<label-id>"
                selected_ids = settings.pop("selectedLabelIds", None)
                if selected_ids is not None:
                    selected_ids = [f"{source_name};0;{sid}" for sid in selected_ids]
                    this_settings["selectedSegmentIds"] = selected_ids

                # segmentation might have contrastLimits, which we can just ignore
                settings.pop('contrastLimits', None)

            assert not settings, f"Not all settings fields were parsed: {list(settings.keys())}"
            names.append(source_name)
            sources.append([source_name])
            source_types.append(source_type)
            display_settings.append(this_settings)

        view = metadata.get_view(names, source_types, sources, display_settings,
                                 is_exclusive=True, menu_name=menu_name,
                                 viewer_transform=viewer_transform)

    # otherwise, we don't have sources and require a viewerTransform
    else:
        assert viewer_transform is not None
        view = {
            'uiSelectionGroup': menu_name,
            'isExclusive': False,
            'viewerTransform': viewer_transform
        }

    assert not bookmark, f"Not all bookmark fields were parsed: {list(bookmark.keys())}"
    return view


def migrate_bookmark_file(bookmark_file, dataset_folder, is_default=False,
                          parse_source_name=None):
    dataset_metadata = metadata.read_dataset_metadata(dataset_folder)
    all_sources = dataset_metadata['sources']

    with open(bookmark_file) as f:
        bookmarks = json.load(f)

    new_bookmarks = {}
    for name, bookmark in bookmarks.items():
        new_bookmarks[name] = migrate_bookmark(name, bookmark, all_sources,
                                               parse_source_name=parse_source_name)

    if is_default:
        dataset_metadata['views'] = new_bookmarks
        metadata.write_dataset_metadata(dataset_folder, dataset_metadata)
    else:
        write_metadata(bookmark_file, {'bookmarks': new_bookmarks})


def migrate_bookmarks(folder, parse_source_name):
    bookmark_dir = os.path.join(folder, 'misc', 'bookmarks')
    assert os.path.exists(bookmark_dir), bookmark_dir
    default_bookmark_file = os.path.join(bookmark_dir, 'default.json')

    assert os.path.exists(default_bookmark_file), default_bookmark_file
    migrate_bookmark_file(default_bookmark_file, folder, is_default=True,
                          parse_source_name=parse_source_name)
    os.remove(default_bookmark_file)

    bookmark_files = glob(os.path.join(bookmark_dir, '*.json'))
    for bookmark_file in bookmark_files:
        migrate_bookmark_file(bookmark_file, folder,
                              parse_source_name=parse_source_name)


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
    # we might not have tables
    if not os.path.exists(table_root):
        return
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


# write the source name into the xml
# remove "Authentication" from the remote xmls
def migrate_sources(folder):
    sources = metadata.read_dataset_metadata(folder)['sources']
    for source_name, source in sources.items():
        source_type = list(source.keys())[0]
        storage = source[source_type]['imageDataLocations']
        for storage_type, loc in storage.items():
            xml = os.path.join(folder, loc)
            write_name(xml, 0, source_name)
            if storage_type == 'remote':
                remove_authentication_field(xml)


def default_menu_name_parser(source_type, source_name):
    return f"{source_type}s"


def migrate_dataset(folder, parse_menu_name=None, parse_source_name=None):
    """Migrate dataset to spec version 0.2

    Arguments:
        folder [str] - dataset folder
        parse_menu_name [callable] -
        parse_source_name [callable] -
    """
    if parse_menu_name is None:
        parse_menu_name = default_menu_name_parser
    migrate_dataset_metadata(folder, parse_menu_name, parse_source_name)
    migrate_bookmarks(folder, parse_source_name)
    migrate_tables(folder)
    migrate_sources(folder)
    validate_dataset(folder)
