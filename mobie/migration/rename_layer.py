import argparse
import json
import os
import subprocess
import xml.etree.ElementTree as ET
from glob import glob

from pybdv.metadata import indent_xml


def update_im_dict(im_folder, old_name, new_name):

    def update_val(val):
        storage = val['storage']
        storage = {k: v.replace(old_name, new_name) for k, v in storage.items()}

        if 'tableFolder' in val:
            val['tableFolder'] = val['tableFolder'].replace(old_name, new_name)

        return storage

    im_dict_file = os.path.join(im_folder, 'images.json')
    with open(im_dict_file) as f:
        im_dict = json.load(f)

    if old_name in im_dict:
        val = im_dict.pop(old_name)
        im_dict[new_name] = update_val(val)
        with open(im_dict_file, 'w') as f:
            json.dump(im_dict, f, imdent=2, sort_keys=True)


def update_xmls(im_folder, old_name, new_name):
    folders = ['local', 'remote']
    for folder_name in folders:
        folder = os.path.join(im_folder, folder_name)
        if not os.path.exists(folder):
            continue
        xml_pattern = os.path.join(folder, '*.xml')
        xmls = glob(xml_pattern)
        xmls = [xml for xml in xmls if old_name in xml]
        for old_path in xmls:
            new_path = old_path.replace(old_name, new_name)

            tree = ET.parse(old_path)
            root = tree.getroot()

            img_loader = root.find('SequenceDescription').find('ImageLoader')
            node = img_loader.find('n5')
            node.text = node.text.replace(old_name, new_name)

            indent_xml(root)
            tree = ET.ElementTree(root)
            tree.write(new_path)

            subprocess.run(['git', 'rm', old_path])


def update_table_folder(folder, old_name, new_name):
    old_table_folder = os.path.join(folder, old_name)
    if os.path.exists(old_table_folder):
        new_table_folder = old_table_folder.replace(new_name, old_name)
        subprocess.run(['git', 'mv', old_table_folder, new_table_folder])


def rename_layer(folder, old_name, new_name):
    im_folder = os.path.join(folder, 'images')
    update_im_dict(im_folder, old_name, new_name)
    update_xmls(im_folder, old_name, new_name)
    update_table_folder(folder, old_name, new_name)


def rename_layers(root, pattern, old_name, new_name):
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        rename_layer(folder, old_name, new_name)


def main():
    parser = argparse.ArgumentParser(description="Rename a layer in all datasets")
    parser.add_argument('root', type=str)
    parser.add_argument('pattern', type=str)
    parser.add_argument('old_name', type=str)
    parser.add_argument('new_name', type=str)

    args = parser.parse_args()
    rename_layers(args.root, args.pattern, args.old_name, args.new_name)


if __name__ == '__main__':
    main()
