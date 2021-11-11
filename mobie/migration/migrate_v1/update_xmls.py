import os
from glob import glob
import xml.etree.ElementTree as ET
from pybdv.metadata import indent_xml


def add_authentication_field(xml_path, anon):
    root = ET.parse(xml_path).getroot()
    loader = root.find("SequenceDescription").find("ImageLoader")
    auth_text = "Anonymous" if anon else "Protected"
    ET.SubElement(loader, "Authentication").text = auth_text

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(xml_path)


def update_xmls(folder, anon):
    xml_folder = os.path.join(folder, 'images', 'remote')
    xmls = glob(os.path.join(xml_folder, '*.xml'))
    for xml in xmls:
        add_authentication_field(xml, anon)


def update_all_xmls(root, pattern, anon):
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        update_xmls(folder, anon)
