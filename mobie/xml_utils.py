import os
import xml.etree.ElementTree as ET
from pybdv.metadata import get_data_path, indent_xml, get_bdv_format
from pybdv.metadata import write_affine


def copy_xml_with_abspath(xml_in, xml_out):
    path = get_data_path(xml_in, return_absolute_path=True)
    copy_xml_with_newpath(xml_in, xml_out, path,
                          path_type='absolute')


def copy_xml_with_relpath(xml_in, xml_out):
    path = get_data_path(xml_in, return_absolute_path=True)
    xml_root = os.path.split(xml_out)[0]
    path = os.path.relpath(path, xml_root)
    copy_xml_with_newpath(xml_in, xml_out, path,
                          path_type='relative')


def copy_xml_with_newpath(xml_in, xml_out, data_path,
                          path_type='relative', data_format=None):
    assert path_type in ('absolute', 'relative')

    if data_format is None:
        data_format = get_bdv_format(xml_in)

    # get the path node inn the xml tree
    root = ET.parse(xml_in).getroot()
    seqdesc = root.find('SequenceDescription')
    imgload = seqdesc.find('ImageLoader')
    imgload.set('format', data_format)
    et = imgload.find('hdf5')
    if et is None:
        et = imgload.find('n5')
    if et is None:
        raise RuntimeError("Could not find data node")
    et.tag = data_format.split('.')[-1]
    et.text = data_path
    et.set('type', path_type)

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(xml_out)


def add_s3_to_xml(xml, path_in_bucket,
                  service_endpoint=None, bucket_name=None, region=None):
    """ Copy a bdv xml file and replace the image data loader with the bdv.n5.s3 format.

    Arguments:
        xml [str] - path to the xml file
        path_in_bucket [str] - file paths inside of the bucket
        service_endpoint [str] - url of the s3 service end-point.
            By default the s3 root for the project will be used (default: None)
        bucket_name [str] - name of the bucket. By default the s3 root for the project will be used (default: None)
        region [str] - the region. Only relevant if aws.s3 is used.
            By default the s3 root for the project will be used (default: None)
    """

    # check if we have an xml already
    tree = ET.parse(xml)
    root = tree.getroot()

    # load the sequence description
    seqdesc = root.find('SequenceDescription')

    # add the s3 imageloader
    s3_imgload = ET.SubElement(seqdesc, 'S3ImageLoader')
    s3_imgload.set('format', 'bdv.n5.s3')
    el = ET.SubElement(s3_imgload, 'Key')
    el.text = path_in_bucket

    if region is not None:
        el = ET.SubElement(s3_imgload, 'SigningRegion')
        el.text = region
    if service_endpoint is not None:
        el = ET.SubElement(s3_imgload, 'ServiceEndpoint')
        el.text = service_endpoint
    if bucket_name is not None:
        el = ET.SubElement(s3_imgload, 'BucketName')
        el.text = bucket_name

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(xml)


def read_path_in_bucket(xml):
    root = ET.parse(xml).getroot()
    seqdesc = root.find('SequenceDescription')
    imgload = seqdesc.find('ImageLoader')
    el = imgload.find('Key')
    return el.text


def update_transformation_parameter(xml_path, parameter):
    if len(parameter) != 12:
        raise ValueError("Expected affine transformation with 12 parameters, got {len(parameter)}")
    write_affine(xml_path, setup_id=0, affine=parameter,
                 overwrite=True, timepoint=0)
