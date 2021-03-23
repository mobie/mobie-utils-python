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


# should be generalized and moved to pybdv at some point
def copy_xml_as_n5_s3(in_xml, out_xml,
                      service_endpoint, bucket_name, path_in_bucket,
                      region='us-west-2', bdv_type='bdv.n5.s3'):
    """ Copy a bdv xml file and replace the image data loader with the bdv.n5.s3 format.

    Arguments:
        in_xml [str] - path to the input xml
        out_xml [str] - path to the output xml
        service_endpoint [str] - url of the s3 service end-point.
            For EMBL: 'https://s3.embl.de'.
        bucket_name [str] - name of the bucket
        path_in_bucket [str] - file paths inside of the bucket
        region [str] - the region. Only relevant if aws.s3 is used.
            Default: 'us-west-2'
    """
    bdv_types = ('bdv.n5.s3', 'ome.zarr.s3')
    if bdv_type not in bdv_types:
        raise ValueError(f"Invalid bdv type {bdv_type}, expected one of {bdv_types}")

    # check if we have an xml already
    tree = ET.parse(in_xml)
    root = tree.getroot()

    # load the sequence description
    seqdesc = root.find('SequenceDescription')

    # update the image loader
    # remove the old image loader
    imgload = seqdesc.find('ImageLoader')
    seqdesc.remove(imgload)

    # write the new image loader
    imgload = ET.SubElement(seqdesc, 'ImageLoader')
    imgload.set('format', bdv_type)
    el = ET.SubElement(imgload, 'Key')
    el.text = path_in_bucket

    el = ET.SubElement(imgload, 'SigningRegion')
    el.text = region
    el = ET.SubElement(imgload, 'ServiceEndpoint')
    el.text = service_endpoint
    el = ET.SubElement(imgload, 'BucketName')
    el.text = bucket_name

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(out_xml)


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
