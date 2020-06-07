import xml.etree.ElementTree as ET
from pybdv.metadata import get_data_path, indent_xml


def copy_xml_with_abspath(xml_in, xml_out):
    path = get_data_path(xml_in, return_absolute_path=True)
    copy_xml_with_newpath(xml_in, xml_out, path,
                          path_type='absolute')


def copy_xml_with_newpath(xml_in, xml_out, data_path,
                          path_type='relative', data_format='bdv.hdf5'):
    assert path_type in ('absolute', 'relative')
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
                      authentication='Anonymous', region='us-west-2'):
    """ Copy a bdv xml file and replace the image data loader with the bdv.n5.s3 format.

    Arguments:
        in_xml [str] - path to the input xml
        out_xml [str] - path to the output xml
        service_endpoint [str] - url of the s3 service end-point.
            For EMBL: 'https://s3.embl.de'.
        bucket_name [str] - name of the bucket
        path_in_bucket [str] - file paths inside of the bucket
        authentication [str] - the authentication mode, can be 'Anonymous' or 'Protected'.
            Default: 'Anonymous'
        region [str] - the region. Only relevant if aws.s3 is used.
            Default: 'us-west-2'
    """

    auth_modes = ('Anonymous', 'Protected')
    if authentication not in auth_modes:
        raise ValueError(f"Invalid authentication mode {authentication}, expected one of {auth_modes}")

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
    bdv_type = 'bdv.n5.s3'
    imgload.set('format', bdv_type)
    el = ET.SubElement(imgload, 'Key')
    el.text = path_in_bucket

    el = ET.SubElement(imgload, 'SigningRegion')
    el.text = region
    el = ET.SubElement(imgload, 'ServiceEndpoint')
    el.text = service_endpoint
    el = ET.SubElement(imgload, 'BucketName')
    el.text = bucket_name
    el = ET.SubElement(imgload, 'Authentication')
    el.text = authentication

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(out_xml)


def read_path_in_bucket(xml):
    root = ET.parse(xml).getroot()
    seqdesc = root.find('SequenceDescription')
    imgload = seqdesc.find('ImageLoader')
    el = imgload.find('Key')
    return el.text
