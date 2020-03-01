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


def write_simple_xml(xml_path, data_path, path_type='absolute'):
    # write top-level data
    root = ET.Element('SpimData')
    root.set('version', '0.2')
    bp = ET.SubElement(root, 'BasePath')
    bp.set('type', 'relative')
    bp.text = '.'

    seqdesc = ET.SubElement(root, 'SequenceDescription')
    imgload = ET.SubElement(seqdesc, 'ImageLoader')
    imgload.set('format', 'bdv.hdf5')
    el = ET.SubElement(imgload, 'hdf5')
    el.set('type', path_type)
    el.text = data_path

    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(xml_path)


# should be generalized and moved to pybdv at some point
def write_s3_xml(in_xml, out_xml, path_in_bucket,
                 region='us-west-2',
                 service_endpoint='https://s3.embl.de',
                 bucket_name='platybrowser',
                 shape=None, resolution=None):
    nt = 1

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
    bdv_dtype = 'bdv.n5.s3'
    imgload.set('format', bdv_dtype)
    el = ET.SubElement(imgload, 'Key')
    el.text = path_in_bucket

    el = ET.SubElement(imgload, 'SigningRegion')
    el.text = region
    el = ET.SubElement(imgload, 'ServiceEndpoint')
    el.text = service_endpoint
    el = ET.SubElement(imgload, 'BucketName')
    el.text = bucket_name

    # load the view descriptions
    viewsets = seqdesc.find('ViewSetups')
    vs = viewsets.find('ViewSetup')

    oz, oy, ox = 0.0, 0.0, 0.0
    # if resolution is not None, write it, otherwise read it
    vox = vs.find('voxelSize')
    if resolution is None:
        resolution = vox.find('size').text
        resolution = [float(res) for res in resolution.split()][::-1]
        dz, dy, dx = resolution
    else:
        dz, dy, dx = resolution
        voxs = vox.find('size')
        voxs.text = '{} {} {}'.format(dx, dy, dz)

    # write the shape if it is not None
    if shape is not None:
        nz, ny, nx = tuple(shape)
        vss = vs.find('size')
        vss.text = '{} {} {}'.format(nx, ny, nz)

    # load the registration description and write the affines
    vregs = root.find('ViewRegistrations')
    for t in range(nt):
        vreg = vregs.find('ViewRegistration')
        vt = vreg.find('ViewTransform')
        vt.set('type', 'affine')
        vta = vt.find('affine')
        vta.text = '{} 0.0 0.0 {} 0.0 {} 0.0 {} 0.0 0.0 {} {}'.format(dx, ox,
                                                                      dy, oy,
                                                                      dz, oz)
    indent_xml(root)
    tree = ET.ElementTree(root)
    tree.write(out_xml)


def read_path_in_bucket(xml):
    root = ET.parse(xml).getroot()
    seqdesc = root.find('SequenceDescription')
    imgload = seqdesc.find('ImageLoader')
    el = imgload.find('Key')
    return el.text
