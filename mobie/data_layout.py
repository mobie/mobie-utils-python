import os
import json
import warnings
from subprocess import check_output

from .files import copy_version_folder_helper


def make_version_folder(version_folder):
    """ Make the folder structure for a version.
    """
    os.makedirs(os.path.join(version_folder, 'images', 'local'), exist_ok=True)
    os.makedirs(os.path.join(version_folder, 'images', 'remote'), exist_ok=True)
    os.makedirs(os.path.join(version_folder, 'misc'), exist_ok=True)
    os.makedirs(os.path.join(version_folder, 'tables'), exist_ok=True)


def make_initial_layout(root, initial_version_name=None):
    """ Create initial folder layout for the MMB.

    Arguments:
        root [str] - root data folder
        iniital_version_name [str] - name for initial version, defaults to 0.1.0 (default: None)
    """
    os.makedirs(root, exist_ok=True)

    if initial_version_name is None:
        initial_version_name = '0.1.0'

    # make the initial version folder
    version_folder = os.path.join(root, initial_version_name)
    make_version_folder(version_folder)
    image_folder = os.path.join(version_folder, 'images')
    misc_folder = os.path.join(version_folder, 'misc')

    # dump empty image dict
    with open(os.path.join(image_folder, 'images.json'), 'w') as f:
        json.dump({}, f)

    # dump empty bookmark dict
    with open(os.path.join(misc_folder, 'bookmarks.json'), 'w') as f:
        json.dump({}, f)

    # make the version file
    version_file = os.path.join(root, 'versions.json')
    with open(version_file, 'w') as f:
        json.dump([initial_version_name], f)


def copy_version_folder(root, src_version, dst_version):
    """
    """
    version_file = os.path.join(root, 'versions.json')
    with open(version_file) as f:
        versions = json.load(f)

    if src_version not in versions:
        raise ValueError("Could not find src version %s" % src_version)
    if dst_version in versions:
        raise ValueError("Dst version %s already exists" % dst_version)

    src_folder = os.path.join(root, src_version)
    dst_folder = os.path.join(root, dst_version)
    make_version_folder(dst_folder)

    copy_version_folder_helper(src_folder, dst_folder)


def get_version(root, enforce_version_consistency=False):
    """ Get latest version.
    """
    version_file = os.path.join(root, 'versions.json')
    git_tag = check_output(['git', 'describe', '--abbrev=0']).decode('utf-8').rstrip('\n')
    with open(version_file) as f:
        versions = json.load(f)
    version = versions[-1]

    msg = "Git version %s and version from versions.json %s do not agree" % (git_tag, version)
    if version != git_tag and enforce_version_consistency:
        raise RuntimeError(msg)
    elif version != git_tag:
        warnings.warn(msg)

    return version
