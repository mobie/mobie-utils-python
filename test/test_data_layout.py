import os
import unittest
from shutil import rmtree


class TestDataLayout(unittest.TestCase):
    root = './tmp'

    def tearDown(self):
        try:
            rmtree(self.root)
        except OSError:
            pass

    # TODO more checks
    def check_version_folder(self, version_folder):
        self.assertTrue(os.path.exists(version_folder))
        image_folder = os.path.join(version_folder, 'images')
        self.assertTrue(os.path.exists(image_folder))
        self.assertTrue(os.path.exists(os.path.join(version_folder, 'misc')))
        self.assertTrue(os.path.exists(os.path.join(version_folder, 'tables')))

    def test_make_initial_layout(self):
        from mmb.data_layout import make_initial_layout
        make_initial_layout(self.root)
        version_folder = os.path.join(self.root, '0.1.0')
        self.check_version_folder(version_folder)

    def test_copy_version_folder(self):
        from mmb.data_layout import copy_version_folder, make_initial_layout
        make_initial_layout(self.root)
        v1 = '0.1.0'
        vfolder1 = os.path.join(self.root, v1)
        self.check_version_folder(vfolder1)
        v2 = '0.1.1'
        copy_version_folder(self.root, v1, v2)
        vfolder2 = os.path.join(self.root, v2)
        self.check_version_folder(vfolder2)


if __name__ == '__main__':
    unittest.main()
