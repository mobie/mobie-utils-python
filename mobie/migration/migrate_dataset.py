import argparse
import os
from .make_leveled_view import make_leveled_view
from .migrate_bookmarks import migrate_bookmarks
from .migrate_image_dicts import migrate_image_dict
from .remove_additional_tables_file import remove_for_single_dataset
from .update_xmls import update_xmls


def migrate_dataset_to_mobie(folder, anon, normal_vector=None):

    if normal_vector is not None:
        make_leveled_view(folder, normal_vector)

    migrate_bookmarks(os.path.join(folder, 'misc'))

    migrate_image_dict(os.path.join(folder, 'images', 'images.json'))

    remove_for_single_dataset(folder)

    update_xmls(folder, anon)


# TODO pass the normal vector
def main():
    parser = argparse.ArgumentParser(description="Migrate old platybrowser dataset to new MoBIE dataset.")
    parser.add_argument('folder', type=str)
    parser.add_argument('--anon', type=int, default=1)

    args = parser.parse_args()
    migrate_dataset_to_mobie(args.folder, bool(args.anon))


if __name__ == '__main__':
    main()
