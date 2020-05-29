import argparse

from mobie.migration.make_leveled_view import make_leveled_views
from mobie.migration.migrate_bookmarks import migrate_all_bookmakrs
from mobie.migration.migrate_image_dicts import migrate_all_image_dicts
from mobie.migration.remove_additional_tables_file import remove_additional_tables_file
from mobie.migration.update_xmls import update_all_xmls
from mobie.migration.versions_to_datasets import versions_to_datasets


def migrate_to_mobie(root, pattern, anon, normal_vector=None):

    if normal_vector is not None:
        make_leveled_views(root, pattern, normal_vector)

    migrate_all_bookmakrs(root, pattern)

    migrate_all_image_dicts(root, pattern)

    remove_table_files(root, pattern)

    update_all_xmls(root, pattern, anon)

    versions_to_datasets(root)


# TODO pass the normal vector
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('root', type=str)
    parser.add_argument('pattern', type=str)
    parser.add_argument('--anon', type=int, default=1)

    args = parser.parse_args()
    migrate_to_mobie(args.root, args.pattern, bool(args.anon))
