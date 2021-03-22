from .make_leveled_view import make_leveled_views
from .migrate_bookmarks import migrate_all_bookmakrs
from .migrate_image_dicts import migrate_all_image_dicts
from .remove_additional_tables_file import remove_table_files
from .update_xmls import update_all_xmls
from .versions_to_datasets import versions_to_datasets


def migrate_to_mobie(root, pattern, anon, normal_vector=None):
    if normal_vector is not None:
        make_leveled_views(root, pattern, normal_vector)
    migrate_all_bookmakrs(root, pattern)
    migrate_all_image_dicts(root, pattern)
    remove_table_files(root, pattern)
    update_all_xmls(root, pattern, anon)
    versions_to_datasets(root)
