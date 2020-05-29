import os
from glob import glob
from subprocess import run


def remove_table_files(root, pattern):
    folders = glob(os.path.join(root, pattern))
    for folder in folders:
        table_root = os.path.join(folder, 'tables')
        table_folders = glob(os.path.join(table_root, '*'))
        for table_folder in table_folders:
            table_file = os.path.join(table_folder, 'additional_tables.txt')
            if os.path.exists(table_file):
                run(['git', 'rm', table_file])
