import os


def write_additional_table_file(table_folder):
    # get all the file names in the table folder
    file_names = os.listdir(table_folder)
    file_names.sort()

    # make sure we have the default table
    default_name = 'default.csv'
    if default_name not in file_names:
        raise RuntimeError("Did not find the default table ('default.csv') in the table folder %s" % table_folder)

    # don't write anything if we don't have additional tables
    if len(file_names) == 1:
        return

    # write file for the additional tables
    out_file = os.path.join(table_folder, 'additional_tables.txt')
    with open(out_file, 'w') as f:
        for name in file_names:
            ext = os.path.splitext(name)[1]
            # only add csv files
            if ext != '.csv':
                continue
            # don't add the default table
            if name == 'default.csv':
                continue
            f.write(name + '\n')
