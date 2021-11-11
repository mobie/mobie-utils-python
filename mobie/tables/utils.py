

def remove_background_label_row(table):
    if table['label_id'].values[0] == 0:
        table = table.drop(axis='index', labels=0)
    return table
