

def remove_background_label_row(table):
    table = table.drop(axis='index', labels=0)
    return table
