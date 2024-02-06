"""Utility module for importing files"""
def import_file(file_path, file_name):
    f = open(file_path + file_name, 'r')
    data = f.read()

    f.close()

    return data
