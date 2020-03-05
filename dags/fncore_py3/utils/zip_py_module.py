# coding=utf-8
"""

Zips python module for submission to Spark

"""
import os
import shutil
import tempfile


def zip_py(module_dir='fncore'):
    """Zips python module for submission to Spark"""
    root_dir = os.path.abspath(os.path.join(module_dir, os.pardir))
    base_dir = os.path.relpath(module_dir, root_dir)

    temp_dir = tempfile.gettempdir()
    zippath = os.path.join(temp_dir, 'fn_pyspark_module_' + base_dir)

    zipped_file = shutil.make_archive(zippath, 'zip', root_dir, base_dir)
    return zipped_file
