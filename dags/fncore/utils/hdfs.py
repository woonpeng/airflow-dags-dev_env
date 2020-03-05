"""Utilities for working with hdfs"""

import os
import subprocess


def get_hdfs_info(graph_spec):
    """
    Extract HDFS info from environment variable and graph spec, such as the
    data format and the location of the node and edge lists.
    """
    data_path = os.environ['PIPELINE_DATA_PATH']
    data_format = os.environ['PIPELINE_DATA_FORMAT']

    graph_name = graph_spec['name']
    graph_data_path = {
        'tables': os.path.join(
            data_path, graph_name, 'tables'),
        'node_list': os.path.join(
            data_path, graph_name, 'node_list'),
        'node_list_resolved': os.path.join(
            data_path, graph_name, 'node_list_resolved'),
        'edge_list': os.path.join(
            data_path, graph_name, 'edge_list'),
        'edge_list_resolved': os.path.join(
            data_path, graph_name, 'edge_list_resolved')
    }

    return data_format, graph_data_path


def check_file_exists_hdfs(path):
    """
    Checks whether file exists.

    :param path: path to file in hdfs
    :type path: str
    :return: indicates whether file exists
    :rtype: bool
    """
    prs = subprocess.Popen(
        ['hadoop', 'fs', '-test', '-e', path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    prs.communicate()

    if prs.returncode is None:
        raise IOError("Unable to determine whether file "
                      "'{}' exists.".format(path))

    return prs.returncode == 0
