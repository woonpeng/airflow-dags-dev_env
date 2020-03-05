# coding=utf-8
"""

Parse URL connection string into its useful components

"""
import os

# The location of urlparse varies between Python 2 and 3
try:
    from urllib.parse import unquote, urlparse
except ImportError:
    from urlparse import unquote, urlparse


def __split_db_and_table_path(path):
    """Splits URL path to parts"""
    all_parts = []
    while True:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            if parts[0] != '/':
                all_parts.insert(0, parts[0])
            break
        elif parts[1] == path:  # sentinel for relative paths
            all_parts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            all_parts.insert(0, parts[1])
    return all_parts


def decode_url_connection_string(conn_str):
    """URL connection string into more useful parts"""
    parse_result = urlparse(conn_str)

    username = parse_result.username
    password = parse_result.password
    hostname = parse_result.hostname
    decoded = {
        'scheme': parse_result.scheme,
        'username': unquote(username) if username else username,
        'password': unquote(password) if password else password,
        'hostname': unquote(hostname) if hostname else hostname,
        'port': parse_result.port,
        'database': __split_db_and_table_path(parse_result.path)[0]
    }
    return decoded


def encode_db_connstr(name,  # pylint: disable=too-many-arguments
                      host,
                      port,
                      user,
                      password,
                      scheme):
    """ builds a database connection string """
    template = '{scheme}://{user}:{password}@{host}:{port}/{name}'
    return template.format(scheme=scheme,
                           user=user,
                           password=password,
                           host=host,
                           port=port,
                           name=name)
