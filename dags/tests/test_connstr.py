"""Unit testing of connstr module."""
from fncore.utils.connstr import (decode_url_connection_string,
                                  encode_db_connstr)


def test_decode_connection_string():
    """Unit test connstr.py module for decoding of correct conn. string"""
    split_conn_str = decode_url_connection_string(
        'postgresql://my_user:my_p@ssword@my_hostname.com:1234/my_database'
    )
    assert split_conn_str['scheme'] == 'postgresql'
    assert split_conn_str['username'] == 'my_user'
    assert split_conn_str['password'] == 'my_p@ssword'
    assert split_conn_str['hostname'] == 'my_hostname.com'
    assert split_conn_str['port'] == 1234
    assert split_conn_str['database'] == 'my_database'


def test_decode_connection_string_2():
    """Unit test connstr.py module for decoding containing '@' in password"""
    split_conn_str = decode_url_connection_string(
        'sqlserver://sa:Passwor@@@123@@localhost:1433/acra_dump'
    )
    assert split_conn_str['scheme'] == 'sqlserver'
    assert split_conn_str['username'] == 'sa'
    assert split_conn_str['password'] == 'Passwor@@@123@'
    assert split_conn_str['hostname'] == 'localhost'
    assert split_conn_str['port'] == 1433
    assert split_conn_str['database'] == 'acra_dump'


def test_decode_connection_string_3():
    """Unit test connstr.py module for decoding of incorrect string"""
    split_conn_str = decode_url_connection_string('mysql://test_db')
    assert split_conn_str['scheme'] == 'mysql'
    assert not split_conn_str['username']
    assert not split_conn_str['password']
    assert split_conn_str['hostname'] == 'test_db'
    assert not split_conn_str['port']
    assert not split_conn_str['database']


def test_decode_connection_string_4():
    """
    Unit test connstr.py module for decoding of conn. string with encoded
    password
    """
    split_conn_str = decode_url_connection_string(
        'neo4j://my_user:%40h4%5Blft@my_hostname.com:1234'
    )
    assert split_conn_str['scheme'] == 'neo4j'
    assert split_conn_str['username'] == 'my_user'
    assert split_conn_str['password'] == '@h4[lft'
    assert split_conn_str['hostname'] == 'my_hostname.com'
    assert split_conn_str['port'] == 1234
    assert not split_conn_str['database']


def test_encode_db_connstr():
    """ Unit test constr.py module for encoding db connection string """
    constructed = encode_db_connstr(name='my_database',
                                    host='127.0.0.1',
                                    port=5432,
                                    user='postgres',
                                    password='password',
                                    scheme='postgresql')
    assert constructed == (
        'postgresql://postgres:password@127.0.0.1:5432/my_database'
    )
