"""
Neo4j sensor operator to check if the specified neo4j server can be connected
"""

# The location of urlparse varies between Python 2 and 3
try:
    from urllib.parse import unquote, urlparse
except ImportError:
    from urlparse import unquote, urlparse
import logging
import socket

from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from neo4j.v1 import GraphDatabase, basic_auth


# pylint: disable=too-few-public-methods
class Neo4jSensorOperator(BaseSensorOperator):
    """
    Try to open a session with the Neo4j server to ensure that the server is
    accepting connections on the specified address and protocol. It will keep
    trying until timeout

    :param conn_str: Connection string to the Neo4j server
    :type conn_str: string
    :param timeout: Time (s) in which the connection must be established
    :type timeout: int
    """

    @apply_defaults
    def __init__(self, conn_str, timeout, *args, **kwargs):
        self.conn_str = conn_str
        self.conn_timeout = timeout
        self.timeout = 2 * timeout # to ensure that there's time to complete
        parse_result = urlparse(conn_str)
        username = unquote(parse_result.username)
        password = unquote(parse_result.password)
        self.auth = basic_auth(username, password)
        super(Neo4jSensorOperator, self).__init__(*args, **kwargs)

    # pylint: disable=broad-except
    # pylint: disable=unused-argument
    def poke(self, context):
        """ Try to connect to the neo4j server """

        # Set neo4j bolt logging level to WARNING to avoid logging down
        # the credentials
        log = logging.getLogger("neo4j.bolt")
        log.setLevel(logging.WARNING)

        defaulttimeout = socket.getdefaulttimeout()
        try:
            socket.setdefaulttimeout(self.conn_timeout)
            with GraphDatabase.driver(self.conn_str, auth=self.auth) as driver:
                with driver.session():
                    pass
            return True
        except Exception:
            return False
        finally:
            socket.setdefaulttimeout(defaulttimeout)
