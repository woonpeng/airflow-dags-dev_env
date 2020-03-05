# coding=utf-8
"""

Neo4J context manager

"""
import logging
from contextlib import contextmanager

from neo4j.v1 import (AddressError, GraphDatabase, ProtocolError,
                      SecurityError, ServiceUnavailable, basic_auth)

from fncore.utils.connstr import decode_url_connection_string


@contextmanager
def get_neo4j_context(uri):
    """Get the Neo4J context manager"""
    neo4j_context = None
    try:
        # Set neo4j bolt logging level to WARNING
        log = logging.getLogger("neo4j.bolt")
        log.setLevel(logging.WARNING)

        # Get the session
        decoded = decode_url_connection_string(uri)
        auth = basic_auth(decoded['username'], decoded['password'])
        driver = GraphDatabase.driver(uri, auth=auth)
        neo4j_context = driver.session()
        yield neo4j_context
    except (AddressError, ProtocolError,
            ServiceUnavailable, SecurityError) as graph_exception:
        raise graph_exception
    finally:
        if neo4j_context:
            # Perform any clean ups, currently none
            neo4j_context.close()
            neo4j_context = None
