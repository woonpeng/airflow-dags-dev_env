# coding=utf-8
"""
This module defines the airflow function to purge and backup and restore the
Finnet Neo4J database. This is necessary to ensure that existing data in the
neo4j database is wiped, and the insertion of the data into the database will
result in a consistent state. These management operations executed on the
server-side using the following web-hook endpoints:
https://github.com/datagovsg/finnet-neo4j/blob/develop/README.md

The pipeline a limitation in that we only allow for one graph database instance
despite the other stages in the pipeline allowing for multiple graph
specifications.

We try to overcome this by using multiple neo4j databases for each graph
specification, and have the specification store the database connection
information.
"""
# pylint: disable=unused-argument
import logging
import os

from fncore.utils.ssh_manager import (get_ssh_session, run_script,
                                      use_temporary_file)


def backup(graph_specification, execution_date, username=None,
           port=None, **kwargs):
    """
    Backup the existing neo4j database, with the `execution_date`
    as the name of the backup.
    This is used as an airflow task

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param execution_date: The given date and time of execution
    :type execution_date: datetime.datetime
    :param kwargs: Airflow context
    :type kwargs: dict
    :return: Does not return anything
    :rtype: None
    """

    retcode = -1
    with get_ssh_session(graph_specification.graph_uri,
                         port=port,
                         username=username) as session:

        retcode, out, _ = run_script(session, 'manager.sh backup')

        logging.info('Backup via SSH response: ' + str(retcode))
        logging.info('\n'.join(out))

    assert retcode == 0


def purge(graph_specification, username=None, port=None):
    """
    Purge the existing neo4j database.
    This is used as an airflow task

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :return: Does not return anything
    :rtype: None
    """
    retcode = -1
    with get_ssh_session(graph_specification.graph_uri,
                         port=port,
                         username=username) as session:
        retcode, out, _ = run_script(session, 'manager.sh purge')

        logging.info('Purge via SSH response: ' + str(retcode))
        logging.info('\n'.join(out))

    assert retcode == 0


def restore(graph_specification, execution_date, username=None,
            port=None, **kwargs):
    """
    Restore the neo4j database from a backup file, with the `execution_date`
    as the name of the backup.
    This is used as an airflow task

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param execution_date: The given date and time of execution
    :type execution_date: datetime.datetime
    :param kwargs: Airflow context
    :type kwargs: dict
    :return: Does not return anything
    :rtype: None
    """
    cmd = 'manager.sh restore {}'.format(execution_date.date().isoformat())
    retcode = -1

    with get_ssh_session(graph_specification.graph_uri,
                         port=port,
                         username=username) as session:
        retcode, out, _ = run_script(session, cmd)

        logging.info('Restore via SSH response: ' + str(retcode))
        logging.info('\n'.join(out))

    assert retcode == 0


def cleanup(graph_specification, username=None, port=None):
    """
    Build the neo4j database from a zip file, containing node and edges csv
    This is used as an airflow task

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param filename: The full path to a zip file containing the node and edge
                     csv to be built into a graph database
    :type filename: str
    :param kwargs: Airflow context
    :type kwargs: dict
    :return: Does not return anything
    :rtype: None
    """
    retcode = -1
    with get_ssh_session(graph_specification.graph_uri,
                         port=port,
                         username=username) as session:
        retcode, out, _ = run_script(session, 'manager.sh cleanup')

        logging.info('Cleanup via SSH response: ' + str(retcode))
        logging.info('\n'.join(out))

    assert retcode == 0


def build_db(graph_specification, filename, username=None, port=None, **kwargs):
    """
    Build the neo4j database from a zip file, containing node and edges csv
    This is used as an airflow task

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param filename: The full path to a zip file containing the node and edge
                     csv to be built into a graph database
    :type filename: str
    :param username: user which has ssh access to the graph database server
    :type username: str
    :param port: the port on which the graph database server provides ssh access
    :type port: int
    :param kwargs: Airflow context
    :type kwargs: dict
    :return: Does not return anything
    :rtype: None
    """

    # Get the base filename
    basefile = os.path.basename(filename)

    retcode = -1
    with get_ssh_session(graph_specification.graph_uri,
                         port=port,
                         username=username) as session:

        with use_temporary_file(session, filename) as dirpath:
            cmd = 'manager.sh builddb {}'.format(
                os.path.join(dirpath, basefile))
            retcode, out, _ = run_script(session, cmd)

            logging.info('Build database via SSH response: ' + str(retcode))
            logging.info('\n'.join(out))

    assert retcode == 0
