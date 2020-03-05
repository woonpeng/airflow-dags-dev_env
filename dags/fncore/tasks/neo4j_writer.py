# coding=utf-8
"""
This module defines the airflow function to insert the data into the neo4j
database.

For each node list, the graph specification is parsed to identify which fields
to hide (not to insert into the database), which fields that should be indexed
for searching (and is replicated as a `_label` field), and the corresponding
friendly names for the fields (as the data may contain non-readable names). The
primary key field is also replicated as `_node_id` field. `_node_id` and
`_label` together are used in searching for the suggestions for the front end.

A label `_searchable` for the nodes is created to enable searching across
different types of entities. Correspondingly, the `_canonical_id` field is
indexed and constrained to be unique with respect to the `_searchable` label.
Similarly, the `_node_id` and `_label` fields are also indexed to enable for
faster searching.

The nodes are keyed by the `_canonical_id` field, while the edges /
relationships are keyed by `_canonical_id_source`, `_canonical_id_target` and
the field specified by the graph specification (if any).

The database is imported into the HDFS at a location specified by the
environment variable ``PIPELINE_DATA_PATH`` and in a sub-directory
corresponding to the graph name. Within the sub-directory, it is further split
into the directories:

- `tables`: which contains the imported tables from the database
- `node_list`: which contains the tables corresponding to each node list as
  specified in the graph specification
- `edge_list`: which contains the tables corresponding to each edge list as
  specified in the graph specification
- `node_list_resolved`: which contains the tables of the node lists with
  entries referring to the same entity being resolved to have the same
  canonical id
- `edge_list_resolved`: which contains the tables of the edge lists with
  entries referring to the same entity being resolved to have the same
  canonical id

Each node / edge list is saved with an unique uuid that is specified in the
graph specification, and contains the fields as specified in the graph
specification.

Please refer to the documentation for more information regarding the graph
specifications
"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
import logging
import os

from pyspark.sql import SQLContext
from pyspark.sql.functions import upper

from fncore.utils.graph_specification import (get_fields_with_property,
                                              get_friendly_name_mapping)
from fncore.utils.hdfs import get_hdfs_info
from fncore.utils.neo4j_conf import get_neo4j_context
from fncore.utils.neo4j_tools import (commit_graphdb, create_index,
                                      create_uniqueness_constraint,
                                      generate_merge_edge_statement,
                                      generate_merge_node_statement,
                                      get_indexes)
from fncore.utils.spark_tools import get_spark_context


# pylint: disable=too-many-arguments
def push_edges(conf, tags, keyname, rows, mapping, hiddenfields, noappendfields):
    """
    Given a neo4j configurations, insert the edges specified into
    the neo4j database

    :param conf: dictionary containing the configuration parameters of the
           connection to the neo4j database
    :type conf: dict
    :param tags: a string describing the type of the edges to be inserted
    :type tags: str
    :param keyname: a string describing the property to use as primary key for
           the edge
    :type keyname: str
    :param rows: a spark dataframe row describing the edges to be inserted.
           The row must contain `_canonical_id_source` and
           `_canonical_id_target` which contains the canonical ids of the
           source and target nodes respectively
    :type rows: list(pyspark.sql.Row)
    :param mapping: a dictionary mapping from the field names (key) to the
           friendly names (value)
    :type mapping: dict
    :param hiddenfields: fields of the data that should not be inserted into
           the database
    :type hiddenfields: list
    :param noappendfields: fields of the data that will be overwritten with new
           values when the function encounters another entry of an entity that
           already exist in the database
    :type noappendfields: list
    :return: Does not return anything
    :rtype: None
    """
    # pylint: disable=too-many-locals

    # Converts the rows to a set of edges to be added into the Neo4j database
    batchsize = conf.get('max_batchsize', 500)
    maxretry = conf.get('max_retries', 5)
    statements = []

    with get_neo4j_context(conf['uri']) as neo_ctx:
        count = batchsize
        nodes_used = {}

        for row in rows:
            source_id = row['_canonical_id_source']
            target_id = row['_canonical_id_target']
            rowdict = row.asDict()
            for key, value in rowdict.items():
                rowdict[key] = unicode(value)

            statement = generate_merge_edge_statement(rowdict,
                                                      hiddenfields,
                                                      noappendfields,
                                                      mapping,
                                                      keyname,
                                                      tags)

            if source_id in nodes_used or target_id in nodes_used:
                count = batchsize
                nodes_used = {}
                commit_graphdb(neo_ctx, statements, maxretry)
                statements = []

            statements.append((rowdict, statement))
            nodes_used[source_id] = 1
            nodes_used[target_id] = 1
            count -= 1
            if count <= 0:
                count = batchsize
                nodes_used = {}
                commit_graphdb(neo_ctx, statements, maxretry)
                statements = []

        commit_graphdb(neo_ctx, statements, maxretry)


# pylint: disable=too-many-locals
def push_nodes(conf, tags, rows, mapping, hiddenfields, noappendfields):
    """
    Given a neo4j configurations, insert the nodes specified into
    the neo4j database

    :param conf: dictionary containing the configuration parameters of
           the connection to the neo4j database
    :type conf: dict
    :param tags: a list of strings describing the type of the nodes to be
           inserted
    :type tags: list
    :param rows: a spark dataframe row describing the nodes to be inserted.
           The row must contain `_canonical_id` which contains the canonical
           ids of the nodes
    :type rows: list(pyspark.sql.Row)
    :param mapping: a dictionary mapping from the field names (key) to the
           friendly names (value)
    :type mapping: dict
    :param hiddenfields: fields of the data that should not be inserted into the
           database
    :type hiddenfields: list
    :param noappendfields: fields of the data that will be overwritten with new
           values when the function encounters another entry of an entity that
           already exist in the database
    :type noappendfields: list
    :return: Does not return anything
    :rtype: None
    """

    # Assumptions
    # Configuration providing neo4j server address and
    # authentication is provided, spark sql rows will be passed in

    # Notes:
    # Possible points of failure include
    # - not able to get the context
    #   - wrong configuration
    #   - neo4j server down
    #   - host network down
    #   - authentication error
    # - tags does not exist [CHECKED]
    # - unable to start neo4j cypher transaction
    # - constrained property value does not exist [CHECKED]
    # - Node properties with spaces [CHECKED]
    # - Node properties that are numbers
    # - Empty node property name [CHECKED]
    # - Empty node property value [CHECKED]
    # - Empty tag value, or value is not a string [CHECKED]
    # - Fail to commit transaction
    #   - nodes exist and are locked for modifications

    batchsize = conf.get('max_batchsize', 500)
    maxretry = conf.get('max_retries', 5)
    statements = []
    with get_neo4j_context(conf['uri']) as neo_ctx:
        count = batchsize

        for row in rows:

            rowdict = row.asDict()
            for key, value in rowdict.items():
                rowdict[key] = unicode(value)
            statement = generate_merge_node_statement(rowdict,
                                                      hiddenfields,
                                                      noappendfields,
                                                      mapping,
                                                      tags)
            statements.append((rowdict, statement))

            count -= 1
            if count <= 0:
                count = batchsize
                commit_graphdb(neo_ctx, statements, maxretry)
                statements = []

        commit_graphdb(neo_ctx, statements, maxretry)


def write_neo4j_nodes(graph_specification, spark_config):
    """
    Given the graph specification, spark and neo4j configurations, insert the
    nodes in the data (specified in the graph specification) into the neo4j
    database. This is used as an airflow task

    :param graph_specification: graph specification in dictionary format
    :type graph_specification: dict
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :return: Does not return anything
    :rtype: None
    """
    # pylint: disable=too-many-locals
    data_format, graph_data_path = get_hdfs_info(graph_specification)

    # Use graph specification's neo4j connection
    neo_config = {
        'uri': graph_specification['graph_uri'],
        'max_retries': 5,
        'max_batchsize': 20000
    }

    with get_spark_context(spark_config.create()) as spark_ctx:
        sql_context = SQLContext(spark_ctx)

        # create and save node lists
        node_list = graph_specification.get('node_lists')
        count = 0
        if node_list:
            for node_kind in node_list:
                count += 1
                logging.info(
                    "%d/%d: %s",
                    count,
                    len(node_list),
                    node_kind['safe_name']
                )

                # Load in the node list with duplicates dropped
                # and invalid entries
                data = sql_context\
                    .read.format(data_format)\
                    .option('header', 'true')\
                    .option('inferschema', 'true')\
                    .load(os.path.join(graph_data_path['node_list_resolved'],
                                       node_kind['safe_name']))\
                    .dropna(how='any', subset=['_canonical_id'])

                # Get the friendly name mapping
                mapping = get_friendly_name_mapping(node_kind)

                # Get the hidden fields
                hiddenfields = get_fields_with_property(
                    node_kind, prop='hidden')

                # Get the labelled fields
                labelledfields = get_fields_with_property(
                    node_kind, prop='use_as_label')
                indexfields = ['_label' if k == 0 else '_label_' + str(k)
                               for k in range(len(labelledfields))]

                labelledfields.append(
                    node_kind['index_column'].get('safe_name'))
                indexfields.append('_node_id')

                # Drop invalid data in the fields that need to be indexed
                data = data.dropna(how='any', subset=labelledfields)

                # Ignore node id and label fields
                noappendfields = indexfields + [labelledfields[-1]]

                # Update the data frame to have the labels
                for oldfield, newfield in zip(labelledfields, indexfields):
                    data = data.withColumn(newfield, upper(data[oldfield]))

                # Setup the node constraints and indices on the labels
                tags = node_kind['tags'] + ['_searchable']
                with get_neo4j_context(neo_config['uri']) as neo_ctx:
                    for tag in tags:
                        create_uniqueness_constraint(neo_ctx, tag, '_canonical_id')
                    already_indexed = get_indexes(neo_ctx, '_searchable')
                    for curindex in indexfields:
                        if curindex not in already_indexed:
                            create_index(neo_ctx, '_searchable', curindex)

                data.foreachPartition(
                    lambda x, t=tags, m=mapping, h=hiddenfields, n=noappendfields:
                    push_nodes(neo_config, t, x, m, h, n)
                )


def write_neo4j_edges(graph_specification, spark_config):
    """
    Given the graph specification, spark and neo4j configurations, insert the
    edges in the data (specified in the graph specification) into the neo4j
    database. This is used as an airflow task

    :param graph_specification: graph specification in dictionary format
    :type graph_specification: dict
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :return: Does not return anything
    :rtype: None
    """
    # pylint: disable=too-many-locals
    data_format, graph_data_path = get_hdfs_info(graph_specification)

    # Use graph specification's neo4j connection
    neo_config = {
        'uri': graph_specification['graph_uri'],
        'max_retries': 5,
        'max_batchsize': 20000
    }

    with get_spark_context(spark_config.create()) as spark_ctx:
        sql_context = SQLContext(spark_ctx)

        # create and save edge lists
        edge_list = graph_specification.get('edge_lists')
        count = 0
        if edge_list:
            for edge_kind in edge_list:
                count += 1
                logging.info("# " +
                             str(count) +
                             "/" +
                             str(len(edge_list)) +
                             ": " +
                             edge_kind['safe_name'])

                # Load in the edge list with duplicates dropped
                data = sql_context\
                    .read.format(data_format)\
                    .load(os.path.join(graph_data_path['edge_list_resolved'],
                                       edge_kind['safe_name']))

                # Get the friendly name mapping
                mapping = get_friendly_name_mapping(edge_kind)

                # Get the hidden fields
                hiddenfields = get_fields_with_property(
                    edge_kind, prop='hidden')

                # Drops duplicates
                keyname = None
                keylist = ['_canonical_id_source', '_canonical_id_target']
                if 'index_column' in edge_kind:
                    keyname = edge_kind['index_column']\
                        .get('safe_name', None)
                    if keyname:
                        keylist.append(keyname)
                data = data.dropDuplicates(keylist)\
                           .dropna(how='any', subset=keylist)

                data = data.repartition(1000)
                logging.info("Count: " + str(data.count()))

                # Insert the edges into the Neo4j database
                tags = edge_kind['tags'] if 'tags' in edge_kind else 'related'
                data.foreachPartition(
                    lambda x, t=tags, key=keyname, m=mapping, h=hiddenfields, n=keylist:
                    push_edges(neo_config, t, key, x, m, h, n)
                )
