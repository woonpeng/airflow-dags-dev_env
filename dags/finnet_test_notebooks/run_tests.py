"""
Test the pipeline with the test data
"""
# pylint: disable=import-error
# pylint: disable=unused-argument
# pylint: disable=invalid-name
# pylint: disable=too-many-locals
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=unused-import

import itertools
import json
import logging
import os
import re
import time
from datetime import datetime

import requests_mock
from pyspark import SparkConf
from pyspark.sql import SQLContext, SparkSession

from fncore.tasks import (build_edge_lists, build_node_lists, graph_to_neo4j,
                          neo4j_manager, neo4j_writer, resolve_edge_entities,
                          resolve_node_entities)
from fncore.utils.graph_specification import GraphSpec
from fncore.utils.graphframe_tools import (get_graph_dataframes,
                                           load_node_edge_lists)
from fncore.utils.neo4j_conf import get_neo4j_context
from fncore.utils.spark_tools import SparkConfFactory, get_spark_context

DATA_PATH = os.environ['PIPELINE_DATA_PATH']
DATA_FORMAT = os.environ['PIPELINE_DATA_FORMAT']
LOCAL_DATA_PATH = os.path.join(os.getcwd(), 'data')
os.environ['PYSPARK_SUBMIT_ARGS'] = \
     "--packages com.databricks:spark-csv_2.11:1.4.0 " \
     "pyspark-shell"


# Setup the spark configuration
CONFIG = dict()
CONFIG['SparkConfiguration'] = (SparkConf()
                                .setMaster('local')
                                .setAppName("test create data")
                                .set("spark.executor.memory", "512m"))


def test_neo4j_writer(sc, sql_context, neo_context, spec):
    """
    Test the neo4j writer based on the defined behavior
    :param sc: Spark context
    :param sql_context: SQL context
    :param neo_context: Py2neo context
    :param spec: Graph specification
    :return: True if all tests successful, else throw assertion exceptions
    """
    graph_name = spec['name']
    graph_data_path = {
        'tables': os.path.join(
            DATA_PATH, graph_name, 'tables'),
        'node_list': os.path.join(
            DATA_PATH, graph_name, 'node_list_resolved'),
        'edge_list': os.path.join(
            DATA_PATH, graph_name, 'edge_list_resolved')
    }

    # Combine all the entities in the node list
    node_data = None
    node_list = spec.get('node_lists')
    if node_list:
        for node_kind in node_list:

            # Check that the node list exist
            filename = os.path.join(graph_data_path['node_list'],
                                    node_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)
            assert data is not None, "Node list %s does not exist" % node_kind['safe_name']

            if node_data:
                node_data = node_data.unionAll(data[['_canonical_id']])
            else:
                node_data = data[['_canonical_id']]

    # Drop duplicates
    if node_data:
        node_data = node_data\
                    .dropDuplicates(['_canonical_id'])\
                    .dropna(how='any', subset=['_canonical_id'])
        numcount = node_data.count()
    else:
        numcount = 0

    # Count the number of nodes in Neo4j
    querystring = "MATCH (n: _searchable) return count(n) as count"
    cursor = neo_context.run(querystring)
    neo_count = cursor.data()[0]['count']
    assert numcount == neo_count, "Neo4J has %d nodes, expected %d nodes" \
                                  % (neo_count, numcount)

    # Check that the properties of the nodes do not have repeated entries
    querystring = "MATCH (n: _searchable) return n"
    cursor = neo_context.run(querystring)
    nodes = [cur_node['n'] for cur_node in cursor]
    for cur_node in nodes:
        for prop_key in cur_node:
            prop = cur_node.get(prop_key, None)
            if isinstance(prop, list):
                propset = list(set(prop))
                assert sorted(prop) == sorted(propset)

    # Check each edge list
    edge_data = None
    edge_list = spec.get('edge_lists')
    if edge_list:
        for edge_kind in edge_list:
            # Check that the edge list exist
            filename = os.path.join(graph_data_path['edge_list'], edge_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .load(filename)
            assert data is not None, "Edge list %s does not exist" \
                                     % edge_kind['safe_name']

            # Check that the data contains the columns specified
            assert '_canonical_id_source' in data.columns, \
                   "_canonical_id_source not found in %s" % edge_kind['safe_name']
            assert '_canonical_id_target' in data.columns, \
                   "_canonical_id_target not found in %s" % edge_kind['safe_name']


            # Drops duplicates
            keyname = None
            keylist = ['_canonical_id_source', '_canonical_id_target']
            if 'index_column' not in edge_kind:
                data = data.dropDuplicates(keylist)
            data = data.dropna(how='any', subset=keylist)

            if edge_data:
                edge_data = edge_data.unionAll(data[['_canonical_id_source',
                                                     '_canonical_id_target']])
            else:
                edge_data = data[['_canonical_id_source',
                                  '_canonical_id_target']]


    # Drop duplicates
    if edge_data:

        # Remove edges where nodes do not exist
        node_data.registerTempTable("node")
        edge_data.registerTempTable("edge")
        querystring = 'SELECT e.* from edge e inner join node n on '\
                       'e.`_canonical_id_source` = n.`_canonical_id` '\
                       'inner join node b on e.`_canonical_id_target`' \
                       '= b.`_canonical_id` '
        final_edge_data = sql_context.sql(querystring)
        numcount = final_edge_data.count()
        sql_context.dropTempTable("edge")
        sql_context.dropTempTable("node")

    else:
        numcount = 0

    # Count the number of edges in Neo4j
    querystring = "MATCH ()-[r]->() return count(r) as count"
    cursor = neo_context.run(querystring)
    neo_count = cursor.data()[0]['count']
    assert numcount == neo_count, "Neo4j has %d edges, expected %d edges" \
                                   % (neo_count, numcount)

    # Test successful
    return True


def test_neo4j_purger(neo_context):
    """
    Test the neo4j writer based on the defined behavior
    :param neo_context: Py2neo context
    :return: True if all tests successful, else throw assertion exceptions
    """

    # Count the number of nodes in Neo4j
    querystring = "MATCH (n: _searchable) return count(n) as count"
    cursor = neo_context.run(querystring)
    neo_count = cursor.data()[0]['count']
    assert neo_count == 0, "Neo4J has %d nodes, expected 0 nodes" % neo_count

    # Count the number of edges in Neo4j
    querystring = "MATCH ()-[r]->() return count(r) as count"
    cursor = neo_context.run(querystring)
    neo_count = cursor.data()[0]['count']
    assert neo_count == 0, "Neo4j has %d edges, expected 0 edges" % neo_count

    # Test successful
    return True


def parse_entity_mapping(mapjson):
    """
    Parse the entity mapping json to return two dictionaries
    of lists of value to replace and the new value, indexed
    by the node list
    """
    erdictorig = {}
    erdictrep = {}
    for item in mapjson:
        cid = item.get('id')
        if 'source_entities' in item and \
           isinstance(item['source_entities'], list):
            for sitem in item['source_entities']:
                node_list = sitem.get('node_list')
                entity_id = sitem.get('entity_id')
                for cur_node, cur_id in itertools.product(node_list, entity_id):
                    cur_node_list = erdictorig.get(cur_node, [])
                    cur_node_list.append(cur_id)
                    erdictorig[cur_node] = cur_node_list

                    cur_node_list = erdictrep.get(cur_node, [])
                    cur_node_list.append(cid)
                    erdictrep[cur_node] = cur_node_list

    return erdictorig, erdictrep


def test_resolve_entities(sc, sql_context, spec, mapjson):
    """
    Define the behavior of resolve_entities and test
    """
    graph_name = spec['name']
    graph_data_path = {
        'tables': os.path.join(
            DATA_PATH, graph_name, 'tables'),
        'node_list': os.path.join(
            DATA_PATH, graph_name, 'node_list_resolved'),
        'edge_list': os.path.join(
            DATA_PATH, graph_name, 'edge_list_resolved')
    }

    # Parse the mapping file
    origmap, repmap = parse_entity_mapping(mapjson)

    # Check each node list
    node_list = spec.get('node_lists')
    if node_list:
        for node_kind in node_list:

            # Check that the node list exist
            filename = os.path.join(graph_data_path['node_list'], node_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)
            assert data is not None, "Node list %s does not exist" % node_kind['safe_name']

            # Check that the data contains the columns specified
            key_name = node_kind['index_column']['safe_name']
            assert key_name in \
                   data.columns
            if 'metadata_columns' in node_kind:
                for q in node_kind['metadata_columns']:
                    assert q['safe_name'] in data.columns

            # Check that canonical id column exist
            assert '_canonical_id' in data.columns, \
                    "_canonical_id not found in node list %s" % node_kind['safe_name']

            # Replace the mapped entities to their canonical names
            data = data.replace(origmap[node_kind['safe_name']], repmap[node_kind['safe_name']],
                                subset=[key_name])

            # Check that no two rows with different keys mapping to the same
            # canonical id
            data.registerTempTable('node')
            querystring = 'select * from node a inner join node b on ' + \
                          'a.`_canonical_id` = b.`_canonical_id` ' + \
                          'where a.`' + str(key_name) + '` != b.`' + \
                          str(key_name) + '`'
            result = sql_context.sql(querystring)
            numconflict = result.count()
            assert numconflict == 0, \
                   "%d nodes with conflicting canonical id found" % numconflict

            # Check that no two rows with same keys mapping to different
            # canonical id
            querystring = 'select * from node a inner join node b on ' + \
                          'a.`' + str(key_name) + '` = b.`' + str(key_name) + '` '\
                          'where a.`_canonical_id` != b.`_canonical_id` '
            result = sql_context.sql(querystring)
            numconflict = result.count()
            sql_context.dropTempTable('node')
            assert numconflict == 0, \
                   "%d conflicting nodes with similar canonical id found" \
                   % numconflict

            # Count the number of entries
            numdata = data.count()

            # Check that it contains the same number of entries as the original
            # tables
            safe_table = node_kind['safe_table_name']
            filename = os.path.join(graph_data_path['tables'], safe_table)
            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)

            numdata_orig = data.count()
            assert numdata_orig == numdata, \
                   "Resolved node list %s has %d entries, expected %d entries" \
                    % (node_kind['safe_name'], numdata_orig, numdata)

    # Check each edge list
    edge_list = spec.get('edge_lists')
    if edge_list:
        for edge_kind in edge_list:
            # Check that the edge list exist
            filename = os.path.join(graph_data_path['edge_list'], edge_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .load(filename)
            assert data is not None, "Edge list %s does not exist" \
                                      % edge_kind['safe_name']

            # Check that the data contains the columns specified
            assert '_canonical_id_source' in data.columns, \
                   "_canonical_id_source not found in %s" % edge_kind['safe_name']
            assert '_canonical_id_target' in data.columns, \
                   "_canonical_id_target not found in %s" % edge_kind['safe_name']

            source_key = edge_kind['source_column']['safe_name']
            target_key = edge_kind['target_column']['safe_name']

            # Replace the mapped entities to their canonical names
            data = data.replace(origmap[edge_kind['safe_name']], repmap[edge_kind['safe_name']],
                                subset=[source_key, target_key])

            # Check that no two rows with different keys mapping to the same
            # canonical id
            data.registerTempTable('edge')
            querystring = 'select * from edge a inner join edge b on ' + \
                          'a.`_canonical_id_source` = b.`_canonical_id_source` ' + \
                          'where a.`' + str(source_key) + '` != b.`' + str(source_key) + '` '
            result = sql_context.sql(querystring)
            numconflict = result.count()
            assert numconflict == 0, \
                   "%d edges with conflicting canonical id found" % numconflict

            querystring = 'select * from edge a inner join edge b on ' + \
                          'a.`_canonical_id_target` = b.`_canonical_id_target` ' + \
                          'where a.`' + str(target_key) + '` != b.`' + str(target_key) + '` '
            result = sql_context.sql(querystring)
            numconflict = result.count()
            assert numconflict == 0, \
                   "%d edges with conflicting canonical id found" % numconflict

            # Check that no two rows with the same keys mapping to different
            # canonical id
            querystring = 'select * from edge a inner join edge b on ' + \
                          'a.`' + str(source_key) + '` = b.`' + str(source_key) + '` ' + \
                          'where a.`_canonical_id_source` != b.`_canonical_id_source` '
            result = sql_context.sql(querystring)
            numconflict = result.count()
            assert numconflict == 0, \
                   "%d conflicting edges with similar canonical id found" % numconflict

            querystring = 'select * from edge a inner join edge b on ' + \
                          'a.`' + str(target_key) + '` = b.`' + str(target_key) + '` ' + \
                          'where a.`_canonical_id_target` != b.`_canonical_id_target` '
            result = sql_context.sql(querystring)
            numconflict = result.count()
            sql_context.dropTempTable('edge')
            assert numconflict == 0, \
                   "%d conflicting edges with similar canonical id found" % numconflict

            # Count the number of entries
            numdata = data.count()

            # Check that it contains the same number of entries as the original
            # tables
            safe_table = edge_kind['safe_table_name']
            filename = os.path.join(graph_data_path['tables'], safe_table)
            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)

            numdata_orig = data.count()
            assert numdata_orig == numdata, \
                   "Resolved edge list %s has %d entries, expected %d entries" \
                    % (edge_kind['safe_name'], numdata_orig, numdata)

    return True


def test_build_lists(sc, sql_context, spec):
    """
    Define the behavior of build_lists and test
    """
    graph_name = spec['name']
    graph_data_path = {
        'tables': os.path.join(
            DATA_PATH, graph_name, 'tables'),
        'node_list': os.path.join(
            DATA_PATH, graph_name, 'node_list'),
        'edge_list': os.path.join(
            DATA_PATH, graph_name, 'edge_list')
    }

    # Check each node list
    node_list = spec.get('node_lists')
    if node_list:
        for node_kind in node_list:

            # Check that the node list exist
            filename = os.path.join(graph_data_path['node_list'],
                                    node_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)
            assert data is not None

            # Check that the data contains the columns specified
            assert node_kind['index_column']['safe_name'] in data.columns
            if 'metadata_columns' in node_kind:
                for q in node_kind['metadata_columns']:
                    assert q['safe_name'] in data.columns

            # Count the number of entries
            numdata = data.count()

            # Check that it contains the same number of entries as the original
            # tables
            safe_table = node_kind['safe_table_name']
            filename = os.path.join(graph_data_path['tables'], safe_table)
            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)

            numdata_orig = data.count()
            assert numdata_orig == numdata

    # Check each edge list
    edge_list = spec.get('edge_lists')
    if edge_list:
        for edge_kind in edge_list:

            # Check that the edge list exist
            filename = os.path.join(graph_data_path['edge_list'],
                                    edge_kind['safe_name'])

            data = sql_context \
                .read.format(DATA_FORMAT) \
                .load(filename)
            assert data is not None

            # Check that the data contains the columns specified
            assert edge_kind['source_column']['safe_name'] in data.columns
            assert edge_kind['target_column']['safe_name'] in data.columns
            if 'metadata_columns' in edge_kind:
                for q in edge_kind['metadata_columns']:
                    assert q['safe_name'] in data.columns

            # Count the number of entries
            numdata = data.count()

            # Check that it contains the same number of entries as the original
            # tables
            safe_table = edge_kind['safe_table_name']
            filename = os.path.join(graph_data_path['tables'],
                                    safe_table)
            data = sql_context \
                .read.format(DATA_FORMAT) \
                .option('header', 'true') \
                .option('inferschema', 'true') \
                .load(filename)

            numdata_orig = data.count()
            assert numdata_orig == numdata

    return True


def test_pipeline_tasks():
    """
    Test the pipeline
    """

    # Set the list of tasks to test
    dolist = [
        'build_lists', 'resolve_entities',
        'neo4j_purger', 'neo4j_writer',
        'graph_tools'
    ]

    # Get neo4j ssh username and port
    neo4j_ssh_username = os.environ.get('NEO4J_SSH_USERNAME', 'neo4j')
    neo4j_ssh_port = int(os.environ.get('NEO4J_SSH_PORT', 9000))

    # Setup the spark configuration
    config = dict()
    config['SparkConfiguration'] = (SparkConf()
                                    .setMaster('local[*]')
                                    .setAppName("test create data")
                                    .set("spark.executor.memory", "1024m"))

    # Get the graph specs
    datalist = os.listdir(LOCAL_DATA_PATH)
    jsonlist = [k for k in datalist if re.match(r'.*\.json$', k)]

    # Read in the graph spec
    for gspec in jsonlist:
        # Load the graph spec
        with open(os.path.join(LOCAL_DATA_PATH, gspec), 'r') as f:
            graph_spec = GraphSpec.from_dict(json.load(f))
            spec = graph_spec.to_dict()

        tables_path = os.path.join(DATA_PATH, graph_spec.name, 'tables')
        n_path = os.path.join(DATA_PATH, graph_spec.name, 'node_list')
        e_path = os.path.join(DATA_PATH, graph_spec.name, 'edge_list')
        n_path_res = os.path.join(DATA_PATH, graph_spec.name, 'node_list_resolved')
        e_path_res = os.path.join(DATA_PATH, graph_spec.name, 'edge_list_resolved')

        logging.info("Processing " + gspec)

        # Use graph specification's neo4j connection
        neo_config = {
            'uri': spec['graph_uri'],
            'max_retries': config.get('neo4j.max_retries', 5),
            'max_batchsize': config.get('neo4j.max_batchsize', 10000)
        }

        # Build list
        if 'build_lists' in dolist:
            logging.info("Building lists...")
            build_node_lists(
                graph_specification=graph_spec,
                spark_config=(SparkConfFactory()
                              .set_master('local[*]')
                              .set_app_name('test create data')
                              .set('spark.executor.memory', '1g')),
                tables_path=tables_path,
                node_path=n_path,
                data_format=DATA_FORMAT,
            )
            build_edge_lists(
                graph_specification=graph_spec,
                spark_config=(SparkConfFactory()
                              .set_master('local[*]')
                              .set_app_name('test create data')
                              .set('spark.executor.memory', '1g')),
                tables_path=tables_path,
                edge_path=e_path,
                data_format=DATA_FORMAT,
            )
            logging.info("Checking build_lists...")
            with get_spark_context(config['SparkConfiguration']) as spark_ctx:
                sql_context = SQLContext(spark_ctx, sparkSession=SparkSession(spark_ctx))
                assert test_build_lists(spark_ctx, sql_context, spec)

        # Resolve entities
        if 'resolve_entities' in dolist:
            logging.info("Resolving entities...")
            resolve_node_entities(
                graph_specification=graph_spec,
                spark_config=(SparkConfFactory()
                              .set_master('local[*]')
                              .set_app_name('test create data')
                              .set('spark.executor.memory', '1g')),
                entity_maps=dict(),
                input_node_path=n_path,
                output_node_path=n_path_res,
                output_node_id='_canonical_id',
                data_format=DATA_FORMAT
            )
            resolve_edge_entities(
                graph_specification=graph_spec,
                spark_config=(SparkConfFactory()
                              .set_master('local[*]')
                              .set_app_name('test create data')
                              .set('spark.executor.memory', '1g')),
                entity_maps=dict(),
                input_edge_path=e_path,
                output_edge_path=e_path_res,
                output_edge_source_id='_canonical_id_source',
                output_edge_target_id='_canonical_id_target',
                data_format=DATA_FORMAT
            )

        # Purging the graph
        if 'neo4j_purger' in dolist:
            logging.info("Purging Neo4j...")
            neo4j_manager.purge(graph_spec,
                                username=neo4j_ssh_username,
                                port=neo4j_ssh_port)
            logging.info("Checking purging neo4j...")
            with get_neo4j_context(neo_config['uri']) as neo_context:
                assert test_neo4j_purger(neo_context)

        # Graph writer
        if 'neo4j_writer' in dolist:
            logging.info("Writing to Neo4j...")
            graph_to_neo4j.graph_to_neo4j(graph_specification=graph_spec,
                                          spark_config=SparkConfFactory()
                                          .set_master('local[*]')
                                          .set_app_name('write neo4j nodes')
                                          .set("spark.driver.maxResultSize",
                                               "1g")
                                          .set('spark.executor.memory',
                                               '1g'),
                                          input_node_path=n_path_res,
                                          input_edge_path=e_path_res,
                                          username=neo4j_ssh_username,
                                          port=neo4j_ssh_port
                                          )

            # This inserts node properties that were not captured above, more convenient like this???
            neo4j_writer.write_neo4j_nodes(graph_specification=spec,
                                           spark_config=SparkConfFactory()
                                           .set_master('local[*]')
                                           .set_app_name('write neo4j nodes')
                                           .set('spark.executor.memory',
                                                '1g')
                                           )

            datetime_now = datetime.now()
            logging.info("Backing up db, then purge it...")
            neo4j_manager.backup(graph_spec, datetime_now,
                                 username=neo4j_ssh_username,
                                 port=neo4j_ssh_port)
            neo4j_manager.purge(graph_spec,
                                username=neo4j_ssh_username,
                                port=neo4j_ssh_port)
            logging.info("Restoring the backup to db...")
            neo4j_manager.restore(graph_spec,
                                  datetime_now,
                                  username=neo4j_ssh_username,
                                  port=neo4j_ssh_port)

            logging.info("Checking write neo4j...")
            with get_spark_context(config['SparkConfiguration']) as spark_ctx:
                sql_context = SQLContext(spark_ctx, sparkSession=SparkSession(spark_ctx))
                with get_neo4j_context(neo_config['uri']) as neo_context:
                    assert test_neo4j_writer(
                        spark_ctx, sql_context, neo_context, spec
                    )

        if 'graph_tools' in dolist:
            # Test graph_construction_coi.get_graph_dataframes
            data_path = os.environ['PIPELINE_DATA_PATH']
            graph_name = graph_spec.name
            node_path_resolved = os.path.join(data_path, graph_name, 'node_list_resolved')
            edge_path_resolved = os.path.join(data_path, graph_name, 'edge_list_resolved')
            with get_spark_context(config['SparkConfiguration']) as spark_ctx:
                sql_context = SQLContext(spark_ctx, sparkSession=SparkSession(spark_ctx))
                graph = get_graph_dataframes(graph_spec, sql_context,
                                             node_path_resolved, edge_path_resolved,
                                             DATA_FORMAT)

                assert 'node_list' in graph
                assert 'edge_list' in graph
                assert len(graph['node_list']) == len(graph_spec.node_lists)
                for cur_node_list in graph_spec.node_lists:
                    assert cur_node_list.safe_name in graph['node_list']
                assert len(graph['edge_list']) == len(graph_spec.edge_lists)
                for cur_edge_list in graph_spec.edge_lists:
                    assert cur_edge_list.safe_name in graph['edge_list']

            # Test graph_construction_coi.data_loading
            with get_spark_context(config['SparkConfiguration']) as spark_ctx:
                sql_context = SQLContext(spark_ctx, sparkSession=SparkSession(spark_ctx))
                tables = load_node_edge_lists(sql_context, graph_spec,
                                              node_path_resolved, edge_path_resolved,
                                              DATA_FORMAT)
                for cur_edge_list in graph_spec.edge_lists:
                    assert (cur_edge_list.safe_table_name,
                            cur_edge_list.source_column.safe_name,
                            cur_edge_list.target_column.safe_name) in tables
                assert len(tables) == len(graph_spec.node_lists) + len(graph_spec.edge_lists)
    logging.info("Completed run_tests()")
