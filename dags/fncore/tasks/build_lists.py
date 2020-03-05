""" Build list task module.

This module defines the task to build the tables corresponding to the node
lists and edge lists as specified in the graph specification. This task
normally follows the use of the `table_import` task.

"""

# pylint: disable=too-many-arguments

import os

from pyspark.sql import SQLContext

from fncore.utils.spark_tools import get_spark_context


def _build_node_lists(graph_specification,
                      spark_context,
                      tables_path,
                      node_path,
                      data_format='parquet'):
    """Build node lists task.

    Given `graph specification`, build the node lists from the tables imported
    at the `tables_path`. Node lists are then saved to the `node_path`.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param tables_path: Path to get table files for this graph.
    :type tables_path: str
    :param node_path: Path to output node files for this graph.
    :type node_path: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    sql_context = SQLContext(spark_context)

    # create and save node lists
    for node_list in graph_specification.node_lists:
        # path of the table to read the node list from
        table_path = os.path.join(tables_path, node_list.safe_table_name)

        # columns to select from table
        columns = ([node_list.index_column.safe_name]
                   + [col.safe_name for col in node_list.metadata_columns])

        # location to save the node list
        save_path = os.path.join(node_path, node_list.safe_name)

        # load the table and columns
        data = (sql_context.read
                .format(data_format)
                .option('header', 'true')
                .option('inferschema', 'true')
                .load(table_path)
                .select(columns))

        # save the data to the save path
        (data.write
         .format(data_format)
         .mode(saveMode='overwrite')
         .save(save_path))


def _build_edge_lists(graph_specification,
                      spark_context,
                      tables_path,
                      edge_path,
                      data_format='parquet'):
    """Build edge lists task.

    Given `graph specification`, build the edge lists from the tables imported
    at the `tables_path`. Edge lists are then saved to the `edge_path`.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param tables_path: Path to get table files for this graph.
    :type tables_path: str
    :param edge_path: Path to output edge files for this graph.
    :type edge_path: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    sql_context = SQLContext(spark_context)

    # create and save edge lists
    for edge_kind in graph_specification.edge_lists:
        # path of the table to read the edge list from
        table_path = os.path.join(tables_path, edge_kind.safe_table_name)

        # columns to select from table
        columns = ([edge_kind.source_column.safe_name]
                   + [edge_kind.target_column.safe_name]
                   + [col.safe_name for col in edge_kind.metadata_columns]
                   + ([edge_kind.index_column.safe_name]
                      if edge_kind.index_column is not None else []))

        # location to save the node list
        save_path = os.path.join(edge_path, edge_kind.safe_name)

        # load the table and columns
        data = (sql_context.read
                .format(data_format)
                .option('header', 'true')
                .option('inferschema', 'true')
                .load(table_path)
                .select(columns))

        # save the data to the save path
        (data.write
         .format(data_format)
         .mode(saveMode='overwrite')
         .save(save_path))


def build_node_lists(graph_specification,
                     spark_config,
                     tables_path,
                     node_path,
                     data_format='parquet'):
    """Runner for build node lists task that sets up the SparkContext.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param tables_path: Path to get table files for this graph.
    :type tables_path: str
    :param node_path: Path to output node files for this graph.
    :type node_path: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    with get_spark_context(spark_config.create()) as spark:
        _build_node_lists(graph_specification=graph_specification,
                          spark_context=spark,
                          tables_path=tables_path,
                          node_path=node_path,
                          data_format=data_format)


def build_edge_lists(graph_specification,
                     spark_config,
                     tables_path,
                     edge_path,
                     data_format='parquet'):
    """Runner for build edge lists task that sets up the SparkContext.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param tables_path: Path to get table files for this graph.
    :type tables_path: str
    :param edge_path: Path to output edge files for this graph.
    :type edge_path: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    with get_spark_context(spark_config.create()) as spark:
        _build_edge_lists(graph_specification=graph_specification,
                          spark_context=spark,
                          tables_path=tables_path,
                          edge_path=edge_path,
                          data_format=data_format)
