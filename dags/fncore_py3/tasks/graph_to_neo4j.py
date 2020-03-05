"""
Graph to Neo4j module.

This module defines the task of preparing the imported node and edge lists to a
format where Neo4j can import directly. The advanlabele of this approach is that
the import can be done offline and is non-transactional, hence faster.
Currently, this has the limitation that the properties from multiple nodes /
edges cannot be combined / concantenated together. For nodes, only the
canonical id columns will be inserted, while for the edges, they are replaced.
Hence a subsequent slabele of inserting the properties by the neo4j writer is
needed.

Refer to https://neo4j.com/docs/operations-manual/current/tutorial/import-tool/
for more information

"""

# pylint: disable=no-name-in-module

import functools
import os
import shutil
import tempfile

from itertools import chain, combinations
from uuid import uuid4

from pyspark.sql import DataFrame, HiveContext, SQLContext
from pyspark.sql.functions import (array, array_distinct, array_join, col, collect_set, explode, lit, split, trim,
                                   flatten, array_sort, size,
                                   udf, explode_outer, concat_ws, when, collect_list, last, element_at)
from pyspark.sql.types import ArrayType, StringType, StructType

from fncore_py3.tasks.neo4j_manager import build_db
from fncore_py3.utils.dataframe_tools import *
from fncore_py3.utils.graph_specification import NodeListSpec
from fncore_py3.utils.spark_tools import get_spark_context


def generate_label_cols(col_list, labels):
    """
    Generates list of columns to be used as labels and also
    preprend the static labels as literal columns

    :param col_list: List of all columns to consider
    :param labels: List of static labels
    :return: List of sql column types of columns to read and literals of the static label(s)
    """
    return [lit(l) for l in labels] + [
        col(c.col_name) for c in col_list if c.use_as_label and not c.hidden
    ]


def generate_prop_cols_and_aliases(col_list):
    """
    Generates list of property columns, and friendly name alias

    :param col_list: List of all columns to consider
    :return: List of tuples of columns and tuples
    """
    return [
        (col(c.col_name), strip_ticks(c.friendly_name or c.name))
        for c in col_list if not c.use_as_label and not c.hidden
    ]


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def load_transform_node_table(node_spec,
                              spark_context,
                              output_node_id_col,
                              output_label_col,
                              data_format='parquet'):
    """
    Reads and formats node table

    :param node_spec: Node specification.
    :type node_spec: fncore.utils.graph_specification.NodeListSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param output_node_id_col: Column name to use for node id.
    :type output_node_id_col: str
    :param output_label_col: Column name to use for node labels.
    :type output_label_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    """
    sql_context = HiveContext(spark_context)
    label_cols = generate_label_cols(node_spec.metadata_columns, node_spec.labels)
    prop_cols = [c.alias(a) for c, a in generate_prop_cols_and_aliases(node_spec.metadata_columns)]

    data = (
        sql_context
        .read.format(data_format)
        .option('header', 'true')
        .option('inferschema', data_format != "parquet")
        .load(os.path.join(node_spec.table_path, node_spec.table_name))
    )

    transformed = (
        data
        # Make canonical id column
        .withColumn(output_node_id_col, col(node_spec.index_column.col_name))
        .filter(col(output_node_id_col) != '')
        .dropna(how='any', subset=[output_node_id_col])
        # Make label array column
        .withColumn(output_label_col, array(*label_cols).cast(ArrayType(StringType(), True)))
        # Select with desired property columns
        .select(output_node_id_col, output_label_col, *prop_cols))

    return transformed


def get_combined_nodes(graph_specification,
                       spark_config,
                       output_label_col,
                       common_labels,
                       data_format='parquet',
                       array_delimiter=';'):
    """
    Return a iterable of dataframes of all nodes

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param output_node_id_col: Column name to use for node id.
    :type output_node_id_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param common_labels: Common labels to append to all the nodes
    :type common_labels: array of str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param array_delimiter: Delimiter used to separate items in array
    :type array_delimiter: str
    """
    # Generate all node lists, also interpret edge tables to read source and tgt nodes
    node_spec_list = (
        graph_specification.node_lists + graph_specification.community_lists)
    for edge_spec in graph_specification.edge_lists:
        node_spec_list += [
            NodeListSpec(
                name=edge_spec.name,
                table_name=edge_spec.table_name,
                table_path=edge_spec.table_path,
                index_column=edge_spec.source_column,
                metadata_columns=edge_spec.source_metadata_columns,
                labels=edge_spec.source_labels,
                index_space=edge_spec.source_index_space),
            NodeListSpec(
                name=edge_spec.name,
                table_name=edge_spec.table_name,
                table_path=edge_spec.table_path,
                index_column=edge_spec.target_column,
                metadata_columns=edge_spec.target_metadata_columns,
                labels=edge_spec.target_labels,
                index_space=edge_spec.target_index_space)
        ]

    node_space_set = set([(spec.index_space, spec.export_id_col) for spec in node_spec_list])
    for (node_space, output_node_id_col) in node_space_set:
        # Spark context will be closed when the iteration ends
        with get_spark_context(spark_config.create()) as spark_context:
            # Union all the node df, then drop those with no id
            nodes_union = (
                union_all_by_name(
                    spark_context,
                    (load_transform_node_table(
                        node_spec=node_spec,
                        spark_context=spark_context,
                        output_node_id_col=output_node_id_col,
                        output_label_col=output_label_col,
                        data_format=data_format
                    ) for node_spec in node_spec_list if node_spec.index_space == node_space)))

            # Condense information of all nodes
            nodes_coalesced = (
                nodes_union
                .groupby(output_node_id_col)
                .agg(
                    # Distinct union of lists of labels
                    array_distinct(flatten(collect_list(output_label_col))).alias(output_label_col),
                    # FIXME: Naive logic to take last value, maybe take latest??
                    *[element_at(collect_list(add_ticks(c)), -1).alias(strip_ticks(c)) for c
                      in set(nodes_union.columns) - {output_node_id_col, output_label_col}])
                # Process label columns to concatenated string
                .withColumn(output_label_col, prepend(common_labels)(output_label_col))
                .withColumn(output_label_col, when(size(col(output_label_col)) != 0, col(output_label_col)))
                .withColumn(output_label_col, array_to_str(array_delimiter)(output_label_col))
            ).cache()

            yield nodes_coalesced


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def get_transformed_edge_table(edge_spec,
                               spark_context,
                               output_source_col,
                               output_target_col,
                               output_label_col,
                               data_format='parquet'):
    """
    Reads and formats edge list

    :param edge_spec:
    :type edge_spec: fncore.utils.graph_specification.EdgeListSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param output_source_col: Column name to use for source id.
    :type output_source_col: str
    :param output_target_col: Column name to use for target id.
    :type output_target_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :return:
    """
    sql_context = SQLContext(spark_context)

    # Define selection columns
    source_col = edge_spec.source_column.col_name
    target_col = edge_spec.target_column.col_name
    st_arr_col = str(uuid4())
    info_columns = [
        c for c in edge_spec.metadata_columns + [edge_spec.index_column] if c]
    label_cols = generate_label_cols(info_columns, edge_spec.labels)
    prop_cols = [c.alias(a) for c, a in generate_prop_cols_and_aliases(info_columns)]

    # Read data
    data = (
        sql_context
        .read.format(data_format)
        .option('header', 'true')
        .option('inferschema', data_format != "parquet")
        .load(os.path.join(edge_spec.table_path, edge_spec.table_name))
    )

    # Drop invalid rows, put source and target into array, order if not directed
    transformed = (
        data
        .dropna(how='any', subset=[source_col, target_col])
        .filter((col(source_col) != '') & (col(target_col) != ''))
        .withColumn(st_arr_col, array(source_col, target_col))
    )
    if not edge_spec.directed:
        transformed = transformed.withColumn(st_arr_col, array_sort(col(st_arr_col)))

    edge_formatted = (
        transformed
        # Make canonical id columnS
        .withColumn(output_source_col, element_at(col(st_arr_col), 1))
        .withColumn(output_target_col, element_at(col(st_arr_col), 2))
        # Make label array column
        .withColumn(output_label_col, array(*label_cols).cast(ArrayType(StringType(), True)))
        # Select with desired property columns
        .select(output_source_col, output_target_col, output_label_col, *prop_cols)
    )

    return edge_formatted


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def get_transformed_community_edge_table(comm_spec,
                                         spark_context,
                                         output_source_col,
                                         output_target_col,
                                         output_label_col,
                                         data_format='parquet'):
    """
    Reads community tables and transforms into edge list

    :param comm_spec:
    :type comm_spec: fncore.utils.graph_specification.CommunityListSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param output_source_col: Column name to use for source id.
    :type output_source_col: str
    :param output_target_col: Column name to use for target id.
    :type output_target_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :return:
    """
    sql_context = SQLContext(spark_context)

    # Define selection columns
    id_col = comm_spec.index_column.col_name
    grouping_cols = [c.col_name for c in comm_spec.grouping_columns]
    label_cols = generate_label_cols(comm_spec.edge_metadata_columns, comm_spec.edge_labels)
    prop_tups = generate_prop_cols_and_aliases(comm_spec.edge_metadata_columns)
    group_nohide_tups = generate_prop_cols_and_aliases(comm_spec.edge_metadata_columns)

    # Define udf to make combinations
    @udf("array<struct<_1: string, _2: string>>")
    def get_pairs(lst):
        return [sorted(p) for p in combinations(lst, 2)]

    # Read data
    data = (
        sql_context
        .read.format(data_format)
        .option('header', 'true')
        .option('inferschema', data_format != "parquet")
        .load(os.path.join(comm_spec.table_path, comm_spec.table_name))
    )

    data_group = (
        data
        .filter(col(id_col) != '')
        .dropna(how='any', subset=[id_col])
        # Collect all labels per row into an array, array ignores null, so we safe?
        .withColumn(output_label_col, array(*label_cols).cast(ArrayType(StringType(), True)))
        # Group by the grouping criteria
        .dropna(how='any', subset=grouping_cols)
        .groupby(*grouping_cols)
        .agg(
            # Collect all nodes into a set, this guarantees uniqueness
            collect_set(id_col).alias(strip_ticks(id_col)),
            # Distinct union of lists of labels
            array_distinct(flatten(collect_list(output_label_col))).alias(output_label_col),
            # Get last element for properties
            *[element_at(collect_list(c), -1).alias(strip_ticks(c)) for c, a in prop_tups])
    )

    data_pairs = (
        data_group
        .withColumn(strip_ticks(id_col), explode(get_pairs(col(id_col))))
        .select(
            col(id_col + "._1").alias(output_source_col),
            col(id_col + "._2").alias(output_target_col),
            output_label_col,
            *[c.alias(a) for c, a in prop_tups + group_nohide_tups])
    ).cache()

    return data_pairs


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def get_combined_edges(graph_specification,
                       spark_config,
                       output_label_col,
                       data_format='parquet',
                       array_delimiter=';'):
    """
    Return a iterable of dataframes of all edges

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param output_source_col: Column name to use for source id.
    :type output_source_col: str
    :param output_target_col: Column name to use for target id.
    :type output_target_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param array_delimiter: Delimiter used to separate items in array
    :type array_delimiter: str
    """
    edge_category_set = set([
        (edge_spec.edge_category, edge_spec.export_source_col, edge_spec.export_target_col) for edge_spec
        in graph_specification.edge_lists + graph_specification.community_lists])

    for (edge_category, output_source_col, output_target_col) in edge_category_set:
        with get_spark_context(spark_config.create()) as spark_context:
            edge_tables = (
                get_transformed_edge_table(
                    edge_spec=edge_spec,
                    spark_context=spark_context,
                    output_source_col=output_source_col,
                    output_target_col=output_target_col,
                    output_label_col=output_label_col,
                    data_format=data_format)
                for edge_spec in graph_specification.edge_lists
                if edge_spec.edge_category == edge_category
            )

            comm_tables = (
                get_transformed_community_edge_table(
                    comm_spec=comm_spec,
                    spark_context=spark_context,
                    output_source_col=output_source_col,
                    output_target_col=output_target_col,
                    output_label_col=output_label_col,
                    data_format=data_format)
                for comm_spec in graph_specification.community_lists
                if comm_spec.edge_category == edge_category
            )

            union_table = union_all_by_name(
                spark_context,
                (df for df in chain(edge_tables, comm_tables)))

            combined_edge = (
                union_table
                .groupby([output_source_col, output_target_col])
                .agg(
                    # Distinct union of lists of labels
                    array_distinct(flatten(collect_list(output_label_col))).alias(output_label_col),
                    # FIXME: Naive logic to take last value, maybe take latest??
                    *[element_at(collect_list(add_ticks(c)), -1).alias(strip_ticks(c)) for c
                      in set(union_table.columns) - {output_source_col, output_target_col, output_label_col}])
                .withColumn(output_label_col, array_to_str(array_delimiter)(output_label_col))
            )

            yield combined_edge


# pylint: disable=too-many-locals
def graph_to_neo4j(graph_specification,
                   spark_config,
                   username=None,
                   port=None,
                   data_format='parquet',
                   max_result_size=1e9,
                   push_to_neo4j=True,
                   temp_dir=None,
                   preserve_tmp_files=False
                   ):
    """
    Transform the node and edge lists and push into neo4j.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param username: user which has ssh access to the graph database server
    :type username: str
    :param port: the port on which the graph database server provides ssh access
    :type port: int
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param max_result_size: Maximum result size that spark driver accept
    :type max_result_size: int
    :param push_to_neo4j: If true, export nodes and edges to neo4j graph
    :type push_to_neo4j: bool
    :param temp_dir: If specified, write temporary files to this directory
    :type temp_dir: str
    :param preserve_tmp_files: If true, don't delete files after completing task
    :type preserve_tmp_files: bool
    """

    field_delimiter = ','
    quote_char = '"'

    # Get a temporary directory to save the intermediate files
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)
        os.mkdir(temp_dir)
    temp_dir = temp_dir or tempfile.mkdtemp()

    try:
        # Get the nodes
        nodes_df = get_combined_nodes(
            graph_specification=graph_specification,
            spark_config=spark_config,
            output_label_col=':LABEL',
            common_labels=['_searchable'],
            array_delimiter=';',
            data_format=data_format
        )
        nodes = (
            pdf for df in nodes_df
            for pdf in to_pandas_iterator(df, max_result_size=max_result_size)
        )

        # Iteratively export out the nodes
        for i, curr_node in enumerate(nodes):

            # Get a temporary file to ensure we are not overwriting any existing
            # files
            handle, temp_file = tempfile.mkstemp(dir=temp_dir, suffix='.csv')
            if handle:
                os.close(handle)

            # Save the node table as the temporary file
            curr_node.to_csv(temp_file,
                             sep=field_delimiter,
                             header=True,
                             index=False,
                             encoding='utf-8',
                             quotechar=quote_char)

            # Verbose
            if preserve_tmp_files:
                print("Wrote temp nodes .csv to {}".format(temp_file))

        # Get a generator of the edges
        # Iterates over each edge_spec.safe_table_name
        # Each one can be further batched if large
        edges_df = get_combined_edges(
            graph_specification=graph_specification,
            spark_config=spark_config,
            output_label_col=':TYPE'
        )
        edges_result = (
            pdf for df in edges_df
            for pdf in to_pandas_iterator(df, max_result_size=max_result_size)
        )

        # For each edge
        for i, edges in enumerate(edges_result):

            # Get a temporary file to ensure we are not
            # overwriting any existing files
            handle, temp_file = tempfile.mkstemp(dir=temp_dir, suffix='.csv')
            if handle:
                os.close(handle)

            # Save the node table as the temporary file
            edges.to_csv(temp_file,
                         sep=field_delimiter,
                         header=True,
                         index=False,
                         encoding='utf-8',
                         quotechar=quote_char)

            # Verbose
            if preserve_tmp_files:
                print("Wrote temp edges .csv to {}".format(temp_file))

        # Compress the tables and remove the exported tables
        zipped_file = shutil.make_archive(temp_dir, 'zip', temp_dir, '.')

        # Verbose
        if preserve_tmp_files:
            print("Wrote temp .zip to {}".format(temp_dir + '.zip'))

    except:
        raise

    # Call the hook to build the database from the zip file
    try:
        # Neo4J can automatically tell node tables from edge tables based on header names
        if push_to_neo4j:
            build_db(graph_specification, zipped_file, username=username, port=port)
    except:
        raise
    finally:
        if not preserve_tmp_files:
            # Clean up the zip file
            os.remove(temp_dir + '.zip')
            shutil.rmtree(temp_dir, ignore_errors=True)

