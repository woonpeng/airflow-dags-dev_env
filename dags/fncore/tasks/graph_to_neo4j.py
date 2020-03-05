"""
Graph to Neo4j module.

This module defines the task of preparing the imported node and edge lists to a
format where Neo4j can import directly. The advantage of this approach is that
the import can be done offline and is non-transactional, hence faster.
Currently, this has the limitation that the properties from multiple nodes /
edges cannot be combined / concantenated together. For nodes, only the
canonical id columns will be inserted, while for the edges, they are replaced.
Hence a subsequent stage of inserting the properties by the neo4j writer is
needed.

Refer to https://neo4j.com/docs/operations-manual/current/tutorial/import-tool/
for more information

"""

# pylint: disable=no-name-in-module

import functools
import os
import shutil
import tempfile
from uuid import uuid4

from pyspark.sql import DataFrame, HiveContext, SQLContext
from pyspark.sql.functions import (col, collect_set, explode, lit, split, trim,
                                   udf)
from pyspark.sql.types import ArrayType, StringType

from fncore.tasks.neo4j_manager import build_db
from fncore.utils.spark_tools import get_spark_context


def prepend(items):
    """
    Returns a UDF that prepend items to an iterable

    :param items: list of items to prepend to iterable
    :type items: iterable
    """
    return udf(lambda iterable: [item for item in items] + iterable, ArrayType(StringType()))


# pylint: disable=unnecessary-lambda
def array_to_str(delimiter):
    """
    Returns a UDF that converts an iterable to a string

    :param delimiter: delimiter to use to separate the items in the iterable in
                      string representation
    :type delimiter: str
    """
    return udf(lambda iterable: delimiter.join(iterable), StringType())


def union_all(iterable):
    """
    Union all the Spark data frames in an iterable

    :param iterable: an iterable of spark data frames
    :type iterable: an iterable of spark data frames
    """
    return functools.reduce(DataFrame.unionAll, iterable)


def to_pandas_iterator(dataframe, max_result_size=1e9, factor=1.5):
    """
    Returns an iterable of Pandas dataframe from Spark dataframe

    :param dataframe: a spark dataframe
    :type dataframe: pyspark.sql.DataFrame
    :param max_result_size: the maximum result size (in bytes) that spark driver accept
    :type max_result_size: int
    :param factor: the safety factor in estimating how much should a batch size be.
                   A factor of 1.0 implies that the number of rows in a batch that can
                   fit into the maximnum result size will be based on the estimate of
                   the size of a single row, while a factor greater than 1.0, e.g. 1.2
                   means the estimate will be based on the assumption that the actual
                   size of a row is 1.2 times of the estimated size
    :type factor: float
    """

    # Get the number of rows in the dataframe
    num_rows = dataframe.count()

    # Get the size of the first row
    row = dataframe.limit(1).toPandas()
    row_size = len(row.to_csv(index=False, encoding='utf-8', header=False))

    # Get the size of the header
    header_size = len(row.to_csv(index=False, encoding='utf-8', header=True)) - row_size

    # Take into account of the safety factor
    header_size = header_size * factor
    row_size = row_size * factor

    # Compute the number of rows that will fit within the maximum result size
    num_rows_per_batch = int((max_result_size - header_size) / row_size)

    # Compute the number of batches
    num_batch = int(num_rows / num_rows_per_batch) + 1

    # Split the dataframe into the calculated number of batches
    df_list = dataframe.randomSplit([1.0 for _ in range(num_batch)])

    # Iterate through the splitted dataframes
    for cur_dataframe in df_list:
        yield cur_dataframe.toPandas()


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def load_transform_nodes(graph_specification,
                         spark_context,
                         input_node_path,
                         output_node_id_col,
                         output_label_col,
                         output_tag_col,
                         data_format='parquet',
                         array_delimiter=';'):
    """
    A generator that returns each processed node in the graph specification.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param input_node_path: Path to input node files for this graph.
    :type input_node_path: str
    :param output_node_id_col: Column name to use for node id.
    :type output_node_id_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param output_tag_col: Column name to use for node tag.
    :type output_tag_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param array_delimiter: Delimiter used to separate items in array
    :type array_delimiter: str
    """
    sql_context = HiveContext(spark_context)

    for node_kind in graph_specification.node_lists:
        actual_node_id_col = node_kind.index_column.safe_name

        default_label_col = '-'.join(['_default_label', str(uuid4())])

        # I don't get this definition, we only return one label when there could be many?
        # TODO: It should support mutltiple _label (or properties) per table
        actual_label_col = next(
            iter(
                [column.safe_name
                 for column
                 in node_kind.metadata_columns + [node_kind.index_column]
                 if column.use_as_label]),
            default_label_col
        )

        node_tags = array_delimiter.join(node_kind.tags)

        data = (sql_context.read
                .format(data_format)
                .option('header', 'true')
                .option('inferschema', 'true')
                .load(os.path.join(input_node_path, node_kind.safe_name)))

        transformed = (
            data
            .withColumn(default_label_col, lit(None).cast(StringType()))
            .select(actual_node_id_col, actual_label_col)
            .withColumnRenamed(actual_node_id_col, output_node_id_col)
            .withColumnRenamed(actual_label_col, output_label_col)
            .withColumn(output_tag_col, lit(node_tags).cast(StringType()))
            .withColumn(output_tag_col, explode(split(col(output_tag_col), array_delimiter)))
            .select(output_node_id_col, output_label_col, output_tag_col)
            .distinct()
        )

        yield transformed


def get_combined_nodes(graph_specification,
                       spark_config,
                       input_node_path,
                       output_node_id_col,
                       output_label_col,
                       output_tag_col,
                       common_tags,
                       data_format='parquet',
                       array_delimiter=';',
                       max_result_size=1e9,
                       debug_write=False):
    """
    Return a Pandas data frame of the combined nodes

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param input_node_path: Path to input node files for this graph.
    :type input_node_path: str
    :param output_node_id_col: Column name to use for node id.
    :type output_node_id_col: str
    :param output_label_col: Column name to use for node label.
    :type output_label_col: str
    :param output_tag_col: Column name to use for node tag.
    :type output_tag_col: str
    :param common_tags: Common tags to append to all the nodes
    :type common_tags: array of str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param array_delimiter: Delimiter used to separate items in array
    :type array_delimiter: str
    :param max_result_size: Maximum result size that spark driver accept
    :type max_result_size: int
    """

    with get_spark_context(spark_config.create()) as spark_context:
        transformed_node_lists = load_transform_nodes(
            graph_specification=graph_specification,
            spark_context=spark_context,
            input_node_path=input_node_path,
            output_node_id_col=output_node_id_col,
            output_label_col=output_label_col,
            output_tag_col=output_tag_col,
            data_format=data_format,
            array_delimiter=array_delimiter
        )

        nodes_formatted = (
            union_all(transformed_node_lists)
            .groupby(output_node_id_col)
            .agg(collect_set(output_label_col).alias(output_label_col),
                 collect_set(output_tag_col).alias(output_tag_col))
            .withColumn(output_tag_col, prepend(common_tags)(output_tag_col))
            .withColumn(output_label_col, array_to_str(array_delimiter)(output_label_col))
            .withColumn(output_tag_col, array_to_str(array_delimiter)(output_tag_col))
            .repartition(1000)
            .cache()
        )

        # Drop nodes with empty id (required field)
        nodes_dropped = (
            nodes_formatted
            .filter(nodes_formatted[output_node_id_col] != '')
            .dropna(how='any', subset=[output_node_id_col])
        )

        # If debug mode, write nodes_dropped to hdfs
        if debug_write:
            (nodes_dropped.write
             .format(data_format)
             .mode(saveMode='overwrite')
             .save(os.path.join(input_node_path, "combined_nodes")))

        # Return the dataframe in batches
        for dataframe in to_pandas_iterator(nodes_dropped, max_result_size=max_result_size):
            yield dataframe


# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
def get_transformed_edges(graph_specification,
                          spark_config,
                          input_edge_path,
                          input_source_col,
                          input_target_col,
                          output_source_col,
                          output_target_col,
                          output_tag_col,
                          data_format='parquet',
                          array_delimiter=';',
                          max_result_size=1e9,
                          debug_write=False):
    """
    A generator that returns a Panda data frame of each processed edge
    in the graph specification

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param input_edge_path: Path to input edge files for this graph.
    :type input_edge_path: str
    :param output_source_col: Column name to use for source id.
    :type output_source_col: str
    :param output_target_col: Column name to use for target id.
    :type output_target_col: str
    :param output_tag_col: Column name to use for node tag.
    :type output_tag_col: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param array_delimiter: Delimiter used to separate items in array
    :type array_delimiter: str
    :param max_result_size: Maximum result size that spark driver accept
    :type max_result_size: int
    """

    for edge_kind in graph_specification.edge_lists:
        with get_spark_context(spark_config.create()) as spark_context:
            sql_context = SQLContext(spark_context)

            data = (sql_context
                    .read.format(data_format)
                    .option('header', 'true')
                    .option('inferschema', 'true')
                    .load(os.path.join(input_edge_path, edge_kind.safe_name)))

            edge_kind_columns = (
                edge_kind.metadata_columns
                + [edge_kind.source_column]
                + [edge_kind.target_column]
                + ([edge_kind.index_column] if edge_kind.index_column else [])
                + ([edge_kind.weight_column] if edge_kind.weight_column else [])
            )

            transformed = data

            # Drops duplicates (if index column does not exist)
            # TODO: Support multi field index in the future
            if not edge_kind.index_column:
                dedup_columns = (
                    [edge_kind.source_column.safe_name]
                    + [edge_kind.target_column.safe_name]
                )
                transformed = transformed.dropDuplicates(subset=dedup_columns)

            for column in edge_kind_columns:
                transformed = transformed.withColumnRenamed(
                    column.safe_name,
                    column.friendly_name or column.name
                )

            edge_tags = array_delimiter.join(edge_kind.tags)

            transformed = (
                transformed
                .withColumn(output_source_col, trim(transformed[input_source_col]))
                .withColumn(output_target_col, trim(transformed[input_target_col]))
                .withColumn(output_tag_col, lit(edge_tags))
            )

            transformed = (
                transformed
                .dropna(how='any', subset=[output_source_col, output_target_col])
                .filter(transformed[output_source_col] != '')
                .filter(transformed[output_target_col] != '')
            )

            # TODO: Ought to select only needed columns, no? Lots of redundant columns

            transformed.schema
            if debug_write:
                (transformed.write
                 .format(data_format)
                 .mode(saveMode='overwrite')
                 .save(os.path.join(input_edge_path, "transformed_edges_{}".format(edge_kind.safe_name))))

            for dataframe in to_pandas_iterator(transformed, max_result_size=max_result_size):
                yield dataframe


# pylint: disable=too-many-locals
def graph_to_neo4j(graph_specification,
                   spark_config,
                   input_node_path,
                   input_edge_path,
                   username=None,
                   port=None,
                   data_format='parquet',
                   max_result_size=1e9,
                   debug_write=False,
                   verbose=False
                   ):
    """
    Transform the node and edge lists and push into neo4j.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param entity_maps: Canonical id mapping.
    :type entity_maps: dict
    :param input_node_path: Path to input node files for this graph.
    :type input_node_path: str
    :param input_edge_path: Path to input edge files for this graph.
    :type input_edge_path: str
    :param username: user which has ssh access to the graph database server
    :type username: str
    :param port: the port on which the graph database server provides ssh access
    :type port: int
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param max_result_size: Maximum result size that spark driver accept
    :type max_result_size: int
    """

    field_delimiter = ','
    quote_char = '"'

    # Get a temporary directory to save the intermediate files
    temp_dir = tempfile.mkdtemp()

    try:
        # Get the nodes
        nodes = get_combined_nodes(
            graph_specification=graph_specification,
            spark_config=spark_config,
            input_node_path=input_node_path,
            output_node_id_col='_canonical_id:ID',
            output_label_col='_label',
            output_tag_col=':LABEL',
            common_tags=['_searchable'],
            array_delimiter=';',
            data_format=data_format,
            max_result_size=max_result_size,
            debug_write=debug_write
        )

        # Iteratively export out the nodes
        for curnode in nodes:

            # Get a temporary file to ensure we are not overwriting any existing
            # files
            handle, temp_file = tempfile.mkstemp(dir=temp_dir, suffix='.csv')
            if handle:
                os.close(handle)

            # Save the node table as the temporary file
            curnode.to_csv(temp_file,
                           sep=field_delimiter,
                           header=True,
                           index=False,
                           encoding='utf-8',
                           quotechar=quote_char)

            # Verbose
            if verbose:
                print("Wrote temp nodes .csv to {}".format(temp_file))

        # Get a generator of the edges
        # Iterates over each edge_kind.safe_name
        # Each one can be further batched if large
        edges_result = get_transformed_edges(
            graph_specification=graph_specification,
            spark_config=spark_config,
            input_edge_path=input_edge_path,
            input_source_col='_canonical_id_source',
            input_target_col='_canonical_id_target',
            output_source_col=':START_ID',
            output_target_col=':END_ID',
            output_tag_col=':TYPE',
            max_result_size=max_result_size,
            debug_write=debug_write
        )

        # For each edge
        for edges in edges_result:

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
            if verbose:
                print("Wrote temp edges .csv to {}".format(temp_file))

        # Compress the tables and remove the exported tables
        handle, temp_zip = tempfile.mkstemp(suffix='.zip')
        if handle:
            os.close(handle)
        temp_path, temp_zip_prefix = os.path.split(temp_zip)
        temp_zip_prefix = os.path.splitext(temp_zip_prefix)[0]
        temp_zip_prefix = os.path.join(temp_path, temp_zip_prefix)
        zipped_file = shutil.make_archive(temp_zip_prefix, 'zip', temp_dir, '.')
    except:
        raise
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Call the hook to build the database from the zip file
    try:
        # Neo4J can automatically tell node tables from edge tables based on header names
        build_db(graph_specification, zipped_file, username=username, port=port)
    except:
        raise
    finally:
        # Clean up the zip file
        os.remove(temp_zip)
