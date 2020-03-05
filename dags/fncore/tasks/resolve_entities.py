"""Resolve entities task module.

This module defines the task of replacing values in column(s) of a node list or
edge list with those in an entity mapping dictionary. This task normally
follows the use of the `build_lists` task.

"""

# pylint: disable=too-many-arguments
# pylint: disable=too-many-locals
# pylint: disable=invalid-name
# pylint: disable=no-name-in-module

import os
from uuid import uuid4

from pyspark.sql import SQLContext
from pyspark.sql.functions import coalesce, col, trim
from pyspark.sql.types import StringType, StructField, StructType

from fncore.utils.spark_tools import get_spark_context


class EntityMapper(object):
    """Entity map."""
    def __init__(self, spark_context, entity_map):
        self.spark_context = spark_context
        self._from = '-'.join(['from', str(uuid4())])
        self._to = '-'.join(['to', str(uuid4())])
        self._map = self.frame_entity_map(entity_map)

    def frame_entity_map(self, entity_map):
        """Converts the entity map dict into a dataframe."""
        entity_map = entity_map or {}

        # format the entity map dict for conversion to dataframe
        formatted_entity_map = ({self._from: name, self._to: canonical_name}
                                for name, canonical_name
                                in entity_map.items())

        # convert the entity map dict to dataframe
        df_map = SQLContext(self.spark_context).createDataFrame(
            data=self.spark_context.parallelize(formatted_entity_map),
            schema=StructType(
                [StructField(self._from, StringType(), False),
                 StructField(self._to, StringType(), False)]
            )
        )

        return df_map

    def apply(self, data, on, to=None):
        """Apply entity map"""
        applied = (
            data
            .join(self._map, data[on] == self._map[self._from], "left_outer")
            .withColumn(to or on, coalesce(self._map[self._to], data[on]))
            .drop(self._from)
            .drop(self._to)
        )
        return applied


def trim_white_spaces_from_df(data_df):
    """Trims white spaces from string columns of a Spark Dataframe.

    :param data_df: Spark Dataframe to trim white spaces from
    :type data_df: pyspark.sql.DataFrame
    :return: Spark Dataframe with white spaces trimmed from string
        columns
    :rtype: pyspark.sql.DataFrame
    """
    data_df = (data_df
               .select([trim(col(c)).alias(c)
                        if ctype == 'string'
                        else c
                        for c, ctype in data_df.dtypes]))

    return data_df


def _resolve_node_entities(graph_specification,
                           spark_context,
                           entity_maps,
                           input_node_path,
                           output_node_path,
                           output_node_id=None,
                           data_format='parquet'):
    """Resolved node entities task.

    Given a `graph specification` and an `entity_map`, resolve the node
    entities to their canonical ids.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param entity_maps: Canonical id mapping.
    :type entity_maps: dict
    :param input_node_path: Path to input node files for this graph.
    :type input_node_path: str
    :param output_node_path: Path to output node files for this graph.
    :type output_node_path: str
    :param output_node_id: Output column name. If it is set to `None`, the
    output will replace the mapped column.
    :type output_node_id: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    sql_context = SQLContext(spark_context)

    # create and save node lists
    for node_kind in graph_specification.node_lists:
        # load the node list
        data = (sql_context.read
                .format(data_format)
                .option('header', 'true')
                .option('inferschema', 'true')
                .load(os.path.join(input_node_path, node_kind.safe_name)))

        # trim white spaces
        data = trim_white_spaces_from_df(data)

        # create the mapper for the node list
        entity_mapper = EntityMapper(
            spark_context=spark_context,
            entity_map=entity_maps.get(node_kind.safe_name)
        )

        # insert the canonical id for the node list
        data = entity_mapper.apply(data,
                                   on=node_kind.index_column.safe_name,
                                   to=output_node_id)

        (data.write
         .format(data_format)
         .mode(saveMode='overwrite')
         .save(os.path.join(output_node_path, node_kind.safe_name)))


def _resolve_edge_entities(graph_specification,
                           spark_context,
                           entity_maps,
                           input_edge_path,
                           output_edge_path,
                           output_edge_source_id=None,
                           output_edge_target_id=None,
                           data_format='parquet'):
    """Resolved node entities task.

    Given a `graph specification` and an `entity_map`, resolve the edge
    entities to their canonical ids.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_context: Spark context.
    :type spark_context: pyspark.SparkContext
    :param entity_maps: Canonical id mapping.
    :type entity_maps: dict
    :param input_edge_path: Path to input edge files for this graph.
    :type input_edge_path: str
    :param output_edge_path: Path to output edge files for this graph.
    :type output_edge_path: str
    :param output_edge_source_id: Output column name. If it is set to `None`,
    the output will replace the mapped column.
    :type output_edge_source_id: str
    :param output_edge_target_id: Output column name. If it is set to `None`,
    the output will replace the mapped column.
    :type output_edge_target_id: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    sql_context = SQLContext(spark_context)

    # create and save edge lists
    for edge_kind in graph_specification.edge_lists:
        # load the edge list
        data = (sql_context.read
                .format(data_format)
                .option('header', 'true')
                .option('inferschema', 'true')
                .load(os.path.join(input_edge_path, edge_kind.safe_name)))

        # trim white spaces
        data = trim_white_spaces_from_df(data)

        # create the mapper for the edge list
        entity_mapper = EntityMapper(
            spark_context=spark_context,
            entity_map=entity_maps.get(edge_kind.safe_name),
        )

        # insert canonical id for source and target columns
        data = entity_mapper.apply(data,
                                   on=edge_kind.source_column.safe_name,
                                   to=output_edge_source_id)

        data = entity_mapper.apply(data,
                                   on=edge_kind.target_column.safe_name,
                                   to=output_edge_target_id)

        (data.write
         .format(data_format)
         .mode(saveMode='overwrite')
         .save(os.path.join(output_edge_path, edge_kind.safe_name)))


def resolve_node_entities(graph_specification,
                          spark_config,
                          entity_maps,
                          input_node_path,
                          output_node_path,
                          output_node_id=None,
                          data_format='parquet'):
    """Runner for resolve node entities task that sets up the SparkContext.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param entity_maps: Canonical id mapping.
    :type entity_maps: dict
    :param input_node_path: Path to input node files for this graph.
    :type input_node_path: str
    :param output_node_path: Path to output node files for this graph.
    :type output_node_path: str
    :param output_node_id: Output column name. If it is set to `None`, the
    output will replace the mapped column.
    :type output_node_id: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    with get_spark_context(spark_config.create()) as spark:
        _resolve_node_entities(graph_specification=graph_specification,
                               spark_context=spark,
                               entity_maps=entity_maps,
                               input_node_path=input_node_path,
                               output_node_path=output_node_path,
                               output_node_id=output_node_id,
                               data_format=data_format)


def resolve_edge_entities(graph_specification,
                          spark_config,
                          entity_maps,
                          input_edge_path,
                          output_edge_path,
                          output_edge_source_id=None,
                          output_edge_target_id=None,
                          data_format='parquet'):
    """Runner for resolve edge entities task that sets up the SparkContext.

    :param graph_specification: Graph specification.
    :type graph_specification: fncore.utils.graph_specification.GraphSpec
    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param entity_maps: Canonical id mapping.
    :type entity_maps: dict
    :param input_edge_path: Path to input edge files for this graph.
    :type input_edge_path: str
    :param output_edge_path: Path to output edge files for this graph.
    :type output_edge_path: str
    :param output_edge_source_id: Output column name. If it is set to `None`,
    the output will replace the mapped column.
    :type output_edge_source_id: str
    :param output_edge_target_id: Output column name. If it is set to `None`,
    the output will replace the mapped column.
    :type output_edge_target_id: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str

    """
    with get_spark_context(spark_config.create()) as spark:
        _resolve_edge_entities(graph_specification=graph_specification,
                               spark_context=spark,
                               entity_maps=entity_maps,
                               input_edge_path=input_edge_path,
                               output_edge_path=output_edge_path,
                               output_edge_source_id=output_edge_source_id,
                               output_edge_target_id=output_edge_target_id,
                               data_format=data_format)
