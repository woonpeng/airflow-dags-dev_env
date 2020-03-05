"""
Utils for agd visualization utilities
"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
# pylint: disable=too-many-locals
import urllib

from pyspark.sql.functions import array
from pyspark.sql.functions import min as pyspark_min
from pyspark.sql.functions import monotonically_increasing_id, udf
from pyspark.sql.types import ArrayType, StringType


def generate_viz_url_main(columns):
    """
    Extract entities from an array of columns and return a list of entities.
    An edge which is transformed will be reconstructed back to the original
    edge.

    :param columns: list of columns
    :type columns: pyspark.sql.functions.array of column
    :return: list of entities, which appear in the visualization url
    :rtype: list of str
    """
    entities = list()
    for column in columns:
        entities.append(column.src)
        if column.Type in ['address', 'bank-account', 'cost-centre']:
            entities.append(column.relationship)
    entities.append(columns[-1].dst)

    return entities


def generate_viz_url(prefix, main):
    """
    Concatenate entities in part1 and part2 and build the viz url.

    :param prefix: list of entities to be inserted in front of the main list
                   of entities in the viz url
    :type prefix: list of str
    :param main: list of entities in the viz url
    :type main: list of str
    :return: viz url
    :rtype: str
    """

    return build_url(entities=prefix + main)


def udf_generate_viz_url_main():
    """
    udf wrapper for generate_viz_url_main

    :return: wrapper for generate_viz_url_main
    :rtype: pyspark.sql.functions.udf
    """

    return udf(f=generate_viz_url_main, returnType=ArrayType(StringType()))


def udf_generate_viz_url_combine():
    """
    udf wrapper for generate_viz_url

    :return: wrapper for generate_viz_url
    :rtype: pyspark.sql.functions.udf
    """

    return udf(
        f=generate_viz_url,
        returnType=StringType()
    )


def generate_viz_url_prefix(*args):
    """
    construct a list with necessary prefixes

    :param args: list of entities, which will form the prefix of the viz url
    :type args: list of str
    :return: list of entities, which will form the prefix of the viz url
    :rtype: list of str
    """

    return [arg for arg in args]


def udf_generate_viz_url_prefix():
    """
    udf wrapper for generate_viz_url_prefix

    :return: wrapper for generate_viz_url_prefix
    :rtype: pyspark.sql.functions.udf
    """

    return udf(
        f=generate_viz_url_prefix,
        returnType=ArrayType(StringType())
    )


def generate_list_node_types(columns):
    """
    Extract node entities from the list

    :param columns: list of columns
    :type columns: pyspark.sql.functions.array
    :return: list of types of entities in the viz url
    :rtype: list of str
    """
    entities = 'Subbusiness_Unit, '
    for column in columns:
        entities += (column.Source_Type + ', ')
        if column.Type == 'address':
            entities += ('Address' + ', ')
        elif column.Type == 'bank-account':
            entities += ('BankAccount' + ', ')
        elif column.Type == 'cost-centre':
            entities += ('Department' + ', ')
    entities += columns[-1].Target_Type

    return entities


def udf_generate_list_node_types():
    """
    udf wrapper for generate_list_node_types

    :return: wrapper for generate_list_node_types
    :rtype: pyspark.sql.functions.udf
    """

    return udf(f=generate_list_node_types, returnType=StringType())


def build_url(entities):
    """
    Generate the viz path from the address, port, list of entities and the icon
    of the viz button

    :param entities: list of entities to appear in the viz path
    :type entities: list of str
    :return: viz url
    :rtype: string
    """

    return '/?p={query}'.format(query='+'.join(['"' + urllib.quote(entity) + '"'
                                                for entity in entities]))


def unique_paths_with_main_viz(paths):
    """
    Return unique paths with additional 'cycle_viz_main' column.

    :param paths: paths follows the schema below
        (only the more important fields are shown)
        | |-- from: pyspark.sql.StructType
        | |-- -- id: str
        | |-- e0: pyspark.sql.StructType
        | |-- -- src: str
        | |-- -- dst: str
        | |-- v1: pyspark.sql.StructType
        | |-- -- id: str
        | .
        | .
        | .
        | |-- to: pyspark.sql.StructType
        | |-- -- id: str
    :type paths: pyspark.sql.DataFrame
    :return: `unique_paths`,
        unique paths satisfying follows the schema below
        (only the more important fields are shown)
            | |-- from: pyspark.sql.StructType
            | |-- -- id: str
            | |-- e0: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- v1: pyspark.sql.StructType
            | |-- -- id: str
            | .
            | .
            | .
            | |-- to: pyspark.sql.StructType
            | |-- -- id: str
            | |-- cycle_viz_main: str
    :rtype: pyspark.sql.DataFrame
    """
    e_columns = sorted(
        [column for column in paths.columns if column[0] == 'e']
    )

    cycle_viz_main = udf_generate_viz_url_main()(array(e_columns))
    paths = (
        paths
        .withColumn('cycle_viz_main', cycle_viz_main)
        .withColumn('index', monotonically_increasing_id())
    )

    unique_indices = (
        paths
        .select('index', 'cycle_viz_main')
        .groupby('cycle_viz_main').agg(pyspark_min("index").alias("index"))
        .select('index')
    )

    unique_paths = paths.join(unique_indices, on='index').drop('index')

    return unique_paths
