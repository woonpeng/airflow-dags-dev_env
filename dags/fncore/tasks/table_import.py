"""
This module defines the airflow function to import relevant table from a
specified database into the computing / hdfs cluster. The airflow function /
task makes use of Spark to perform the ingestation of the data from the
database, and is defined as a PythonOperator in airflow.

The database is imported into the HDFS at a location specified by the
environment variable ``PIPELINE_DATA_PATH`` and in a sub-directory
corresponding to the graph name. Within the sub-directory, it is further split
into the directories

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

The information needed to import the table from the database is completely
specified in a graph specification and is extracted during the construction
of the airflow pipeline. For more information, refer to the documentation in
the DAG construction module

Please refer to the documentation for more information regarding the graph
specifications
"""

# pylint: disable=no-name-in-module

from pyspark.sql import SQLContext
from pyspark.sql.functions import col

from fncore.utils.spark_tools import get_spark_context


# pylint: disable=too-many-arguments
def import_jdbc_table(spark_config,
                      uri,
                      input_table,
                      input_cols,
                      output_table,
                      output_cols,
                      driver,
                      data_format,
                      debug=False):
    """
    Reads a table from microsoft sql server and writes the relevant columns
    into a parquet file.

    :param spark_config: Spark config.
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param uri: database connection string
    :type uri: str
    :param input_table: table to import
    :type input_table: str
    :param input_cols: list of columns to import
    :type input_cols: List[str]
    :param output_table: path to save the imported table
    :type output_table: str
    :param output_cols: columns names for saving the imported table
    :type output_cols: List[str]
    :param driver: jdbc driver to use
    :type driver: str
    :param data_format: Format to read and write files for this graph.
    :type data_format: str
    :param debug: flag to sample a small fraction of the data
    :type debug: bool
    """
    with get_spark_context(spark_config.create()) as spark_context:
        sql_context = SQLContext(spark_context)

        # Make use of pushdown optimization to read only columns needed
        # https://docs.databricks.com/spark/latest/data-sources/sql-databases.html
        df_table = (sql_context.read
                    .jdbc(url=uri,
                          table=input_table,
                          properties={'driver': driver})
                    .select([col(c).alias(sc)
                             for c, sc
                             in zip(input_cols, output_cols)]))

        if debug:
            df_table = df_table.sample(False, 0.025)

        (df_table.write
         .format(data_format)
         .mode(saveMode='overwrite')
         .save(output_table))
