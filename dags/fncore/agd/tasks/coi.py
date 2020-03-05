# coding=utf-8
"""
This task performs conflict of interest analysis, formats the results
and writes them to a parquet.
"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
import datetime
import functools
import os

from pyspark.sql import DataFrame, SQLContext
from pyspark.sql.functions import array, col, lit

from fncore.agd.utils.agd_graph_utils import (division_vendor_transactions_table,
                                              load_agd_coi_edges)
from fncore.agd.utils.agd_viz_utils import (udf_generate_list_node_types,
                                            udf_generate_viz_url_combine,
                                            udf_generate_viz_url_prefix,
                                            unique_paths_with_main_viz)
from fncore.utils.dataframe_graph_tools import bi_direction_bfs_fixed
from fncore.utils.graphframe_tools import load_node_edge_lists
from fncore.utils.hdfs import check_file_exists_hdfs
from fncore.utils.spark_tools import get_spark_context


def _process_coi_viz(coi_result,
                     transactions_date,
                     division_vendor_transactions):
    """
    Processes the link to the visualization for the result and returns
    a dataframe with the relevant columns for conflict of interest analysis.

    :param coi_result: result from bfs for conflict of interest detection;
        paths satisfying 'start' and 'end' in `src_dst_df` of
        shortest length; follows the schema below (only the more important
        fields are shown)
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
    :type coi_result: pyspark.sql.DataFrame
    :param transactions_date: date of transactions
    :type transactions_date: str
    :param division_vendor_transactions: a dataframe that contains division and
        vendor for transactions on given a date. The schema is as follows:
            | |-- division: str
            | |-- vendor: str
            | |-- subbusiness_unit: str
            | |-- voucher_id: str
            | |-- gross_amt: decimal(27,3)
            | |-- invoice_dt: date
    :type: pyspark.sql.DataFrame
    :return: `coi_result`, properly formatted dataframe for further analysis;
        follows the schema below:
            | |-- transactions_date: str
            | |-- voucher_id: str
            | |-- subbusiness_unit: str
            | |-- division: str
            | |-- vendor: str
            | |-- gross_amt: decimal(27,3)
            | |-- invoice_dt: date
            | |-- degree_separation: int
            | |-- node_types: str
            | |-- cycle_viz: str
    :rtype: pyspark.sql.DataFrame
    """
    coi_result = unique_paths_with_main_viz(coi_result)

    e_columns = sorted(
        [column for column in coi_result.columns if column[0] == 'e']
    )
    degree = len(e_columns)

    coi_result = coi_result.withColumn('degree_separation', lit(degree))
    coi_result = (
        coi_result
        .withColumn('division', col('from.id'))
        .withColumn('vendor', col('to.id'))
        .join(division_vendor_transactions, on=['division', 'vendor'])
    )

    cycle_viz_prefix = udf_generate_viz_url_prefix()("vendor", "subbusiness_unit")
    coi_result = coi_result.withColumn('cycle_viz_prefix', cycle_viz_prefix)

    cycle_viz_udf = udf_generate_viz_url_combine()
    cycle_viz = cycle_viz_udf('cycle_viz_prefix', 'cycle_viz_main')
    coi_result = coi_result.withColumn('cycle_viz', cycle_viz)

    node_types = udf_generate_list_node_types()(array(e_columns))
    coi_result = (
        coi_result
        .withColumn('node_types', node_types)
        .withColumn('transactions_date', lit(transactions_date))
    )

    coi_result = (
        coi_result
        .select(
            'transactions_date', 'account_code', 'voucher_id',
            'subbusiness_unit', 'division', 'vendor', 'vendor_name',
            'gross_amt', 'invoice_dt', 'fiscal_year', 'degree_separation',
            'node_types', 'cycle_viz'
        )
    )
    coi_result = coi_result.distinct()

    return coi_result


# pylint: disable=unused-argument
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
def coi_detection(offset_day, graph_spec, node_resolved_dir,
                  edge_resolved_dir, results_dir, data_format,
                  spark_config, max_path_len, execution_date, **kwargs):
    """
    Entry point of the airflow task: coi_detection.
    kwargs contains additional information that airflow passes to the function.

    :param offset_day: number of days offset from the execution date
    :type offset_day: int
    :param graph_spec: the graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param node_resolved_dir: path to the resolved node lists
    :type node_resolved_dir: str
    :param edge_resolved_dir: path to the resolved edge lists
    :type edge_resolved_dir: str
    :param results_dir: path to write the final results
    :type results_dir: str
    :param data_format: data format used in the pipeline (set to `parquet`)
    :type data_format: str
    :param spark_config: configurations for spark context
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param max_path_len: maximum length of path for bfs
    :type max_path_len: int
    :param execution_date: date of the vouchers for checking
    :type execution_date: datetime.datetime
    :return: None
    :rtype: None
    """
    execution_date = execution_date - datetime.timedelta(days=offset_day)
    transactions_date = str(execution_date.date())

    with get_spark_context(spark_config.create()) as spark_context:
        sql_context = SQLContext(spark_context)

        tables = load_node_edge_lists(sql_context,
                                      graph_spec,
                                      node_resolved_dir,
                                      edge_resolved_dir,
                                      data_format)

        # 1. Get relevant edges
        edges = load_agd_coi_edges(
            tables, graph_spec, transactions_date
        )
        edges = edges.cache()

        # 2. Get division vendor transactions for a given `transactions_date`
        division_vendor_transactions = division_vendor_transactions_table(
            tables, transactions_date, graph_spec
        )

        src_dst_df = (
            division_vendor_transactions
            .select(
                col('division').alias('start'),
                col('vendor').alias('end')
            )
        )
        src_dst_df = src_dst_df.dropna().distinct().cache()

        # 3. Properly format the results from breadth first search
        # Only performs breadth first search if src_dst_df is non-empty
        if src_dst_df.count() > 0:
            list_coi_results = list()
            for coi_result in bi_direction_bfs_fixed(
                    edges, src_dst_df, max_path_len=max_path_len):
                list_coi_results.append(
                    _process_coi_viz(
                        coi_result,
                        transactions_date,
                        division_vendor_transactions
                    )
                )

            if list_coi_results:
                coi_results = functools.reduce(
                    DataFrame.unionAll,
                    list_coi_results
                )

                # 4. Write the results to parquet
                results_filename = (
                    "coi_results_{}.parquet".format(
                        execution_date.strftime('%Y_%m_%d')
                    )
                )
                results_path = os.path.join(results_dir, results_filename)
                coi_results.write.parquet(results_path, mode='overwrite')


def coi_results_writer(offset_day, coi_results_dir, jdbc_url,
                       driver, coi_table_name,
                       spark_config, execution_date, **kwargs):
    """
    Entry point of the airflow task: coi_results_writer. Writes the results to database.
    kwargs contains additional information that airflow passes to the function.

    :param offset_day: number of days offset from the execution date
    :type offset_day: int
    :param coi_results_dir: path to the coi results for execution date
    :type coi_results_dir: str
    :param jdbc_url: url to the microsoft sql server
    :type jdbc_url: str
    :param driver: jdbc driver to use
    :type driver: str
    :param coi_table_name: name of table to save the results to
    :type coi_table_name: str
    :param spark_config: configurations for spark context
    :type spark_config: fncore.utils.spark_tools.SparkConfFactory
    :param execution_date: date of the vouchers for checking
    :type execution_date: datetime.datetime
    :return: None
    :rtype: None
    """
    execution_date = execution_date - datetime.timedelta(days=offset_day)
    with get_spark_context(spark_config.create()) as spark_context:
        sql_context = SQLContext(spark_context)

        coi_results_filename = (
            "coi_results_{}.parquet".format(
                execution_date.strftime('%Y_%m_%d')
            )
        )
        coi_results_path = os.path.join(coi_results_dir, coi_results_filename)

        if check_file_exists_hdfs(coi_results_path):
            coi_results = sql_context.read.parquet(coi_results_path).cache()
            coi_results.count()
            coi_results.write.mode(saveMode='append').jdbc(
                url=jdbc_url,
                table=coi_table_name,
                properties={'driver': driver}
            )
