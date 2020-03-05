# coding=utf-8
"""
This task performs grey list analysis, formats the results
and writes them to a parquet.
"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
import datetime
import functools
import os

from pyspark.sql import DataFrame, SQLContext
from pyspark.sql.functions import array, col, lit

from fncore.agd.utils.agd_graph_utils import (debarred_vendor_table,
                                              division_vendor_transactions_table,
                                              load_agd_greylist_edges)
from fncore.agd.utils.agd_viz_utils import (udf_generate_list_node_types,
                                            udf_generate_viz_url_combine,
                                            udf_generate_viz_url_prefix,
                                            unique_paths_with_main_viz)
from fncore.utils.dataframe_graph_tools import bi_direction_bfs_any
from fncore.utils.graphframe_tools import load_node_edge_lists
from fncore.utils.hdfs import check_file_exists_hdfs
from fncore.utils.spark_tools import get_spark_context


# pylint: disable=too-many-arguments
def _process_grey_list_viz(grey_list_result,
                           transactions_date,
                           transacted_vendor,
                           debarred_vendor):
    """
    Processes the link to the visualization for the result and returns
    a dataframe with the relevant columns for conflict of interest analysis.

    :param grey_list_result: result from bfs for grey list detection;
        paths satisfying any 'start' in `src_df` and any 'end' in `dst_df` of
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
    :type grey_list_result: pyspark.sql.DataFrame
    :param transactions_date: date of transactions
    :type transactions_date: str
    :param transacted_vendor: dataframe of transactions with the schema:
             | |-- division: string (nullable = true)
             | |-- vendor: string (nullable = true)
             | |-- subbusiness_unit: string (nullable = true)
             | |-- voucher_id: string (nullable = true)
             | |-- gross_amt: decimal (nullable = true)
             | |-- invoice_dt: date (nullable = true)
    :type transacted_vendor: pyspark.sql.DataFrame
    :param debarred_vendor: dataframe of the debarred vendors with the schema:
             | |-- debarred_vendor: string (nullable = true)
             | |-- debarred_from_dt: date (nullable = true)
             | |-- debarred_to_dt: date (nullable = true)
    :type debarred_vendor: pyspark.sql.DataFrame
    :return: `grey_list_result`, properly formatted dataframe for
        further analysis; follows the schema below:
            | |-- transactions_date: str
            | |-- voucher_id: str
            | |-- subbusiness_unit: str
            | |-- division: str
            | |-- vendor: str
            | |-- debarred_vendor: str
            | |-- debarred_from_dt: date
            | |-- debarred_to_dt: date
            | |-- gross_amt: decimal(27,3)
            | |-- invoice_dt: date
            | |-- degree_separation: int
            | |-- node_types: str
            | |-- cycle_viz: str
    :rtype: pyspark.sql.DataFrame
    """
    grey_list_result = unique_paths_with_main_viz(grey_list_result)

    e_columns = sorted([column
                        for column in grey_list_result.columns
                        if column[0] == 'e'])
    degree = len(e_columns)

    grey_list_result = grey_list_result.withColumn(
        'degree_separation',
        lit(degree)
    )
    grey_list_result = (
        grey_list_result
        .withColumn('vendor', col('from.id'))
        .withColumn('debarred_vendor', col('to.id'))
        .join(transacted_vendor, on='vendor')
    )

    cycle_viz_prefix = (udf_generate_viz_url_prefix()('subbusiness_unit'))
    grey_list_result = (
        grey_list_result.withColumn('cycle_viz_prefix', cycle_viz_prefix)
    )

    cycle_viz_udf = udf_generate_viz_url_combine()
    cycle_viz = cycle_viz_udf('cycle_viz_prefix', 'cycle_viz_main')
    grey_list_result = grey_list_result.withColumn('cycle_viz', cycle_viz)

    node_types = udf_generate_list_node_types()(array(e_columns))
    grey_list_result = (
        grey_list_result
        .withColumn('node_types', node_types)
        .withColumn('transactions_date', lit(transactions_date))
        .join(debarred_vendor, on='debarred_vendor')
    )

    grey_list_result = (
        grey_list_result.select(
            'transactions_date', 'account_code', 'voucher_id',
            'subbusiness_unit', 'division', 'vendor', 'vendor_name',
            'debarred_vendor', 'debarred_vendor_name', 'debarred_from_dt',
            'debarred_to_dt', 'gross_amt', 'invoice_dt', 'fiscal_year',
            'degree_separation', 'node_types', 'cycle_viz'
        )
    )
    grey_list_result = grey_list_result.distinct()

    return grey_list_result


def _get_direct_transaction(transactions_date,
                            transacted_vendor,
                            debarred_vendor):
    """
    Get direct transactions between subbusiness unit and debarred vendors

    :param transactions_date: date of transactions
    :type transactions_date: str
    :param transacted_vendor: dataframe of transactions with the schema:
             | |-- division: string (nullable = true)
             | |-- vendor: string (nullable = true)
             | |-- subbusiness_unit: string (nullable = true)
             | |-- voucher_id: string (nullable = true)
             | |-- gross_amt: decimal (nullable = true)
             | |-- invoice_dt: date (nullable = true)
    :type transacted_vendor: pyspark.sql.DataFrame
    :param debarred_vendor: dataframe of the debarred vendors with the schema:
             | |-- debarred_vendor: string (nullable = true)
             | |-- debarred_from_dt: date (nullable = true)
             | |-- debarred_to_dt: date (nullable = true)
    :type debarred_vendor: pyspark.sql.DataFrame
    :return: dataframe containing final results, with the below schema:
             | |-- transactions_date: str
             | |-- voucher_id: str
             | |-- subbusiness_unit: str
             | |-- division: str
             | |-- vendor: str
             | |-- debarred_vendor: str
             | |-- debarred_from_dt: date
             | |-- debarred_to_dt: date
             | |-- gross_amt: decimal(27,3)
             | |-- invoice_dt: date
             | |-- degree_separation: int
             | |-- node_types: str
             | |-- cycle_viz: str
    :rtype: pyspark.sql.DataFrame
    """
    one_hop = (
        transacted_vendor.join(
            debarred_vendor,
            transacted_vendor['vendor'] == debarred_vendor['debarred_vendor'],
            how='inner'
        )
    )

    one_hop = one_hop.withColumn('degree_separation', lit(1))

    cycle_viz_part1 = udf_generate_viz_url_prefix()('subbusiness_unit')
    one_hop = one_hop.withColumn('cycle_viz_part1', cycle_viz_part1)

    cycle_viz_part2 = udf_generate_viz_url_prefix()('debarred_vendor')
    one_hop = one_hop.withColumn('cycle_viz_part2', cycle_viz_part2)

    cycle_viz_udf = udf_generate_viz_url_combine()
    cycle_viz = cycle_viz_udf('cycle_viz_part1', 'cycle_viz_part2')
    one_hop = one_hop.withColumn('cycle_viz', cycle_viz)

    one_hop = (
        one_hop
        .withColumn('node_types', lit('Subbusiness_Unit, Business'))
        .withColumn('transactions_date', lit(transactions_date))
    )

    one_hop = (
        one_hop.select(
            'transactions_date', 'account_code', 'voucher_id',
            'subbusiness_unit', 'division', 'vendor', 'vendor_name',
            'debarred_vendor', 'debarred_vendor_name', 'debarred_from_dt',
            'debarred_to_dt', 'gross_amt', 'invoice_dt', 'fiscal_year',
            'degree_separation', 'node_types', 'cycle_viz'
        )
    )
    one_hop = one_hop.distinct()

    return one_hop


# pylint: disable=unused-argument
# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
def grey_list_detection(offset_day, graph_spec, node_resolved_dir,
                        edge_resolved_dir, results_dir, data_format,
                        spark_config, max_path_len, execution_date, **kwargs):
    """
    Entry point of the airflow task: grey_list_detection.
    kwargs contains additional information that airflow passes in.

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

        tables = load_node_edge_lists(
            sql_context,
            graph_spec,
            node_resolved_dir,
            edge_resolved_dir,
            data_format
        )

        # 1. Get relevant edges
        edges = load_agd_greylist_edges(
            tables, graph_spec, transactions_date
        )
        edges = edges.cache()

        # 2. Get vendor subbusiness transactions for a `transactions_date`
        # as well as the debarred vendors for the `transactions_date`
        debarred_vendor = debarred_vendor_table(
            tables, transactions_date, graph_spec
        )
        transacted_vendor = division_vendor_transactions_table(
            tables, transactions_date, graph_spec
        )

        src_df = (
            transacted_vendor
            .select(col('vendor').alias('start'))
            .distinct()
            .cache()
        )
        dst_df = (
            debarred_vendor
            .select(col('debarred_vendor').alias('end'))
            .distinct()
            .cache()
        )

        # 3. Properly format the results from breadth first search
        if src_df.count() > 0 and dst_df.count() > 0:
            list_grey_list_results = []

            one_hop = _get_direct_transaction(
                transactions_date,
                transacted_vendor,
                debarred_vendor
            )

            if one_hop.take(1):
                list_grey_list_results.append(one_hop)

            for grey_list_result in bi_direction_bfs_any(
                    edges, src_df, dst_df, max_path_len=max_path_len):

                list_grey_list_results.append(
                    _process_grey_list_viz(
                        grey_list_result,
                        transactions_date,
                        transacted_vendor,
                        debarred_vendor
                    )
                )

            if list_grey_list_results:
                grey_list_results = (
                    functools.reduce(DataFrame.unionAll, list_grey_list_results)
                )

                # 4. Write the results to parquet
                results_filename = (
                    "grey_list_results_{}.parquet".format(
                        execution_date.strftime('%Y_%m_%d')
                    )
                )
                results_path = os.path.join(results_dir, results_filename)
                grey_list_results.write.parquet(results_path, mode='overwrite')


def grey_list_results_writer(offset_day, grey_list_results_dir,
                             jdbc_url, driver, grey_list_table_name,
                             spark_config, execution_date, **kwargs):
    """
    Entry point of the airflow task: grey_list_csv_writer. Writes the results
    to a csv file in hdfs. kwargs contains additional information that
    airflow passes to the function.

    :param offset_day: number of days offset from the execution date
    :type offset_day: int
    :param grey_list_results_dir: path to the coi results for execution date
    :type grey_list_results_dir: str
    :param jdbc_url: url to the microsoft sql server
    :type jdbc_url: str
    :param driver: jdbc driver to use
    :type driver: str
    :param grey_list_table_name: name of table to save the results to
    :type grey_list_table_name: str
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

        grey_list_results_filename = (
            "grey_list_results_{}.parquet".format(
                execution_date.strftime('%Y_%m_%d')
            )
        )
        grey_list_results_path = os.path.join(
            grey_list_results_dir,
            grey_list_results_filename
        )

        if check_file_exists_hdfs(grey_list_results_path):
            grey_list_results = sql_context.read.parquet(grey_list_results_path).cache()
            grey_list_results.count()
            grey_list_results.write.mode(saveMode='append').jdbc(
                url=jdbc_url,
                table=grey_list_table_name,
                properties={'driver': driver}
            )
