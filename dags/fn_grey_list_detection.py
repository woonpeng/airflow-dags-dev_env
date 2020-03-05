# coding=utf-8
"""
Finnet Grey List detection pipeline ver. 1

1. Objective:

   This dag aims to generate an output table, which contains all grey list
   exceptions on the execution date of the dag.

2. What is Grey List exception?

   There is a black list of business entities, which no government subbusiness
   units are supposed to do procurement with. Business entities are related to
   other business entities through various ways. For example, they may share
   the same bank accounts, they may have common directors, etc. All business
   entities, which are related reasonably (the number of hops in the relation
   is less than or equal to four) to a company which is blacklisted and are
   transacted with subbusiness units form the Grey List.

3. Desired Output:

   Each row in the output table shows one path between SBU and business entity
   what is either black-listed or grey-listed in the below schema:

        |-- voucher_id: str
        |-- subbusiness_unit: str
        |-- vendor: str
        |-- debarred_vendor: str
        |-- debarred_from_dt: date
        |-- debarred_to_dt: date
        |-- gross_amt: decimal
        |-- invoice_dt: date
        |-- degree_separation: int
        |-- node_types: str
        |-- cycle_viz: str
        |-- transactions_date: str

4. How to find all potential Grey List exceptions?

   First, we construct a heterogeneous graph, where the the node types include
   Business, Dependant, Division, Officer, Person (acra officer), Shareholder,
   Subbusiness_Unit.

   The edge types includes Division being connected to an Officer via the cost
   centre (Dept); Division is connected to Subbusiness_Unit, Officer connected
   to a Dependant. Officer connected to another Officer via the same address;
   Person (acra officer) connected to a Business; Shareholder connected to a
   Business; Business connected to another Business via the bank, branch and
   bank account.

   The graph is weightless.

   Some graph transformations are done to simplify the graph.
   1. Officer is related to a Division through cost centre
   2. Officer is related to another Officer through address
   3. Business is related to another Business through bank, branch and bank account.

   Find the shortest path from any Business that transacted with the Subbusiness_Unit
   to any Business that is grey listed using bi_direction_bfs_any.
   The computed shortest paths are then reformatted according to the desired schema.

5. Tasks in the dag

   There is only one task in the dag to run `grey_list_detection`.

   1. Collects all the relevant edges.
   2. Finds the shortest paths.
   3. Formats the paths according to the desired schema and write to parquet.
"""

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from fncore.agd.tasks import grey_list
from fncore.utils.cron_config_tools import compute_days_between_runs
from fncore.utils.graph_specification import GraphSpec
from fncore.utils.spark_tools import SparkConfFactory

# DEBUG = True
DEBUG = True if os.environ.get('DEBUG', 'False') == 'True' else False
DATA_PATH = os.environ['PIPELINE_DATA_PATH']
DATA_FORMAT = os.environ['PIPELINE_DATA_FORMAT']
SPEC_NAME = 'figov_1.json'
SPEC_FILE = os.path.join(os.environ.get('PIPELINE_SPEC_PATH', '.'), SPEC_NAME)

PIPELINE_ARGS = {
    'owner': 'finnet',
    'depends_on_past': False,
    'start_date': datetime(2017, 1, 1),
    'email': ['finnet@data.gov.sg'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}


# pylint: disable=invalid-name
try:
    with open(SPEC_FILE) as spec_data:
        graph_spec = GraphSpec.from_dict(json.load(spec_data))
except (ValueError, IOError, KeyError) as e:
    logging.warning(e)
    graph_spec = None


if graph_spec:
    dag = DAG('fn_figov_grey_list_detection',
              catchup=True,
              default_args=PIPELINE_ARGS,
              schedule_interval=graph_spec.poll)

    num_days_between_runs = compute_days_between_runs(graph_spec.poll)

    # 1. Create task to wait for data ingestion
    grey_list_wait_for_data_ingestion = ExternalTaskSensor(
        external_dag_id='fn_figov_coi_detection',
        external_task_id='fn_figov_coi_results_writer_'
                         'minus_{}_days'.format(num_days_between_runs - 1),
        allowed_states=['success'],
        dag=dag,
        task_id='fn_figov_grey_list_wait_for_coi_completion',
        timeout=60,
        retries=12*60,
        priority_weight=0
    )

    # 2. Create task for grey list detection
    grey_list_results_dir = os.path.join(
        DATA_PATH,
        graph_spec.name,
        'grey_list_results'
    )

    for offset_day in range(num_days_between_runs):
        grey_list_detection = PythonOperator(
            python_callable=grey_list.grey_list_detection,
            dag=dag,
            task_id='fn_figov_grey_list_detection_'
                    'minus_{}_days'.format(offset_day),
            provide_context=True,
            priority_weight=2,
            op_kwargs={
                'offset_day': offset_day,
                'graph_spec': graph_spec,
                'node_resolved_dir': os.path.join(DATA_PATH,
                                                  graph_spec.name,
                                                  'node_list_resolved'),
                'edge_resolved_dir': os.path.join(DATA_PATH,
                                                  graph_spec.name,
                                                  'edge_list_resolved'),
                'results_dir': grey_list_results_dir,
                'data_format': DATA_FORMAT,
                'spark_config': (
                    SparkConfFactory()
                    .set_master('yarn-client')
                    .set_app_name('fn_grey_list_detection')
                    .set("spark.scheduler.revive.interval", 3)
                    .set("spark.task.maxFailures", 4)
                    .set("spark.executor.instances", 2 if DEBUG else 10)
                    .set("spark.executor.cores", 3)
                    .set("spark.executor.memory", "5g" if DEBUG else "16g")
                    .set("spark.driver.maxResultSize", "4g" if DEBUG else "8g")
                    .set("spark.memory.fraction", "0.5")
                ),
                'max_path_len': 3
            }
        )

        # 3. Create task to write result to database
        jdbc_url = (
            'jdbc:{scheme}://{host}:{port};'
            'database={dbname};user={user};password={pw}'
            .format(
                scheme=os.environ['RESULTS_DB_SCHEME'],
                host=os.environ['RESULTS_DB_HOST'],
                port=os.environ['RESULTS_DB_PORT'],
                dbname=os.environ['RESULTS_DB_NAME'],
                user=os.environ['RESULTS_DB_USER'],
                pw=os.environ['RESULTS_DB_PASS']
            )
        )

        grey_list_results_writer = PythonOperator(
            python_callable=grey_list.grey_list_results_writer,
            dag=dag,
            task_id='fn_figov_grey_list_results_'
                    'writer_minus_{}_days'.format(offset_day),
            provide_context=True,
            priority_weight=2,
            op_kwargs={
                'offset_day': offset_day,
                'grey_list_results_dir': grey_list_results_dir,
                'jdbc_url': jdbc_url,
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'grey_list_table_name': 'grey_list_results',
                'spark_config': (
                    SparkConfFactory()
                    .set_master('yarn-client')
                    .set_app_name('fn_grey_list_results_writer')
                    .set("spark.scheduler.revive.interval", 3)
                    .set("spark.task.maxFailures", 4)
                    .set("spark.executor.instances", 2 if DEBUG else 4)
                    .set("spark.executor.cores", 3)
                    .set("spark.executor.memory", "5g" if DEBUG else "10g")
                    .set("spark.driver.maxResultSize", "4g" if DEBUG else "8g")
                    .set("spark.memory.fraction", "0.5")
                )
            }
        )

        # 4. Create pipeline
        grey_list_detection.set_upstream(grey_list_wait_for_data_ingestion)
        grey_list_results_writer.set_upstream(grey_list_detection)

        # Can remove when there are sufficient compute resources
        # pylint: disable=used-before-assignment
        if offset_day > 0:
            grey_list_detection.set_upstream(prev_grey_list_results_writer)

        prev_grey_list_results_writer = grey_list_results_writer
