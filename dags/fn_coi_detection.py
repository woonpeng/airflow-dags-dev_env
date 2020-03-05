# coding=utf-8
"""
Finnet COI detection pipeline ver. 1

1. Objective:

   This dag aims to generate an output table, which contains all conflict of
   interest exceptions on the execution date of the dag.

2. What is Conflict of Interest exception (COI)?

   It starts from a procurement from a subbusiness unit (SBU) in a government
   agency. There are two parties involved in the procurement process: SBU and
   Vendor. The question is: are these two parties related in other ways?

   If they are closely related, then there is a probability that the
   procurement may be fishy. An example of COI could be: Mr. Lim works in MOF01
   and his wife is a main shareholder of a company T & L Pte. Ltd. while MOF01
   purchases frequently from T & L Pte. Ltd.

3. Desired Output:

   Each row in the output table shows one path between SBU and business entity
   in the below schema:
        |-- voucher_id: str
        |-- subbusiness_unit: str
        |-- division: str
        |-- vendor: str
        |-- gross_amt: decimal
        |-- invoice_dt: date
        |-- degree_separation: int
        |-- node_types: str
        |-- cycle_viz: str
        |-- transactions_date: str

4. How to find all potential COIs?

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

   Find the shortest path from the Division (Division connected to the Subbusiness_Unit
   that transacted with the Business) to the Business using bi_direction_bfs_fixed.
   The computed shortest paths are then reformatted according to the desired schema.

5. Task in the dag

   There is only one task in the dag to run `coi_detection`.

   1. Collects all the relevant edges.
   2. Finds the shortest paths.
   3. Formats the paths according to the desired schema and write to parquet.
"""
# pylint: disable=import-error

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import ExternalTaskSensor

from fncore.agd.tasks import coi
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
    dag = DAG('fn_figov_coi_detection',
              catchup=True,
              default_args=PIPELINE_ARGS,
              schedule_interval=graph_spec.poll)

    # 1. Create task to wait for data ingestion
    coi_wait_for_node_ingestion = ExternalTaskSensor(
        external_dag_id='fn_{0}_pipeline'.format(graph_spec.name),
        external_task_id='fn_{0}_resolve_nodes'.format(graph_spec.name),
        allowed_states=['success'],
        dag=dag,
        task_id='fn_figov_coi_wait_for_node_ingestion',
        timeout=60,
        retries=12*60,
        priority_weight=0
    )

    coi_wait_for_edge_ingestion = ExternalTaskSensor(
        external_dag_id='fn_{0}_pipeline'.format(graph_spec.name),
        external_task_id='fn_{0}_resolve_edges'.format(graph_spec.name),
        allowed_states=['success'],
        dag=dag,
        task_id='fn_figov_coi_wait_for_edge_ingestion',
        timeout=60,
        retries=12*60,
        priority_weight=0
    )

    # 2. Create task for coi detection
    coi_results_dir = os.path.join(
        DATA_PATH,
        graph_spec.name,
        'coi_results'
    )

    num_days_between_runs = compute_days_between_runs(graph_spec.poll)
    for offset_day in range(num_days_between_runs):
        coi_detection = PythonOperator(
            python_callable=coi.coi_detection,
            dag=dag,
            task_id='fn_figov_coi_detection_'
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
                'results_dir': coi_results_dir,
                'data_format': DATA_FORMAT,
                'spark_config': (
                    SparkConfFactory()
                    .set_master('yarn-client')
                    .set_app_name('fn_coi_detection')
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

        coi_results_writer = PythonOperator(
            python_callable=coi.coi_results_writer,
            dag=dag,
            task_id='fn_figov_coi_results_writer_'
                    'minus_{}_days'.format(offset_day),
            provide_context=True,
            priority_weight=2,
            op_kwargs={
                'offset_day': offset_day,
                'coi_results_dir': coi_results_dir,
                'jdbc_url': jdbc_url,
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'coi_table_name': 'coi_results',
                'spark_config': (
                    SparkConfFactory()
                    .set_master('yarn-client')
                    .set_app_name('fn_coi_results_writer')
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
        coi_detection.set_upstream(coi_wait_for_node_ingestion)
        coi_detection.set_upstream(coi_wait_for_edge_ingestion)
        coi_results_writer.set_upstream(coi_detection)

        # Can remove when there are sufficient compute resources
        # pylint: disable=used-before-assignment
        if offset_day > 0:
            coi_detection.set_upstream(prev_coi_results_writer)

        prev_coi_results_writer = coi_results_writer
