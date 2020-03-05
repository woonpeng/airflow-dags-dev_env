"""Finnet graph db restore pipeline.

This is a generic graph db restore DAG. It reads formatted JSON files at
`PIPELINE_SPEC_PATH` and builds the restore DAGs based on the configuration
defined in each file.
"""

# pylint: disable=invalid-name

import json
import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from fncore.tasks import neo4j_manager
from fncore.utils.graph_specification import GraphSpec

PIPELINE_SPEC_PATH = os.environ.get('PIPELINE_SPEC_PATH', '.')
PIPELINE_ARGS = {'owner': 'finnet',
                 'depends_on_past': False,
                 'start_date': datetime(2017, 1, 1),
                 'email': ['finnet@data.gov.sg'],
                 'email_on_failure': False,
                 'email_on_retry': False,
                 'retries': 3,
                 'retry_delay': timedelta(minutes=3),
                 'retry_exponential_backoff': True,
                 'max_retry_delay': timedelta(minutes=15)}
# TODO: Use Airflow 1.8.2 when possible as it implements a randomised
# exponential backoff.
# More details: https://issues.apache.org/jira/browse/AIRFLOW-1036

# fetch all graph specs
graph_specs = []
spec_files = [os.path.join(PIPELINE_SPEC_PATH, spec_file)
              for spec_file
              in os.listdir(PIPELINE_SPEC_PATH)
              if spec_file.endswith('.json')]

# validate each graph spec
for spec_file in spec_files:
    try:
        with open(spec_file) as spec_data:
            graph_spec = GraphSpec.from_dict(json.load(spec_data))
            graph_specs.append(graph_spec)
    except (ValueError, IOError, KeyError) as e:
        logging.warning(e)

# build pipeline for each graph spec
for graph_spec in graph_specs:
    dag_id = 'fn_{0}_graphdb_restore'.format(graph_spec.name)

    dag = DAG(dag_id,
              default_args=PIPELINE_ARGS,
              schedule_interval=None)

    globals()[dag_id] = dag

    restore_backup = PythonOperator(
        python_callable=neo4j_manager.restore,
        op_args=[graph_spec],
        dag=dag,
        task_id='fn_{0}_restore_backup'.format(graph_spec.name),
        provide_context=True,
        priority_weight=3
    )
