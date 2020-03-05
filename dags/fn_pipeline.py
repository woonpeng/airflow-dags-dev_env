"""Finnet data pipeline.

This is a generic graph construction DAG builder. It reads formatted JSON files
at `PIPELINE_SPEC_PATH` and builds graph construction DAGs based on the
configuration defined in each file.

The graph data is imported into HDFS at a location specified by the environment
variable`PIPELINE_DATA_PATH`. Each graph's data is stored in a sub-folder named
after its `graph_name` property. Each graph's sub-folder follows the structure:

- `tables`: which contains the imported tables from the database necessary to
build the graph.
- `node_list`: which contains the files corresponding to each node list as
specified in the graph specification.
- `edge_list`: which contains the files corresponding to each edge list as
specified in the graph specification.
- `node_list_resolved`: which contains the files of the node lists with entries
referring to the same entity marked by having the same canonical id.
- `edge_list_resolved`: which contains the files of the edge lists with entries
referring to the same entity marked by having the same canonical id.

"""

# pylint: disable=invalid-name

import json
import logging
import os
from datetime import datetime, timedelta
from uuid import uuid4

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from fncore.airflow.operators.Neo4jSensorOperator import Neo4jSensorOperator
from fncore.tasks import (build_edge_lists, build_node_lists, graph_to_neo4j,
                          neo4j_manager, neo4j_writer, resolve_edge_entities,
                          resolve_node_entities)
from fncore.tasks.table_import import import_jdbc_table
from fncore.utils.graph_specification import GraphSpec
from fncore.utils.spark_tools import SparkConfFactory

# DEBUG = True
DEBUG = True if os.environ.get('DEBUG', 'False') == 'True' else False

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
DATA_PATH = os.environ['PIPELINE_DATA_PATH']
DATA_FORMAT = os.environ['PIPELINE_DATA_FORMAT']

logging.info('Starting FN pipelines.')

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

# Get neo4j ssh username and port
neo4j_ssh_username = os.environ['NEO4J_SSH_USERNAME']
neo4j_ssh_port = int(os.environ['NEO4J_SSH_PORT'])


# build pipeline for each graph spec
for graph_spec in graph_specs:
    dag_id = 'fn_{0}_pipeline'.format(graph_spec.name)
    tables_path = os.path.join(DATA_PATH, graph_spec.name, 'tables')
    node_path = os.path.join(DATA_PATH, graph_spec.name, 'nodes')
    edge_path = os.path.join(DATA_PATH, graph_spec.name, 'edges')
    node_path_resolved = os.path.join(DATA_PATH,
                                      graph_spec.name,
                                      'node_list_resolved')
    edge_path_resolved = os.path.join(DATA_PATH,
                                      graph_spec.name,
                                      'edge_list_resolved')

    dag = DAG(dag_id,
              default_args=PIPELINE_ARGS,
              schedule_interval=graph_spec.poll)

    globals()[dag_id] = dag

    # Create task operators
    import_tasks = dict()
    for table, columns in graph_spec.table_details['tables'].items():
        # airflow task id. also used as the task name
        task_id = ('fn_{graph_name}_import_{safe_table_name}'
                   .format(graph_name=graph_spec.name,
                           safe_table_name=table[1]))

        # path to save the imported table
        output_path = ('{data_path}/{graph_name}/tables/{safe_table_name}'
                       .format(data_path=DATA_PATH,
                               graph_name=graph_spec.name,
                               safe_table_name=table[1]))

        # create the import task and add it to the import task collection
        import_tasks[table[1]] = PythonOperator(
            task_id=task_id,
            python_callable=import_jdbc_table,
            dag=dag,
            op_kwargs={
                'uri': graph_spec.data_uri,
                'input_table': table[0],
                'input_cols': [column[0] for column in columns],
                'output_table': output_path,
                'output_cols': [column[1] for column in columns],
                'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                'data_format': DATA_FORMAT,
                'debug': DEBUG,
                'spark_config': (
                    SparkConfFactory()
                    .set_master('yarn-client')
                    .set_app_name(task_id)
                    .set("spark.scheduler.revive.interval", 3)
                    .set("spark.task.maxFailures", 4)
                    .set("spark.executor.instances", 2 if DEBUG else 6)
                    .set("spark.executor.cores", 3)
                    .set("spark.executor.memory", "5g" if DEBUG else "8g")
                    .set("spark.driver.maxResultSize", "4g" if DEBUG else "8g")
                )
            },
            priority_weight=2
        )

    build_nodes_task = PythonOperator(
        task_id='fn_{0}_build_nodes'.format(graph_spec.name),
        python_callable=build_node_lists,
        dag=dag,
        op_kwargs={
            'graph_specification': graph_spec,
            'tables_path': tables_path,
            'node_path': node_path,
            'data_format': DATA_FORMAT,
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{}_build_lists'.format(graph_spec.name))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 2 if DEBUG else 6)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")
            )
        },
        priority_weight=2
    )

    build_edges_task = PythonOperator(
        task_id='fn_{0}_build_edges'.format(graph_spec.name),
        python_callable=build_edge_lists,
        dag=dag,
        op_kwargs={
            'graph_specification': graph_spec,
            'tables_path': tables_path,
            'edge_path': edge_path,
            'data_format': DATA_FORMAT,
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{}_build_lists'.format(graph_spec.name))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 2 if DEBUG else 6)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")
            )
        },
        priority_weight=2
    )

    resolve_nodes_task = PythonOperator(
        task_id='fn_{0}_resolve_nodes'.format(graph_spec.name),
        python_callable=resolve_node_entities,
        dag=dag,
        op_kwargs={
            'graph_specification': graph_spec,
            'entity_maps': dict(),
            'input_node_path': node_path,
            'output_node_path': node_path_resolved,
            'output_node_id': '_canonical_id',
            'data_format': DATA_FORMAT,
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{}_resolve_entities'.format(graph_spec.name))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 2 if DEBUG else 12)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")
            )
        },
        priority_weight=2
    )

    resolve_edges_task = PythonOperator(
        task_id='fn_{0}_resolve_edges'.format(graph_spec.name),
        python_callable=resolve_edge_entities,
        dag=dag,
        op_kwargs={
            'graph_specification': graph_spec,
            'entity_maps': dict(),
            'input_edge_path': edge_path,
            'output_edge_path': edge_path_resolved,
            'output_edge_source_id': '_canonical_id_source',
            'output_edge_target_id': '_canonical_id_target',
            'data_format': DATA_FORMAT,
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{}_resolve_entities'.format(graph_spec.name))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 2 if DEBUG else 12)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")
            )
        },
        priority_weight=2
    )

    backup_graphdb = PythonOperator(
        python_callable=neo4j_manager.backup,
        op_kwargs={
            'graph_specification': graph_spec,
            'username': neo4j_ssh_username,
            'port': neo4j_ssh_port,
        },
        dag=dag,
        task_id='fn_{0}_backup_graphdb'.format(graph_spec.name),
        provide_context=True,
        priority_weight=3
    )

    purge_graphdb = PythonOperator(
        python_callable=neo4j_manager.purge,
        op_kwargs={
            'graph_specification': graph_spec,
            'username': neo4j_ssh_username,
            'port': neo4j_ssh_port,
        },
        dag=dag,
        task_id='fn_{0}_purge_graphdb'.format(graph_spec.name),
        priority_weight=3
    )

    wait_for_graph_db = Neo4jSensorOperator(
        task_id='fn_{0}_wait_for_graph_db'.format(graph_spec.name),
        dag=dag,
        conn_str=graph_spec.graph_uri,
        timeout=60,
        retries=60,
        priority_weight=1
    )

    build_graphdb_csv = PythonOperator(
        task_id='fn_{0}_build_nodes_csv'.format(graph_spec.name),
        dag=dag,
        python_callable=graph_to_neo4j.graph_to_neo4j,
        op_kwargs={
            'graph_specification': graph_spec,
            'username': neo4j_ssh_username,
            'port': neo4j_ssh_port,
            'input_node_path': node_path_resolved,
            'input_edge_path': edge_path_resolved,
            'max_result_size': 0.2e9 if DEBUG else 0.4e9,
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{0}_build_csv_graphdb'
                              .format(graph_spec.name))
                .set("spark.sql.shuffle.partitions", 1000)
                .set("spark.executorEnv.HOME",
                     os.path.join('/tmp', str(uuid4())))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 2 if DEBUG else 12)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")

            )
        },
        priority_weight=2
    )

    build_graphdb_nodes = PythonOperator(
        task_id='fn_{0}_build_nodes_graphdb'.format(graph_spec.name),
        dag=dag,
        python_callable=neo4j_writer.write_neo4j_nodes,
        op_kwargs={
            'graph_specification': graph_spec.to_dict(),
            'spark_config': (
                SparkConfFactory()
                .set_master('yarn-client')
                .set_app_name('fn_{0}_build_nodes_graphdb'
                              .format(graph_spec.name))
                .set("spark.sql.shuffle.partitions", 1000)
                .set("spark.executorEnv.HOME",
                     os.path.join('/tmp', str(uuid4())))
                .set("spark.scheduler.revive.interval", 3)
                .set("spark.task.maxFailures", 4)
                .set("spark.executor.instances", 1)
                .set("spark.executor.cores", 3)
                .set("spark.executor.memory", "5g" if DEBUG else "8g")
                .set("spark.driver.maxResultSize", "2g" if DEBUG else "4g")
            )
        },
        priority_weight=2
    )

    restore_backup = PythonOperator(
        python_callable=neo4j_manager.restore,
        op_kwargs={
            'graph_specification': graph_spec,
            'username': neo4j_ssh_username,
            'port': neo4j_ssh_port,
        },
        dag=dag,
        task_id='fn_{0}_restore_backup_'
                'as_build_fail'.format(graph_spec.name),
        provide_context=True,
        trigger_rule='one_failed',
        priority_weight=2
    )


    cleanup_backup = PythonOperator(
        python_callable=neo4j_manager.cleanup,
        op_kwargs={
            'graph_specification': graph_spec,
            'username': neo4j_ssh_username,
            'port': neo4j_ssh_port,
        },
        dag=dag,
        task_id='fn_{0}_cleanup_backup'.format(graph_spec.name),
        priority_weight=0
    )

    # create pipeline
    for safe_table_name, import_task in import_tasks.items():
        if safe_table_name in [nl.safe_table_name
                               for nl in graph_spec.node_lists]:
            build_nodes_task.set_upstream(import_task)
        if safe_table_name in [el.safe_table_name
                               for el in graph_spec.edge_lists]:
            build_edges_task.set_upstream(import_task)
    resolve_nodes_task.set_upstream(build_nodes_task)
    resolve_edges_task.set_upstream(build_edges_task)
    purge_graphdb.set_upstream(backup_graphdb)
    purge_graphdb.set_upstream(resolve_nodes_task)
    purge_graphdb.set_upstream(resolve_edges_task)
    build_graphdb_csv.set_upstream(purge_graphdb)
    wait_for_graph_db.set_upstream(build_graphdb_csv)
    build_graphdb_nodes.set_upstream(wait_for_graph_db)
    restore_backup.set_upstream(build_graphdb_nodes)
    restore_backup.set_upstream(build_graphdb_csv)
    cleanup_backup.set_upstream(build_graphdb_nodes)
