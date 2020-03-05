# coding=utf-8
"""
This module defines the airflow function to insert the data into the neo4j
database.

"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
from fncore_py3.utils.neo4j_conf import get_neo4j_context
from fncore_py3.tasks.graph_to_neo4j import (get_combined_nodes,
                                             get_combined_edges)

from fncore_py3.utils.neo4j_tools import (commit_graphdb,
                                          generate_merge_node_statement_from_row,
                                          generate_merge_edge_statement_from_row)

from numpy import ceil
import os


def write_statements(conf, statements):
    with get_neo4j_context(conf['uri']) as neo_ctx:
        commit_graphdb(neo_ctx, statements, conf.get('max_retries', 5))


# pylint: disable=too-many-arguments
def insert_df_to_neo4j(df, statement_generator, graph_specification):
    # Use graph specification's neo4j connection
    neo_config = {
        'uri': graph_specification.graph_uri,
        'max_retries': 5,
        'max_batchsize': 5000
    }

    # Partition to <= max_batchsize per paritition
    n_rows = df.cache().count()
    df = df.repartition(
        int(ceil(n_rows/neo_config['max_batchsize']))
    )

    # Generate statements, then collect and push partition-wise to neo4j
    row_chunks = df.rdd.mapPartitions(lambda x: [list(x)], preservesPartitioning=True)
    for i, row_chunk in enumerate(row_chunks.toLocalIterator()):
        print("Pushing...")
        stmt_chunk = [statement_generator(row) for row in row_chunk]
        write_statements(neo_config, stmt_chunk)

def graph_to_neo4j_cql(graph_specification,
                       spark_config,
                       data_format='parquet',
                       array_delimiter=';',
                       debug_write=False,
                       ):
    # Inserting nodes
    nodes = get_combined_nodes(
        graph_specification=graph_specification,
        spark_config=spark_config,
        output_label_col=':LABEL',
        common_labels=['_searchable'],
        data_format=data_format,
        array_delimiter=array_delimiter
    )

    print("Pushing nodes...")
    for i, node_df in enumerate(nodes):
        # If debug mode, write nodes to local dir
        if debug_write:
            debugsavepath = os.path.join(
                os.getcwd(),
                'debug'
            )
            node_df.toPandas().to_csv(os.path.join(debugsavepath, 'node{}.csv'.format(i)))

        insert_df_to_neo4j(
            node_df,
            generate_merge_node_statement_from_row,
            graph_specification)

    # Inserting edges
    edges = get_combined_edges(
        graph_specification=graph_specification,
        spark_config=spark_config,
        output_label_col=':TYPE',
        data_format=data_format,
        array_delimiter=array_delimiter
    )

    for i, edge_df in enumerate(edges):
        # If debug mode, write edges to local dir
        if debug_write:
            debugsavepath = os.path.join(
                os.getcwd(),
                'debug'
            )
            edge_df.toPandas().to_csv(os.path.join(debugsavepath, 'edge{}.csv'.format(i)))

        print("Pushing edges{}...".format(i+1))
        insert_df_to_neo4j(
            edge_df,
            lambda row: generate_merge_edge_statement_from_row(row, None),
            graph_specification)
