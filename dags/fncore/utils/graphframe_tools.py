# coding=utf-8
"""
Utils for coi and grey list detections
"""
# pylint: disable=no-name-in-module
# pylint: disable=import-error
import os

from pyspark.sql.functions import lit


def get_graph_dataframes(graph_spec, sql_context,
                         node_path_resolved, edge_path_resolved,
                         data_format):
    """
    Read DataFrames of node and edge lists from hdfs according to data_format
    and graph_spec. Create a dictionary that stores the mapping from uuids to
    DataFrames.

    :param graph_spec: the graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param sql_context: an object of type pyspark.sql.SQLContext
    :type sql_context: pyspark.sql.SQLContext
    :param node_path_resolved: path to the resolved node lists
    :type node_path_resolved: str
    :param edge_path_resolved: path to the resolved edge lists
    :type edge_path_resolved: str
    :param data_format: data_format
    :type data_format: str
    :return: `graph`, a dictionary with keys: 'node_list', 'edge_list',
        where each of them is a dictionary with keys: uuids and
        values: DataFrames
    :rtype: dict
    """
    graph = dict()
    graph['node_list'], graph['edge_list'] = dict(), dict()

    for node_list in graph_spec.node_lists:
        graph['node_list'][node_list.safe_name] = sql_context.read \
            .format(data_format) \
            .option('header', 'true') \
            .option('inferschema', 'true') \
            .load(os.path.join(node_path_resolved,
                               node_list.safe_name))

    for edge_list in graph_spec.edge_lists:
        graph['edge_list'][edge_list.safe_name] = (
            sql_context.read
            .format(data_format)
            .option('header', 'true')
            .option('inferschema', 'true')
            .load(os.path.join(edge_path_resolved,
                               edge_list.safe_name)))

    return graph


def load_node_edge_lists(sql_context, graph_spec,
                         node_path_resolved, edge_path_resolved,
                         data_format):
    """
    Create mapping from (table, source_id, target_id) for edge lists or
    (table, node_id) for node lists to dataframes. 'table' is the name of the
    table, 'source_id'/'target_id' are the safe_column_name of the
    source node/target node for edge lists, node_id is the safe_column_name
    of the node for node lists.

    :param sql_context: an object of type pyspark.sql.SQLContext
    :type sql_context: pyspark.sql.SQLContext
    :param graph_spec: the graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param node_path_resolved: path to the resolved node lists
    :type node_path_resolved: str
    :param edge_path_resolved: path to the resolved edge lists
    :type edge_path_resolved: str
    :param data_format: data_format
    :type data_format: str
    :return: a dictionary containing the mapping from node/edge to dataframe
    :rtype: dict
    """

    graph = get_graph_dataframes(graph_spec,
                                 sql_context,
                                 node_path_resolved,
                                 edge_path_resolved,
                                 data_format)

    tables = dict(graph['edge_list'], **graph['node_list'])

    table_column_2_uuid = dict()
    for edge_list in graph_spec.edge_lists:
        table_column_2_uuid[
            (edge_list.safe_table_name,
             edge_list.source_column.safe_name,
             edge_list.target_column.safe_name)] \
            = edge_list.safe_name

    for node_list in graph_spec.node_lists:
        table_column_2_uuid[
            (node_list.safe_table_name,
             node_list.index_column.safe_name)] \
            = node_list.safe_name

    # Bypass uuid
    mapping = dict()
    for table_column, uuid in table_column_2_uuid.items():
        mapping[table_column] = tables[uuid]

    return mapping


# pylint: disable=too-many-locals
# pylint: disable=too-many-arguments
def bfs_recursive_worker(annotate,
                         df_start,
                         df_end,
                         nodes,
                         edges,
                         list_results,
                         index,
                         num_hops):
    """
    Recursively find shortest paths from the set of current source nodes
    (df_start) and the set of current target nodes (df_end) with increasing
    number of hops. The idea is to call GraphFrame BFS
    following the below approach. Refer to the code for more details.

    .. code-block:: none

                                  root
                                /     \\
                               /       \\
                              /         \\
                             /           \\
                           l1            r1          one hop
                          /  \\           / \\
                         /    \\         /   \\
                        /      \\       /     \\
                      l2        r2    l2     r2      two hops
                     /\\        /\\     /\\     /\\
                    /  \\      /  \\   /  \\   /  \\
                   /    \\    /    \\ /    \\ /    \\
                   .     .   .     ..     ..     .   three hops
                   .     .   .     ..     ..     .   .
                   .     .   .     ..     ..     .   .
                   .     .   .     ..     ..     .   four hops

    :param annotate: indicate 'left' or 'right' branch
    :type annotate: str
    :param df_start: the current set of source nodes. The schema must be:

        .. code-block:: none

            |-- id: string (nullable = true)
    :type df_start: pyspark.sql.DataFrame
    :param df_end: the current set of target nodes. The schema must be:

        .. code-block:: none

            |-- id: string (nullable = true)
    :type df_end: pyspark.sql.DataFrame
    :param nodes: all nodes for constructing the GraphFrame. The schema is:

        .. code-block:: none

            |-- id: string (nullable = true)
            |-- Category: string (nullable = true)
    :type nodes: pyspark.sql.DataFrame
    :param edges: all edges for constructing the GraphFrame

        .. code-block:: none

            |-- src: string (nullable = true)
            |-- dst: string (nullable = true)
            |-- relationship: string (nullable = true)
            |-- Type: string (nullable = true)
            |-- Source_Type: string (nullable = true)
            |-- Target_Type: string (nullable = true)
    :type edges: pyspark.sql.DataFrame
    :param list_results:
        accumulator to store the results of shortest paths. Each result
        dataframe has the following schemas:

        One hop:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or two hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or three hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or four hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v3: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e3: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
    :type list_results: list of pyspark.sql.DataFrame
    :param index: the current maximum number of hops
    :type index: int
    :param num_hops: the maximum allowed number of hops
    :type num_hops: int
    :return: None
    :rtype: None
    """
    from graphframes import GraphFrame

    if index > num_hops:
        return

    df_start = df_start.withColumn(
        annotate + 'Attribute' + str(index), lit('start'))
    df_end = df_end.withColumn(
        annotate + 'Attribute' + str(index), lit('end'))
    nodes_new = (nodes
                 .join(df_start.unionAll(df_end).distinct(),
                       'id',
                       'left_outer'))[['id',
                                       'Category',
                                       annotate + 'Attribute' + str(index)]]
    nodes_new.persist()

    coi_graph = GraphFrame(nodes_new, edges)
    results = coi_graph.bfs(
        "%sAttribute%d='start'" % (annotate, index),
        "%sAttribute%d='end'" % (annotate, index),
        maxPathLength=num_hops)

    if results.count() >= 1:
        results.persist()
        list_results.append(results)

        if index < num_hops:
            start_remove = results[['from']] \
                .withColumn('id', results['from'].id)[['id']].distinct()
            end_remove = results[['to']].withColumn('id', results['to'].id)[
                ['id']].distinct()

            new_start = df_start[['id']].subtract(start_remove).distinct()
            new_end = df_end[['id']].subtract(end_remove).distinct()

            index = int(len(results.columns) / 2)

            bfs_recursive_worker('left',
                                 new_start,
                                 df_end[['id']],
                                 nodes,
                                 edges,
                                 list_results,
                                 index + 1,
                                 num_hops)
            bfs_recursive_worker('right',
                                 df_start[['id']],
                                 new_end,
                                 nodes,
                                 edges,
                                 list_results,
                                 index + 1,
                                 num_hops)


def bfs_recursive_main(df_start, df_end, nodes, edges, num_hops=4):
    """
    Compute all paths from the a source node (df_start) to the set of
    current target nodes (df_end), where the maximum number of hops is
    num_hops.

    :param df_start: Only works when there is one source node. The schema
        must be:

        .. code-block:: none

            |-- id: string (nullable = true)
    :type df_start: pyspark.sql.DataFrame
    :param df_end: the current set of target nodes. The schema must be:

        .. code-block:: none

            |-- id: string (nullable = true)
    :type df_end: pyspark.sql.DataFrame
    :param nodes: all nodes for constructing the GraphFrame. The schema is:

        .. code-block:: none

            |-- id: string (nullable = true)
            |-- Category: string (nullable = true)
    :type nodes: pyspark.sql.DataFrame
    :param edges: all edges for constructing the GraphFrame

        .. code-block:: none

            |-- src: string (nullable = true)
            |-- dst: string (nullable = true)
            |-- relationship: string (nullable = true)
            |-- Type: string (nullable = true)
            |-- Source_Type: string (nullable = true)
            |-- Target_Type: string (nullable = true)
    :type edges: pyspark.sql.DataFrame
    :param num_hops: the maximum allowed number of hops
    :type num_hops: int
    :return:
        accumulator that stores the results of shortest paths. Each result
        dataframe has the following schemas:

        One hop:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or two hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or three hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or four hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v3: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e3: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
    :rtype: list of pyspark.sql.DataFrame
    """
    list_results = list()
    bfs_recursive_worker('root', df_start, df_end, nodes,
                         edges, list_results, 1, num_hops)
    return list_results


def load_results(sql_context, graph_data_path, exec_date):
    """
    Load the previously computed paths and return them as a list

    :param sql_context: an object of type pyspark.sql.SQLContext
    :type sql_context: pyspark.sql.SQLContext
    :param graph_data_path: the dictionary where the key 'temp_files' is
           required
    :type graph_data_path: dict
    :param exec_date: execution date of the job
    :type exec_date: datetime.datetime
    :return: list of dataframes that stores shortest paths in the following schemas:

        One hop:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or two hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or three hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)

        or four hops:

        .. code-block:: none

            from: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e0: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v1: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e1: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v2: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e2: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            v3: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
            e3: struct (nullable = true)
             |-- src: string (nullable = true)
             |-- dst: string (nullable = true)
             |-- relationship: string (nullable = true)
             |-- Type: string (nullable = true)
             |-- Source_Type: string (nullable = true)
             |-- Target_Type: string (nullable = true)
            to: struct (nullable = true)
             |-- id: string (nullable = true)
             |-- Category: string (nullable = true)
             |-- rightAttribute4: string (nullable = true)
    :rtype: list of pyspark.sql.DataFrame
    """
    import subprocess
    directory = os.path.join(graph_data_path['temp_files'],
                             exec_date.strftime('%Y_%m_%d'),
                             "results")

    prs = subprocess.Popen(["hadoop", "fs", "-ls", directory],
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)

    # pylint: disable=unused-variable
    out, err = prs.communicate()
    listed_folders = list()
    listed_folders += \
        [r[-1] for r in
         [split_string.split() for split_string in out.split("\n") if
          split_string.split()] if directory in r[-1]]
    if len(listed_folders) < 1:
        return list()
    # Load the previously computed results
    results = list()
    for folder in listed_folders:
        results.append(
            sql_context.read.load(folder, "parquet").dropDuplicates())
    return results
