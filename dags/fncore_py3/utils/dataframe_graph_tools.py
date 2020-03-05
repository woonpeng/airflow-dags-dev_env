# coding=utf-8
"""
Perform breadth first search in Dataframes
"""
# pylint: disable=no-name-in-module
# pylint: disable=import-error
import math

from pyspark.sql.functions import col, concat, lit, struct


def _get_a2b(edges):
    """
    Processes the `edges` DataFrame and returns `a2b` DataFrame
        ((a)-[e]->(b)) to be used for each iteration of BFS.

    :param edges: edges of the graph (contains two special
        columns named "src" and "dst" which specifies an
        edge from vertex "src" to vertex "dst") with the
        following schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :type edges: pyspark.sql.DataFrame
    :return: contains three special columns named "a" (src),
        "e" (edge), "b" (dst) with the following schema:
            | |-- a: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
            | |-- e: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- -- relationship: str
            | |-- -- Type: str
            | |-- -- Source_Type: str
            | |-- -- Target_Type: str
            | |-- b: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
    :rtype: pyspark.sql.DataFrame
    """
    edges_column_names = [col(column_name) for column_name in edges.columns]
    a2b = (edges
           .withColumn('e', struct(*edges_column_names))
           .select('e'))
    a2b = (a2b
           .withColumn('a', struct(col('e.src').alias('id'),
                                   col('e.Source_Type').alias('Category')))
           .withColumn('b', struct(col('e.dst').alias('id'),
                                   col('e.Target_Type').alias('Category'))))

    return a2b


# pylint: disable=too-many-locals
def _get_forward_fixed(a2b,
                       src_dst_nodes,
                       num_partitions,
                       max_forward_hops=1,
                       remove_found_paths=False):
    """Yields up to a maximum length of `max_forward_hops`
    1) all the paths starting from src in `src_dst_nodes`
    2) all the shortest paths found in the forward pass beginning at src
    and terminating at dst in `src_dst_nodes`.
    3) the remaining targets in `src_dst_nodes` that have not been found.

    :param a2b: contains three special columns named "a" (src),
        "e" (edge), "b" (dst) with the following schema:
            | |-- a: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
            | |-- e: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- -- relationship: str
            | |-- -- Type: str
            | |-- -- Source_Type: str
            | |-- -- Target_Type: str
            | |-- b: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
    :type a2b: pyspark.sql.DataFrame
    :param src_dst_nodes: src node id + "|" + dst node id
    :type src_dst_nodes: set[str]
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :param max_forward_hops: maximum number of forward hops
    :type max_forward_hops: int
    :param remove_found_paths: indicates whether to remove found paths
    :type remove_found_paths: bool
    :return: (`paths`, `found_paths`, `src_dst_nodes`)
        `paths` satisfy src nodes in `src_dst_nodes` of increasing length
        up to a maximum of `max_forward_hops`;  `paths` follows the
        schema below (only the more important fields are shown):
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
            | |-- v{`max_forward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
        `found_paths` satisfy `src_dst_nodes` of
        increasing length up to a maximum of `max_forward_hops`;
        follows the schema below (only the more important
        fields are shown):
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
        `src_dst_nodes` contains the remaining
        targets that have not been found;
    :rtype: collections.Iterable[
        tuple[pyspark.sql.DataFrame,
              pyspark.sql.DataFrame,
              set[str]]
        ]
    """
    for step in range(max_forward_hops):
        next_vertex = "v%d" % (step + 1)
        next_edge = "e%d" % step
        if step == 0:
            src_nodes = {src_dst.split('|')[0] for src_dst in src_dst_nodes}
            paths = (a2b
                     .filter(col('a.id').isin(src_nodes))
                     .filter(col('a.id') != col('b.id'))
                     .withColumnRenamed('a', 'from')
                     .withColumnRenamed('e', next_edge)
                     .withColumnRenamed('b', next_vertex))
        else:
            prev_vertex = "v%d" % step
            next_links = (a2b
                          .withColumnRenamed('a', prev_vertex)
                          .withColumnRenamed('e', next_edge)
                          .withColumnRenamed('b', next_vertex))

            # pylint: disable=line-too-long
            paths = (
                paths
                .join(next_links, paths[prev_vertex + ".id"] == next_links[prev_vertex + ".id"])
                .drop(paths[prev_vertex])  # remove repeated column
            )
            # pylint: enable=line-too-long

            # No. of partitions increases after join,
            # thus repartitioning to ensure it remains manageable
            if num_partitions is not None:
                paths = paths.repartition(num_partitions)

            # remove self loops
            previous_vertex_checks = reduce(
                lambda c1, c2: c1 & c2,
                [paths["v%d.id" % i] != paths[next_vertex + ".id"]
                 for i in range(1, step + 1)],
                paths["from.id"] != paths[next_vertex + ".id"])
            paths = paths.filter(previous_vertex_checks)

        # add src_dst column
        paths = (
            paths
            .withColumn('src_dst',
                        concat(col('from.id'), lit('|'), col(next_vertex + '.id')))
        )

        found_paths = (
            paths
            .filter(col('src_dst').isin(src_dst_nodes))
            .withColumnRenamed(next_vertex, 'to')
            .cache()
        )

        found_src_dst_nodes = {
            row.src_dst for row in found_paths.select('src_dst').distinct().collect()
        }
        src_dst_nodes.difference_update(found_src_dst_nodes)
        found_paths = found_paths.drop('src_dst')

        if remove_found_paths:
            paths = paths.filter(~col('src_dst').isin(src_dst_nodes))
        paths = paths.drop('src_dst')

        yield paths, found_paths, src_dst_nodes

        if len(src_dst_nodes) == 0:
            break


def _get_backward_paths(a2b,
                        dst_df,
                        num_partitions,
                        max_backward_hops=1):
    """Yields all the paths found in the backward pass starting
    from dst in `dst_df` with increasing length up to a
    maximum of `max_backward_hops`.

    :param a2b: contains three special columns named "a" (src),
        "e" (edge), "b" (dst) with the following schema:
            | |-- a: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
            | |-- e: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- -- relationship: str
            | |-- -- Type: str
            | |-- -- Source_Type: str
            | |-- -- Target_Type: str
            | |-- b: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
    :type a2b: pyspark.sql.DataFrame
    :param dst_df: column named "end" contains end id of vertex
            | |-- end: str
    :type dst_df: pyspark.sql.DataFrame
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :param max_backward_hops: maximum number of backward hops
    :type max_backward_hops: int
    :return: `paths`, paths starting from any "end" in `dst_df` of
        increasing length up to a maximum of `max_backward_hops`;
        follows the schema below (only the more important fields
        are shown)
            | |-- to: pyspark.sql.StructType
            | |-- -- id: str
            | |-- e_back_0: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- v_back_1: pyspark.sql.StructType
            | |-- -- id: str
            | .
            | .
            | .
            | |-- v_back_{`max_backward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
    :rtype: collections.Iterable[pyspark.sql.DataFrame]
    """
    for step in range(max_backward_hops):
        next_vertex = "v_back_%d" % (step + 1)
        next_edge = "e_back_%d" % step
        if step == 0:
            paths = (a2b
                     .withColumn('end', col('b.id'))
                     .join(dst_df, on='end')
                     .drop('end')
                     .filter(col('a.id') != col('b.id'))
                     .withColumnRenamed('a', next_vertex)
                     .withColumnRenamed('e', next_edge)
                     .withColumnRenamed('b', 'to'))
        else:
            prev_vertex = "v_back_%d" % step
            next_links = (a2b
                          .withColumnRenamed('a', next_vertex)
                          .withColumnRenamed('e', next_edge)
                          .withColumnRenamed('b', prev_vertex))

            paths = (
                paths
                .join(next_links,
                      paths[prev_vertex + ".id"] == next_links[prev_vertex + ".id"])
                .drop(paths[prev_vertex])
            )

            if num_partitions is not None:
                paths = paths.repartition(num_partitions)

            previous_vertex_checks = reduce(
                lambda c1, c2: c1 & c2,
                [paths["v_back_%d.id" % i] != paths[next_vertex + ".id"]
                 for i in range(1, step + 1)],
                paths["to.id"] != paths[next_vertex + ".id"])
            paths = paths.filter(previous_vertex_checks)

        yield paths


def _join_paths_fixed(forward_paths,
                      backward_paths,
                      src_dst_nodes,
                      num_partitions):
    """Joins paths in the forward pass with paths in the backward pass.

    :param forward_paths: paths from forward pass of length
        `max_forward_hops` and starting from src; `forward_paths` follows
        the schema below (only the more important fields are shown):
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
            | |-- v{`max_forward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
    :type forward_paths: pyspark.sql.DataFrame
    :param backward_paths: paths starting from dst in `src_dst_nodes` with
        maximum length of `max_backward_hops`; follows the schema below
        (only the more important fields are shown)
            | |-- to: pyspark.sql.StructType
            | |-- -- id: str
            | |-- e_back_0: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- v_back_1: pyspark.sql.StructType
            | |-- -- id: str
            | .
            | .
            | .
            | |-- v_back_{`max_backward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
    :type backward_paths: pyspark.sql.DataFrame
    :param src_dst_nodes: start node id + "|" + end node id
    :type src_dst_nodes: set[str]
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :return: (`found_paths`, `src_dst_nodes`); paths of maximum length
        `max_path_len` satisfying src nodes and dst nodes in the input
        `src_dst_nodes`; `src_dst_nodes` contains the remaining targets
        that have not been found; `found_paths` follows the schema below
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
    :rtype: tuple[pyspark.sql.DataFrame, set[str]]
    """
    for_len = len(forward_paths.columns) / 2
    for_vertex = 'v%d' % for_len
    back_len = len(backward_paths.columns) / 2
    back_vertex = 'v_back_%d' % back_len

    paths = (
        backward_paths
        .join(forward_paths,
              forward_paths[for_vertex + '.id'] == backward_paths[back_vertex + '.id'])
        .withColumn('src_dst', concat(col('from.id'), lit('|'), col('to.id')))
        .drop(for_vertex)
    )

    if num_partitions is not None:
        paths = paths.repartition(num_partitions)

    found_paths = paths.filter(col('src_dst').isin(src_dst_nodes)).cache()

    found_src_dst_nodes = {
        row.src_dst for row in found_paths.select('src_dst').distinct().collect()
    }
    src_dst_nodes.difference_update(found_src_dst_nodes)

    found_paths = found_paths.drop('src_dst')
    for i in range(back_len):
        found_paths = (found_paths
                       .withColumnRenamed('v_back_%d' % (back_len - i),
                                          'v%d' % (for_len + i)))
        # print('v_back_%d' % (back_len - i), 'v%d' % (for_len + i))
        found_paths = (found_paths
                       .withColumnRenamed('e_back_%d' % (back_len - i - 1),
                                          'e%d' % (for_len + i)))
        # print('e_back_%d' % (back_len - i - 1), 'e%d' % (for_len + i))

    return found_paths, src_dst_nodes


def bi_direction_bfs_fixed(edges,
                           src_dst_df,
                           max_path_len,
                           num_partitions=1000):
    """Returns DataFrames of paths of increasing length up to
    `max_path_len`. Each path is a shortest path from vertices
    with id in "start" column of `src_dst_df` to vertices
    with id in "end" column of `src_dst_df`. If multiple paths
    are valid and have the same length, the DataFrame will
    contain one row for each path.

    :param edges: edges of the graph (contains two special
        columns named "src" and "dst" which specifies an
        edge from vertex "src" to vertex "dst") with the
        following schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :type edges: pyspark.sql.DataFrame
    :param src_dst_df: column named "start" contains start id
        of vertex and column named "end" contains end id of vertex
            | |-- start: str
            | |-- end: str
    :type src_dst_df: pyspark.sql.DataFrame
    :param max_path_len: maximum length of a path
    :type max_path_len: int
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :return: paths satisfying `src_dst_nodes` of shortest
        length; follows the schema below (only the more important
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
    :rtype: collections.Iterable[pyspark.sql.DataFrame]
    """
    # Remove cases where start and end are the same
    src_dst_df = src_dst_df.filter("start != end").distinct()

    if edges.take(1) and src_dst_df.take(1) and max_path_len > 0:
        a2b = _get_a2b(edges)
        if num_partitions is not None:
            a2b = a2b.repartition(num_partitions)
        a2b = a2b.cache()
        src_dst_nodes = {
            str(row.start) + '|' + str(row.end)
            for row in src_dst_df.collect()
        }

        max_forward_hops = int(math.ceil(max_path_len / 2.0))
        max_backward_hops = max_path_len - max_forward_hops
        for (forward_paths,
             found_paths,
             src_dst_nodes) in _get_forward_fixed(a2b,
                                                  src_dst_nodes,
                                                  num_partitions,
                                                  max_forward_hops):
            if found_paths.take(1):
                yield found_paths

        # pylint: disable=undefined-loop-variable
        if len(src_dst_nodes) > 0:
            forward_paths = forward_paths.cache()
            dst_df = src_dst_df.select("end").distinct().cache()
            for backward_paths in _get_backward_paths(a2b,
                                                      dst_df,
                                                      num_partitions,
                                                      max_backward_hops):
                found_paths, src_dst_nodes = _join_paths_fixed(forward_paths,
                                                               backward_paths,
                                                               src_dst_nodes,
                                                               num_partitions)

                if found_paths.take(1):
                    yield found_paths

                if len(src_dst_nodes) == 0:
                    break


# pylint: disable=too-many-locals, too-many-arguments
def _get_forward_paths_any(a2b,
                           src_df,
                           dst_df,
                           num_partitions,
                           max_forward_hops=1,
                           remove_found_paths=False):
    """Yields up to maximum length `max_forward_hops`
    1) all paths starting from "start" in `src_df`
    2) all the shortest paths for any source, destination pair found in
    "start" of `src_df` and "end" of `dst_df`
    3) yields all the targets that have been found.

    :param a2b: contains three special columns named "a" (src),
        "e" (edge), "b" (dst) with the following schema:
            | |-- a: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
            | |-- e: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- -- relationship: str
            | |-- -- Type: str
            | |-- -- Source_Type: str
            | |-- -- Target_Type: str
            | |-- b: pyspark.sql.StructType
            | |-- -- id: str
            | |-- -- Category: str
    :type a2b: pyspark.sql.DataFrame
    :param src_df: column named "start" contains start id
        of vertex
            | |-- start: str
    :type src_df: pyspark.sql.DataFrame
    :param dst_df: column named "end" contains end id of vertex
            | |-- end: str
    :type dst_df: pyspark.sql.DataFrame
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :param max_forward_hops: maximum number of forward hops
    :type max_forward_hops: int
    :param remove_found_paths: indicates whether to remove found paths
    :type remove_found_paths: bool
    :return: (`paths`, `found_paths`, `found_src_dst_nodes`)
        `paths` of length `max_forward_hops` starting from
        "start" in `src_df`; follows the schema below
        (only the more important fields are shown):
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
            | |-- v{`max_forward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
        `found_paths` satisfying any "start" in `src_df` and
        any "end" in `dst_df` of increasing length up to a maximum of
        `max_forward_hops`; for a fixed source and destination, only
        returns the shortest path found; follows the schema below
        (only the more important fields are shown):
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
        `found_src_dst_nodes` contains the targets that have been found;
    :rtype: collections.Iterable[
        tuple[pyspark.sql.DataFrame, pyspark.sql.DataFrame, set[str]]
    ]
    """
    found_src_dst_nodes = set()
    for step in range(max_forward_hops):
        next_vertex = "v%d" % (step + 1)
        next_edge = "e%d" % step
        if step == 0:
            paths = (a2b
                     .withColumn('start', col('a.id'))
                     .join(src_df, on='start')
                     .drop('start')
                     .filter(col('a.id') != col('b.id'))
                     .withColumnRenamed('a', 'from')
                     .withColumnRenamed('e', next_edge)
                     .withColumnRenamed('b', next_vertex))
        else:
            prev_vertex = "v%d" % step
            next_links = (a2b
                          .withColumnRenamed('a', prev_vertex)
                          .withColumnRenamed('e', next_edge)
                          .withColumnRenamed('b', next_vertex))

            paths = (
                paths.join(next_links,
                           paths[prev_vertex + ".id"] == next_links[prev_vertex + ".id"])
                .drop(paths[prev_vertex])
            )

            if num_partitions is not None:
                paths = paths.repartition(num_partitions)

            previous_vertex_checks = reduce(
                lambda c1, c2: c1 & c2,
                [paths["v%d.id" % i] != paths[next_vertex + ".id"]
                 for i in range(1, step + 1)],
                paths["from.id"] != paths[next_vertex + ".id"])
            paths = paths.filter(previous_vertex_checks)

        found_paths = (
            paths
            .withColumn('src_dst',
                        concat(col('from.id'), lit('|'), col(next_vertex + '.id')))
            .withColumn('end', col(next_vertex + '.id'))
            .join(dst_df, on='end')
            .drop('end')
        )

        if found_src_dst_nodes:
            found_paths = found_paths.filter(~col('src_dst').isin(found_src_dst_nodes))

        found_paths = found_paths.cache()
        found_src_dst_nodes.update(
            row.src_dst for row in found_paths.select('src_dst').distinct().collect()
        )

        found_paths = found_paths.drop('src_dst')
        if remove_found_paths:
            paths = paths.subtract(found_paths)

        found_paths = found_paths.withColumnRenamed(next_vertex, 'to')

        yield paths, found_paths, found_src_dst_nodes


def _join_paths_any(forward_paths,
                    backward_paths,
                    found_src_dst_nodes,
                    num_partitions):
    """Joins paths in the forward pass with paths in the backward pass.

    :param forward_paths: paths from forward pass, starting from "start"
        of length `max_forward_hops`;
        `forward_paths` follows the schema below (only the more important
        fields are shown):
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
            | |-- v{`max_forward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
    :type forward_paths: pyspark.sql.DataFrame
    :param backward_paths: paths starting from "end" in `dst_df` with
        maximum of length of `max_backward_hops`; follows the schema below
        (only the more important fields are shown)
            | |-- to: pyspark.sql.StructType
            | |-- -- id: str
            | |-- e_back_0: pyspark.sql.StructType
            | |-- -- src: str
            | |-- -- dst: str
            | |-- v_back_1: pyspark.sql.StructType
            | |-- -- id: str
            | .
            | .
            | .
            | |-- v_back_{`max_backward_hops`}: pyspark.sql.StructType
            | |-- -- id: str
    :type backward_paths: pyspark.sql.DataFrame
    :param found_src_dst_nodes: start node id + "|" + end node id that have
        been found
    :type found_src_dst_nodes: set[str]
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :return: (`found_paths`, `found_src_dst_nodes`); paths satisfying any
        "start" in `src_df` and any "end" in `dst_df` of increasing length
        up to a maximum of `max_path_len`; for a fixed source and destination,
        only returns the shortest path found;
        `found_src_dst_nodes` contains the targets that have been found;
        `found_paths` follows the schema below (only the more important
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
    :rtype: tuple[pyspark.sql.DataFrame, set[str]]
    """
    for_len = len(forward_paths.columns) / 2
    for_vertex = 'v%d' % for_len
    back_len = len(backward_paths.columns) / 2
    back_vertex = 'v_back_%d' % back_len

    paths = (
        backward_paths
        .join(forward_paths,
              forward_paths[for_vertex + '.id'] == backward_paths[back_vertex + '.id'])
        .filter("from.id != to.id")
        .withColumn('src_dst', concat(col('from.id'), lit('|'), col('to.id')))
        .drop(for_vertex)
    )

    if num_partitions is not None:
        paths = paths.repartition(num_partitions)

    if found_src_dst_nodes:
        paths = paths.filter(~col('src_dst').isin(found_src_dst_nodes)).cache()

    found_paths = paths.cache()
    found_src_dst_nodes.update(
        {row.src_dst for row in found_paths.select('src_dst').distinct().collect()}
    )

    found_paths = found_paths.drop('src_dst')
    for i in range(back_len):
        found_paths = (found_paths
                       .withColumnRenamed('v_back_%d' % (back_len - i),
                                          'v%d' % (for_len + i)))
        # print('v_back_%d' % (back_len - i), 'v%d' % (for_len + i))
        found_paths = (found_paths
                       .withColumnRenamed('e_back_%d' % (back_len - i - 1),
                                          'e%d' % (for_len + i)))
        # print('e_back_%d' % (back_len - i - 1), 'e%d' % (for_len + i))

    return found_paths, found_src_dst_nodes


def bi_direction_bfs_any(edges,
                         src_df,
                         dst_df,
                         max_path_len,
                         num_partitions=1000):
    """Returns DataFrames of paths of increasing length up to
    `max_path_len`. Each path is a shortest path from any
    vertices with id in "start" column of `src_df` to any
    vertices with id in "end" column of `dst_df`. If multiple paths
    are valid and have the same length, the DataFrame will
    return one row for each path.

    :param edges: edges of the graph (contains two special
        columns named "src" and "dst" which specifies an
        edge from vertex "src" to vertex "dst") with the
        following schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :type edges: pyspark.sql.DataFrame
    :param src_df: column named "start" contains start id
        of vertex
            | |-- start: str
    :type src_df: pyspark.sql.DataFrame
    :param dst_df: column named "end" contains end id of vertex
            | |-- end: str
    :type dst_df: pyspark.sql.DataFrame
    :param max_path_len: maximum length of a path
    :type max_path_len: int
    :param num_partitions: number of partitions
    :type num_partitions: int | None
    :return: paths satisfying `src_dst_nodes` of shortest
        length; follows the schema below (only the more important
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
    :rtype: collections.Iterable[pyspark.sql.DataFrame]
    """
    if (edges.take(1) and src_df.take(1)
            and dst_df.take(1) and max_path_len > 0):
        a2b = _get_a2b(edges)
        if num_partitions is not None:
            a2b = a2b.repartition(num_partitions)
        a2b = a2b.cache()
        src_df = src_df.distinct()
        dst_df = dst_df.distinct()

        max_forward_hops = int(math.ceil(max_path_len / 2.0))
        max_backward_hops = max_path_len - max_forward_hops
        for (forward_paths,
             found_paths,
             found_src_dst_nodes) in _get_forward_paths_any(a2b,
                                                            src_df,
                                                            dst_df,
                                                            num_partitions,
                                                            max_forward_hops):
            if found_paths.take(1):
                yield found_paths

        # pylint: disable=undefined-loop-variable
        forward_paths = forward_paths.cache()
        for backward_paths in _get_backward_paths(a2b,
                                                  dst_df,
                                                  num_partitions,
                                                  max_backward_hops):
            found_paths, found_src_dst_nodes = _join_paths_any(forward_paths,
                                                               backward_paths,
                                                               found_src_dst_nodes,
                                                               num_partitions)

            if found_paths.take(1):
                yield found_paths
