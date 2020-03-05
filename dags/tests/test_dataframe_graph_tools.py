from pyspark.sql.functions import col, struct

from fncore.utils.dataframe_graph_tools import (bi_direction_bfs_any,
                                                bi_direction_bfs_fixed)

TEST_COMPREHENSIVE = True
TEST_NUM_PARTITIONS = 10


def test_dataframe_filter(sql_context):
    """Unit test for filtering rows of a spark DF depending
    on whether a field in a row is in a list
    """
    df = (
        sql_context
        .createDataFrame([(u"\'\"",), (u"1234",)], ['src'])
        .withColumn('a', struct(col('src').alias('id')))
    )
    assert df.filter(col('a.id').isin({u"\'\"", u"1234"})).count() == 2
    assert df.filter(~col('a.id').isin({u"\'\"", u"12345"})).count() == 1


def test_bi_direction_no_paths(sql_context):
    """Unit test module for bfs when there are no paths"""
    columns = ['start', 'end']
    test_src_dst_fixed = sql_context.createDataFrame(
        [('A', 'D'),
         ('D', 'A')],
        columns
    ).cache()
    test_src = test_src_dst_fixed.select('start').cache()
    test_dst = test_src_dst_fixed.select('end').cache()

    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    test_edges = sql_context.createDataFrame(
        [('A', 'B', 'OFFICER', 'VENDOR'),
         ('D', 'C', 'OFFICER', 'OFFICER')],
        columns
    ).cache()

    assert list(
        bi_direction_bfs_fixed(
            edges=test_edges,
            src_dst_df=test_src_dst_fixed,
            max_path_len=4,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_any(
            edges=test_edges,
            src_df=test_src,
            dst_df=test_dst,
            max_path_len=4,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()


def test_bi_direction_bfs_empty(test_src_dst_fixed_empty, test_edges_empty,
                                test_src_dst_fixed1, test_edges1):
    test_src_empty = test_src_dst_fixed_empty.select('start').cache()
    test_dst_empty = test_src_dst_fixed_empty.select('end').cache()

    assert list(
        bi_direction_bfs_fixed(
            edges=test_edges_empty,
            src_dst_df=test_src_dst_fixed1,
            max_path_len=1,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_fixed(
            edges=test_edges1,
            src_dst_df=test_src_dst_fixed_empty,
            max_path_len=1,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_fixed(
            edges=test_edges1,
            src_dst_df=test_src_dst_fixed1,
            max_path_len=0,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    test_src_nonempty = test_src_dst_fixed1.select('start').cache()
    test_dst_nonempty = test_src_dst_fixed1.select('end').cache()

    assert list(
        bi_direction_bfs_any(
            edges=test_edges_empty,
            src_df=test_src_nonempty,
            dst_df=test_dst_nonempty,
            max_path_len=1,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_any(
            edges=test_edges1,
            src_df=test_src_empty,
            dst_df=test_dst_nonempty,
            max_path_len=1,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_any(
            edges=test_edges1,
            src_df=test_src_nonempty,
            dst_df=test_dst_empty,
            max_path_len=1,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()

    assert list(
        bi_direction_bfs_any(
            edges=test_edges1,
            src_df=test_src_nonempty,
            dst_df=test_dst_nonempty,
            max_path_len=0,
            num_partitions=TEST_NUM_PARTITIONS
        )
    ) == list()


def test_bi_direction_bfs1(test_src_dst_fixed1, test_edges1):
    """Unit test module where previous bfs algorithm
    failed to detect paths
    """

    for max_path_len in [2, 3, 5]:
        paths = list(
            bi_direction_bfs_fixed(
                edges=test_edges1,
                src_dst_df=test_src_dst_fixed1,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        assert len(paths) == 1
        assert paths[0].count() == 2

        results = paths[0].collect()
        assert results[0]['from']['id'] != results[1]['from']['id']
        for result in results:
            if result['from']['id'] == 'A\'"-#':
                assert result['v1']['id'] == 'C'
                assert result['to']['id'] == 'D'
            else:
                assert result['from']['id'] == 'B'
                assert result['v1']['id'] == 'D'
                assert result['to']['id'] == 'C'

    test_src = test_src_dst_fixed1.select('start').cache()
    test_dst = test_src_dst_fixed1.select('end').cache()

    for max_path_len in [2, 3, 5]:
        paths = list(
            bi_direction_bfs_any(
                edges=test_edges1,
                src_df=test_src,
                dst_df=test_dst,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )

        assert len(paths) == 2
        assert paths[0].count() == 2
        assert paths[1].count() == 2

        results = paths[0].collect()
        assert results[0]['from']['id'] != results[1]['from']['id']
        for result in results:
            if result['from']['id'] == 'A\'"-#':
                assert result['to']['id'] == 'C'
            else:
                assert result['from']['id'] == 'B'
                assert result['to']['id'] == 'D'

        results = paths[1].collect()
        assert results[0]['from']['id'] != results[1]['from']['id']
        for result in results:
            if result['from']['id'] == 'A\'"-#':
                assert result['v1']['id'] == 'C'
                assert result['to']['id'] == 'D'
            else:
                assert result['from']['id'] == 'B'
                assert result['v1']['id'] == 'D'
                assert result['to']['id'] == 'C'


def test_bi_direction_bfs2(test_src_dst_fixed2, test_edges2):
    """Unit test module where previous bfs algorithm
    failed to determine the correct degree
    """
    def check_paths(paths):
        """Check paths"""
        assert len(paths) == 4
        assert paths[0].count() == 1
        assert paths[1].count() == 1
        assert paths[2].count() == 1
        assert paths[3].count() == 1

        result = paths[0].collect()[0]
        assert result['from']['id'] == 'A'
        assert result['to']['id'] == 'B'

        result = paths[1].collect()[0]
        assert result['from']['id'] == 'A'
        assert result['v1']['id'] == 'B'
        assert result['to']['id'] == 'C'

        result = paths[2].collect()[0]
        assert result['from']['id'] == 'A'
        assert result['v1']['id'] == 'B'
        assert result['v2']['id'] == 'C'
        assert result['to']['id'] == 'D'

        result = paths[3].collect()[0]
        assert result['from']['id'] == 'A'
        assert result['v1']['id'] == 'B'
        assert result['v2']['id'] == 'C'
        assert result['v3']['id'] == 'D'
        assert result['to']['id'] == 'E'

    paths = list(
        bi_direction_bfs_fixed(
            edges=test_edges2,
            src_dst_df=test_src_dst_fixed2,
            max_path_len=4,
            num_partitions=TEST_NUM_PARTITIONS
        )
    )
    check_paths(paths)

    test_src = test_src_dst_fixed2.select('start')
    test_dst = test_src_dst_fixed2.select('end')
    paths = list(
        bi_direction_bfs_any(
            edges=test_edges2,
            src_df=test_src,
            dst_df=test_dst,
            max_path_len=4,
            num_partitions=TEST_NUM_PARTITIONS
        )
    )
    check_paths(paths)


def test_bi_direction_bfs3(sql_context):
    """Additional unit test module for bfs"""
    def check_paths(paths):
        """Check paths for first set of edges"""
        assert len(paths) == 1
        paths[0] = paths[0].cache()
        assert paths[0].distinct().count() == paths[0].count()
        assert paths[0].count() == 2

        results = paths[0].collect()
        for result in results:
            if result['from']['id'] == 'A':
                assert result['v1']['id'] == 'B'
                assert result['to']['id'] == 'C'
            else:
                assert result['from']['id'] == 'C'
                assert result['v1']['id'] == 'B'
                assert result['to']['id'] == 'A'

    columns = ['start', 'end']
    test_src_dst_fixed = sql_context.createDataFrame(
        [('A', 'C'),
         ('C', 'A')],
        columns
    ).cache()
    test_src = test_src_dst_fixed.select('start').cache()
    test_dst = test_src_dst_fixed.select('end').cache()

    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    test_edges = sql_context.createDataFrame(
        [('A', 'B', 'OFFICER', 'VENDOR'),
         ('A', 'A', 'OFFICER', 'OFFICER'),
         ('A', 'D', 'OFFICER', 'VENDOR'),
         ('B', 'A', 'VENDOR', 'VENDOR'),
         ('B', 'C', 'VENDOR', 'VENDOR'),
         ('C', 'B', 'OFFICER', 'VENDOR'),
         ('C', 'C', 'OFFICER', 'OFFICER'),
         ('C', 'D', 'OFFICER', 'VENDOR'),
         ('D', 'B', 'VENDOR', 'VENDOR'),
         ],
        columns
    ).cache()

    if TEST_COMPREHENSIVE:
        end_max_path_len = 6
    else:
        end_max_path_len = 2
    for max_path_len in range(1, end_max_path_len):
        paths = list(
            bi_direction_bfs_fixed(
                edges=test_edges,
                src_dst_df=test_src_dst_fixed,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len == 1:
            assert paths == list()
        else:
            check_paths(paths)

        paths = list(
            bi_direction_bfs_any(
                edges=test_edges,
                src_df=test_src,
                dst_df=test_dst,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len == 1:
            assert paths == list()
        else:
            check_paths(paths)


def test_bi_direction_bfs4(sql_context):
    """Additional unit test module for bfs"""
    def check_paths(paths):
        """Check paths second set of edges"""
        assert len(paths) == 1
        paths[0] = paths[0].cache()
        assert paths[0].distinct().count() == paths[0].count()
        assert paths[0].count() == 2

        results = paths[0].collect()
        for result in results:
            if result['from']['id'] == 'A':
                assert result['v1']['id'] == 'E'
                assert result['v2']['id'] == 'B'
                assert result['to']['id'] == 'C'
            else:
                assert result['from']['id'] == 'C'
                assert result['v1']['id'] == 'E'
                assert result['v2']['id'] == 'B'
                assert result['to']['id'] == 'A'

    columns = ['start', 'end']
    test_src_dst_fixed = sql_context.createDataFrame(
        [('A', 'C'),
         ('C', 'A')],
        columns
    ).cache()
    test_src = test_src_dst_fixed.select('start').cache()
    test_dst = test_src_dst_fixed.select('end').cache()

    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    test_edges = sql_context.createDataFrame(
        [('A', 'E', 'OFFICER', 'VENDOR'),
         ('A', 'A', 'OFFICER', 'OFFICER'),
         ('E', 'B', 'VENDOR', 'VENDOR'),
         ('E', 'D', 'VENDOR', 'VENDOR'),
         ('B', 'A', 'VENDOR', 'VENDOR'),
         ('B', 'C', 'VENDOR', 'VENDOR'),
         ('C', 'E', 'OFFICER', 'VENDOR'),
         ('C', 'C', 'OFFICER', 'OFFICER'),
         ('D', 'B', 'VENDOR', 'VENDOR'),
         ],
        columns
    ).cache()

    if TEST_COMPREHENSIVE:
        end_max_path_len = 8
    else:
        end_max_path_len = 3
    for max_path_len in range(2, end_max_path_len):
        paths = list(
            bi_direction_bfs_fixed(
                edges=test_edges,
                src_dst_df=test_src_dst_fixed,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len < 3:
            assert paths == list()
        else:
            check_paths(paths)

        paths = list(
            bi_direction_bfs_any(
                edges=test_edges,
                src_df=test_src,
                dst_df=test_dst,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len < 3:
            assert paths == list()
        else:
            check_paths(paths)


def test_bi_direction_bfs5(sql_context):
    """Additional unit test module for bfs"""
    def check_paths(paths):
        """Check paths second set of edges"""
        assert len(paths) == 1
        paths[0] = paths[0].cache()
        assert paths[0].distinct().count() == paths[0].count()
        assert paths[0].count() == 2

        results = paths[0].collect()
        for result in results:
            if result['from']['id'] == 'A':
                assert result['v1']['id'] == 'E'
                assert result['v2']['id'] == 'F'
                assert result['v3']['id'] == 'B'
                assert result['to']['id'] == 'C'
            else:
                assert result['from']['id'] == 'C'
                assert result['v1']['id'] == 'E'
                assert result['v2']['id'] == 'F'
                assert result['v3']['id'] == 'B'
                assert result['to']['id'] == 'A'

    columns = ['start', 'end']
    test_src_dst_fixed = sql_context.createDataFrame(
        [('A', 'C'),
         ('C', 'A')],
        columns
    ).cache()
    test_src = test_src_dst_fixed.select('start').cache()
    test_dst = test_src_dst_fixed.select('end').cache()

    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    test_edges = sql_context.createDataFrame(
        [('A', 'E', 'OFFICER', 'VENDOR'),
         ('A', 'A', 'OFFICER', 'OFFICER'),
         ('E', 'F', 'OFFICER', 'VENDOR'),
         ('F', 'B', 'VENDOR', 'VENDOR'),
         ('F', 'D', 'VENDOR', 'VENDOR'),
         ('B', 'A', 'VENDOR', 'VENDOR'),
         ('B', 'C', 'VENDOR', 'VENDOR'),
         ('C', 'E', 'OFFICER', 'VENDOR'),
         ('C', 'C', 'OFFICER', 'OFFICER'),
         ('D', 'B', 'VENDOR', 'VENDOR'),
         ],
        columns
    ).cache()

    if TEST_COMPREHENSIVE:
        end_max_path_len = 10
    else:
        end_max_path_len = 4
    for max_path_len in range(3, end_max_path_len):
        paths = list(
            bi_direction_bfs_fixed(
                edges=test_edges,
                src_dst_df=test_src_dst_fixed,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len < 4:
            assert paths == list()
        else:
            check_paths(paths)

        paths = list(
            bi_direction_bfs_any(
                edges=test_edges,
                src_df=test_src,
                dst_df=test_dst,
                max_path_len=max_path_len,
                num_partitions=TEST_NUM_PARTITIONS
            )
        )
        if max_path_len < 4:
            assert paths == list()
        else:
            check_paths(paths)
