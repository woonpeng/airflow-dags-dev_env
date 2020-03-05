"""
Unit testing of graph_specs module
"""
import pytest

from fncore.utils.graph_specification import GraphSpec


def test_env_graph_spec_loading_conflict(test_conflict_graph_spec):
    with pytest.raises(ValueError) as excinfo:
        _ = GraphSpec.from_dict(test_conflict_graph_spec)
    assert excinfo.value.message == {
        '_schema': [
            'Graph specification cannot contain both data_uri and '
            'data_uri_env. Only one of them should be specified.'
        ]
    }


def test_env_graph_spec_loading(test_env_graph_spec):
    graph_spec = GraphSpec.from_dict(test_env_graph_spec)

    assert graph_spec.name == 'test_data'
    assert graph_spec.poll == '0 0 * * *'
    assert graph_spec.data_uri == 'test_data_uri'
    assert graph_spec.graph_uri == 'test_graph_uri'

    assert len(list(graph_spec.node_lists)) == 0
    assert len(list(graph_spec.edge_lists)) == 0


def test_graph_spec_loading(test_graph_spec):
    graph_spec = GraphSpec.from_dict(test_graph_spec)

    assert graph_spec.name == 'test_data'
    assert graph_spec.poll == '0 0 * * *'
    assert graph_spec.data_uri == 'data_uri_value'
    assert graph_spec.graph_uri == 'graph_uri_value'

    assert len(list(graph_spec.node_lists)) == 3
    assert len(list(graph_spec.edge_lists)) == 3


def test_graph_spec_dumping(test_graph_spec):
    graph_spec = GraphSpec.from_dict(test_graph_spec)
    dumped_graph_spec = graph_spec.to_dict()

    assert graph_spec.name == dumped_graph_spec['name']
    assert graph_spec.poll == dumped_graph_spec['poll']
    assert graph_spec.data_uri == dumped_graph_spec['data_uri']
    assert graph_spec.graph_uri == dumped_graph_spec['graph_uri']

    assert len(list(graph_spec.node_lists)) == len(dumped_graph_spec['node_lists'])
    assert len(list(graph_spec.edge_lists)) == len(dumped_graph_spec['edge_lists'])

    for node_list, dumped_node_list in zip(graph_spec.node_lists,
                                           dumped_graph_spec['node_lists']):
        assert node_list.name == dumped_node_list['name']
        assert node_list.safe_name == dumped_node_list['safe_name']

    for edge_list, dumped_edge_list in zip(graph_spec.edge_lists,
                                           dumped_graph_spec['edge_lists']):
        assert edge_list.name == dumped_edge_list['name']
        assert edge_list.safe_name == dumped_edge_list['safe_name']


def test_invalid_poll(test_graph_spec):
    test_cases = ['1 1', '', '1 1 a b', '100 100 * *']

    for test_case in test_cases:
        test_graph_spec['poll'] = test_case

        with pytest.raises(ValueError) as excinfo:
            _ = GraphSpec.from_dict(test_graph_spec)
        assert excinfo.value.message == {
            'poll': ['Invalid cron expression.']}


def test_invalid_variable_definition(test_graph_spec):
    test_graph_spec['node_lists'][0]['index_column'][
        'variable_definition'] = 'not String'

    with pytest.raises(ValueError) as excinfo:
        _ = GraphSpec.from_dict(test_graph_spec)
    assert excinfo.value.message == {
        'node_lists': {0: {'index_column': {'variable_definition': [
            "Variable definition must be "
            "'String', 'Price' or 'Categorical'."
        ]}}}}


def test_safe_name(test_graph_spec):
    graph_spec = GraphSpec.from_dict(test_graph_spec)

    # data set has 'chocolate nodes', 'sweets nodes' and 'toffee nodes'
    intended_node_safe_names = [
        'fn_chocolate_nodes',
        'fn_sweets_nodes',
        'fn_toffee_nodes'
    ]

    node_safe_names = [node_list.safe_name
                       for node_list
                       in graph_spec.node_lists]

    for safe_name in node_safe_names:
        assert safe_name in intended_node_safe_names


def test_check_unique_names(test_graph_spec):
    test_graph_spec['node_lists'][0]["name"] = "chocolate node"
    test_graph_spec['node_lists'][1]["name"] = "chocolate node"

    with pytest.raises(ValueError) as excinfo:
        _ = GraphSpec.from_dict(test_graph_spec)
    assert excinfo.value.message == {
        '_schema': ["Names are not unique: [u'chocolate node']."]}


def test_table_details(test_graph_spec):
    graph_spec = GraphSpec.from_dict(test_graph_spec)

    assert graph_spec.table_details == {
        'connection': 'data_uri_value',
        'poll_frequency': '0 2 * * *',
        'tables': {
            ('test_data_chocolate_edge_list',
             'fn_test_data_chocolate_edge_list'): {
                ('chocolate_s', 'fn_chocolate_s'),
                ('chocolate_t', 'fn_chocolate_t')
            },
            ('test_data_chocolate_node_list',
             'fn_test_data_chocolate_node_list'): {
                ('id', 'fn_id')
            },
            ('test_data_sweets_edge_list',
             'fn_test_data_sweets_edge_list'): {
                ('sweets_s', 'fn_sweets_s'),
                ('sweets_t', 'fn_sweets_t')
            },
            ('test_data_sweets_node_list',
             'fn_test_data_sweets_node_list'): {
                ('id', 'fn_id'),
                ('prop', 'fn_prop')
            },
            ('test_data_toffee_edge_list',
             'fn_test_data_toffee_edge_list'): {
                ('toffee_s', 'fn_toffee_s'),
                ('toffee_t', 'fn_toffee_t')
            },
            ('test_data_toffee_node_list',
             'fn_test_data_toffee_node_list'): {
                ('hide', 'fn_hide'),
                ('id', 'fn_id'),
                ('prop', 'fn_prop')
            }
        }
    }
