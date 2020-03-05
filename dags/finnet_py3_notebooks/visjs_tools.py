# coding=utf-8
"""
Tools to generate node and edge dictionaries to put into vis.js network
visualization 
"""

def make_node_label(node, node_label_keys):
    """
    Select the first available node property from ordered list of keys
    to use for node labelling
    if no keys are provided, assume id as node label
    """
    if node_label_keys is None:
        return node.id
    else:
        label = None; i = 0
        while not label and i < len(node_label_keys):
            label = node.get(node_label_keys[i])
            i += 1
        return label


def make_node_group(node, node_group_types):
    """
    Select the first available node property from ordered list of types
    If no types provided, just take first type available
    """
    if node_group_types is None:
        if node.labels:
            return list(node.labels)[0]
        else:
            return None
    else:
        group = None; i = 0
        while not group and i < len(node_group_types):
            if node_group_types[i] in node.labels:
                group = node_group_types[i]
            i += 1
        return group


def make_vis_data(
    relation_list,
    node_list=[],
    node_label_keys=None,
    node_group_types=None
):
    """
    Neo4J 'label' is the group / category of the node
    vis.js 'label' is the node's display label, therefore the confusing terminology
    """

    nodes = dict()
    edges = dict()

    for relation in relation_list:
        edges[relation.id] = {
            'from': relation.start_node.id,
            'to': relation.end_node.id,
            'label': relation.type}

        nodes[relation.start_node.id] = {
            'id': relation.start_node.id,
            'label': make_node_label(relation.start_node, node_label_keys),
            'group': make_node_group(relation.start_node, node_group_types)}

        nodes[relation.end_node.id] = {
            'id': relation.end_node.id,
            'label': make_node_label(relation.end_node, node_label_keys),
            'group': make_node_group(relation.end_node, node_group_types)}


    for node in node_list:
        if node.id not in nodes:
            nodes[node.id] = {
                'id': node.id,
                'label': make_node_label(node, node_label_keys),
                'group': make_node_group(node, node_group_types)}

    nodes = list(nodes.values())
    edges = list(edges.values())
    
    return nodes, edges