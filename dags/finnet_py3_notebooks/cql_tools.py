# coding=utf-8
"""
Tools to generate neo4j queries
"""

def make_label_stmt(labels):
    """Make chain of labels for Cypher query"""
    return ":" + ":".join(set(labels))


def make_prop_stmt(props):
    """Make property matching statement for Cyper query"""
    return ", ".join([
        "{}: {}".format(k, "'%s'" % v if isinstance(v, str) else v)
        for k, v  in props.items()])


def gen_neighbourhood_query(
    node_properties,
    node_labels={},
    depth=2
):
    """
    Optimized (I hope) query to get the neighbourhood around identified node(s) up
    to specified depth
    
    Credit to https://stackoverflow.com/questions/29840801/cypher-use-unwind-with-potentially-empty-collection
    for "OPTIONAL UNWIND" functionality snippet, see UNWIND (CASE ....)
    OPTIONAL MATCH in case we reacht the end of the subgraph, we should not return empty list else data will
    be discarded
    
    :param node_properties: Dictionary of properties the seed node(s) must match
    :optional param node_labels: Iterable of labels the seed nodes(s) must match
    :optional param depth: Depth of neighbourhood to query for
    """
    # Please don't be funny
    assert depth > 0, "Why you trying to be funny..."
    
    # Match the seed node
    query_stmt_lst = list()
    query_stmt_lst.append(
        """
        MATCH (n{} {{{}}})
        WITH collect(n) as outerShellList, collect(n) as nodesAcc, [] as relsAcc
        """.format(make_label_stmt(node_labels), make_prop_stmt(node_properties))
    )
    
    # Recursive depth search, the collects 'flatmaps' the lists
    for _ in range(depth):
        query_stmt_lst.append(
            """
            UNWIND (CASE outerShellList WHEN [] then [null] else outerShellList end) as outerShellNode
            OPTIONAL MATCH (outerShellNode) - [r] - (newOuterNode)
            WHERE (not newOuterNode in nodesAcc) or (newOuterNode in outerShellList)
            WITH collect(DISTINCT newOuterNode) as outerShellList, collect(newOuterNode)+nodesAcc as nodesAcc, collect(r)+relsAcc as relsAcc
            UNWIND nodesAcc as nodeAcc
            WITH outerShellList, collect(DISTINCT nodeAcc) as nodesAcc, relsAcc
            """
        )
        
    # Final return
    query_stmt_lst.append(
        """
        UNWIND relsAcc as relAcc
        WITH nodesAcc, collect(DISTINCT relAcc) as relsAcc
        RETURN nodesAcc as node_list, relsAcc as relation_list
        """
    )
    
    return("".join(query_stmt_lst))


def gen_neighbourhood_query_naive(
    node_properties,
    node_labels={},
    depth=2
):
    """
    Unoptimized (I hope) query to get the neighbourhood around identified node(s) up
    to specified depth
    
    :param node_properties: Dictionary of properties the seed node(s) must match
    :optional param node_labels: Iterable of labels the seed nodes(s) must match
    :optional param depth: Depth of neighbourhood to query for
    """
    # Please don't be funny
    assert depth > 0, "Why you trying to be funny..."
    
    # Match the seed node
    query_stmt_lst = list()
    query_stmt_lst.append(
        """
        MATCH (n{} {{{}}})
        WITH n
        """.format(make_label_stmt(node_labels), make_prop_stmt(node_properties))
    )
    
    # Filter condition
    query_stmt_lst.append(
        """
        MATCH (s) - [r] - (t)
        WHERE exists( (n) - [*0..{}] - (s) - [r] -> (t) )
        or exists( (n) - [*0..{}] - (t) <- [r] - (s) )
        """.format(depth - 1, depth - 1)
    )
    
    query_stmt_lst.append(
        """
        WITH (collect(s) + collect(t)) as nodesAcc, collect(DISTINCT r) as relsAcc
        UNWIND nodesAcc as nodeAcc
        
        RETURN collect(DISTINCT nodeAcc) as node_list, relsAcc as relation_list
        """
    )
    
    return("".join(query_stmt_lst))