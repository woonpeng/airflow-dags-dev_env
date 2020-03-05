"""
Helper functions to interact with neo4j
"""
import re

from neo4j.v1 import TransientError


def query_graph_statistics(neo_ctx):
    """
    Given the neo4j context to the neo4j database, returns
    the number of nodes and edges in the database

    :param neo_ctx: neo4j context to the neo4j database
    :type neo_ctx: neo4j.v1.GraphDatabase.driver.session
    :return: a tuple containing the number of nodes and number of edges
             (in the order specified) in the neo4j database currently
    :rtype: tuple
    """
    statement = "MATCH n OPTIONAL MATCH (n)-[r]->() \
                 return count(n) as num_nodes, count(r) as num_edges"
    results = neo_ctx.run(statement).data()[0]
    num_nodes = results['num_nodes']
    num_edges = results['num_edges']
    return num_nodes, num_edges


def generate_set_label_statements(tags):
    """
    Generate the cypher queries to set the labels of a node given the
    tags. This is used in conjunction with a cypher query to match the
    nodes upon which to set the labels

    :param tags: list of labels to assign to the node
    :type tags: list
    :return: string corresponding to the cypher query to set the label
             of a node 'n'
    :rtype: str
    """
    tag_stmt = ""
    if tags and isinstance(tags, list):
        tag_stmt = [k.strip() for k in tags if isinstance(k, (str, unicode))]
        tag_stmt = ['`' + k + '`' for k in tag_stmt if len(k) > 0]
        tag_stmt = 'set n:' + ':'.join(tag_stmt) if len(tag_stmt) > 0 else ""
    return tag_stmt


def generate_set_prop_statements(rowdict, excludefields, noappendfields, mapping):
    """
    Generate the cypher queries to set the properties of a node.
    This is used in conjunction with a cypher query to match the
    nodes upon which to set the labels

    :param rowdict: a dictionary of the properties to assign to the node
    :type rowdict: dict
    :param excludefields: a list of fields (properties) to exclude
    :type excludefields: list
    :param noappendfields: a list of fields (properties) in which to
                           overwrite any existing values. Fields that are
                           not in excludefields or noappendfields are
                           concatenated with the separator
    :type noappendfields: list
    :param mapping: a dictionary mapping from the field name (key) to the
                    corresponding friendly name (value)
    :type mapping: dict
    :return: string corresponding to the cypher query to set the properties of
             a node 'n'
    :rtype: str
    """
    if not excludefields:
        excludefields = []
    keylist = [k for k in rowdict.keys() if k not in excludefields]
    mapping.update({k: k for k in keylist if k not in mapping})
    rowstmt = ', '.join(
        [
            '`' + str(mapping[k]) + '`: case n.`' + str(mapping[k]) + '` when null then ' +
            '[{`' + str(k) + '`}] else ' +
            'filter(x in n.`' + str(mapping[k]) + '` where x <> lower({`' + str(k) +
            '`})) + lower({`' + str(k) + '`}) end '
            if k not in noappendfields
            else
            '`' + str(mapping[k]) + '`: {`' + str(k) + '`} '
            for k in keylist
        ]
    )
    if len(rowstmt) > 0:
        rowstmt = 'set n += { ' + rowstmt + ' } '
    return rowstmt


def generate_relation_key_property(row, keyname, mapping):
    """
    Generate the cypher queries to define the properties of a node / edge to match .
    This is used with a CREATE, MERGE or MATCH cypher query to define the properties
    to match

    :param row: a dictionary of the properties of the node
    :type row: pyspark.sql.Row
    :param keyname: name of the property
    :type keyname: str
    :param mapping: a dictionary mapping from the field name (key) to the
                    corresponding friendly name (value)
    :type mapping: dict
    :return: string corresponding to the cypher query to define the property
             to match on
    :rtype: str
    """
    rowstmt = ""
    if keyname:
        if keyname not in mapping:
            mapping[keyname] = keyname
        rowstmt = '{`' + mapping[keyname] + \
            '`: ' + '"' + str(row[keyname]) + '"}'
    return rowstmt


def commit_graphdb(neo_ctx, statements, max_retry):
    """
    Given a neo4j context to the neo4j database, and the cypher queries to transact,
    attempt to commit the queries up to the specified maximum number of tries

    :rtype: None
    :param neo_ctx: neo4j context to the neo4j database
    :param statements: list of tuples (statement, parameters) to commit to
                       the neo4j database
    :param max_retry: maximum number of tries to commit the transaction
    :return: Returns true if succeeded, false if not
    """
    success = False
    for _ in range(max_retry):
        try:
            with neo_ctx.begin_transaction() as txn:
                for rowdict, stmt in statements:
                    txn.run(stmt, rowdict)
            success = True
            break
        except TransientError:
            continue

    return success


def create_uniqueness_constraint(neo_ctx, label, prop):
    """
    Given a neo4j context to the neo4j database, create an uniqueness constraint on
    the given index for the specified label

    :rtype: None
    :param neo_ctx: neo4j context to the neo4j database
    :param label: label associated with the node to create the constraint on
    :param prop: property associated with the node to create the constraint on
    """
    statement = "CREATE CONSTRAINT on (n:`{label}`) assert n.`{index}` is unique"\
                .format(label=label, index=prop)
    return neo_ctx.run(statement)


def create_index(neo_ctx, label, prop):
    """
    Given a neo4j context to the neo4j database, create an index on
    the given index for the specified label

    :rtype: None
    :param neo_ctx: neo4j context to the neo4j database
    :param label: label associated with the node to create the constraint on
    :param prop: property associated with the node to create the constraint on
    """
    statement = "CREATE INDEX on :`{label}`(`{index}`)"\
                .format(label=label, index=prop)
    return neo_ctx.run(statement)


def get_indexes(neo_ctx, label=None):
    """
    Given a neo4j context to the neo4j database, get the list of indexes created

    :rtype: list of indexes corresponding to the provided label, if not label is
            given, return all indexes
    :param neo_ctx: neo4j context to the neo4j database
    :param label: the label to retrieve the associated indexes
    """
    statement = "call db.indexes() yield description return description"
    results = neo_ctx.run(statement).data()
    pattern = re.compile(r'create index on :(.*)\((.*)\)')
    results = [pattern.match(k['description'].lower()) for k in results]
    results = [k.groups() for k in results if k]
    results = [k[1] for k in results if k[0] == label or label is None]
    return results


def generate_merge_node_statement(rowdict, hiddenfields, noappendfields, mapping, tags):
    """
    Generate a cypher merge statement to create a node if it did not exist yet,
    and to merge properties into an existing node if it already exist. It is
    assumed that the node will have a special field `_canonical_id` which contains
    an unique id identifying the node, and a special label `_searchable` that
    allows searching across all types of nodes

    :param rowdict: dictionary with keys corresponding to the name of the
                    property fields of the node, and the corresponding values
                    as the property values of the node
    :type rowdict: dict
    :param hiddenfields: a list of fields (properties) to exclude
    :type hiddenfields: list
    :param noappendfields: a list of fields (properties) in which to
                           overwrite any existing values. Fields that are
                           not in excludefields or noappendfields are
                           concatenated with the separator
    :type noappendfields: list
    :param mapping: a dictionary mapping from the field name (key) to the
                    corresponding friendly name (value)
    :type mapping: dict
    :param tags: list of labels to assign to the node
    :type tags: list
    :return: string corresponding to the cypher query to create a node with the
             specified properties by merging with the node if it already existed
    :rtype: str
    """
    # Create statement to add tags
    tag_stmt = generate_set_label_statements(tags)

    # Create statement to add properties
    property_stmt = generate_set_prop_statements(rowdict,
                                                 ['_canonical_id'] + hiddenfields,
                                                 noappendfields,
                                                 mapping)
    statement = ''.join([
        'MERGE (n: _searchable ',
        ' {`_canonical_id`: {_canonical_id} }) ',
        property_stmt,
        tag_stmt])

    return statement

# pylint: disable=too-many-arguments


def generate_merge_edge_statement(rowdict, hiddenfields, noappendfields, mapping,
                                  keyname, tags):
    """
    Generate a cypher merge statement to create an edge if it did not exist yet,
    and to merge properties into an existing edge if it already exist. It is
    assumed that the nodes are created using the library, and hence will have
    the label `_searchable` and correspondingly the property `_canonical_id`.
    The data to be inserted is assumed to have two fields '_canonical_id_source`
    and `_canonical_id_target` that indicates the `_canonical_id` field values
    of the source and target nodes corresponding from which to create an edge.
    The data is also assumed to have the key specified as the keyname parameter.

    :param rowdict: dictionary with keys corresponding to the name of the
                    property fields of the node, and the corresponding values
                    as the property values of the node
    :type rowdict: dict
    :param hiddenfields: a list of fields (properties) to exclude
    :type hiddenfields: list
    :param noappendfields: a list of fields (properties) in which to
                           overwrite any existing values. Fields that are
                           not in excludefields or noappendfields are
                           concatenated with the separator
    :type noappendfields: list
    :param mapping: a dictionary mapping from the field name (key) to the
                    corresponding friendly name (value)
    :type mapping: dict
    :param keyname: the name of the field that should be used to key
                    the edge. Set it to None if there's no primary key
    :type keyname: str
    :param tags: list of labels to assign to the node
    :type tags: list
    :return: string corresponding to the cypher query to create a node with the
             specified properties by merging with the node if it already existed
    :rtype: str
    """

    # Base statement to match the two nodes to form the edge
    base_statement = \
        "MATCH (a:_searchable {_canonical_id: {_canonical_id_source}}) " \
        "MATCH (b:_searchable {_canonical_id: {_canonical_id_target}}) "

    # Statement to create the edge
    merge_edge_statement = \
        "MERGE (a)-[n:`{}` {} ]->(b) {}"

    # Generate the key property to use to identify the edge
    keyprop = generate_relation_key_property(rowdict, keyname, mapping)

    # Get the label of the edge
    relation = \
        tags[0] if isinstance(tags, list) and len(tags) > 0 \
        else tags if isinstance(tags, str) \
        else 'related'

    # Get the properties to set
    property_stmt = generate_set_prop_statements(rowdict,
                                                 [keyname, '_canonical_id_source',
                                                  '_canonical_id_target'] + hiddenfields,
                                                 noappendfields,
                                                 mapping)

    # Generate the statement to add the edge
    statement = base_statement + \
        merge_edge_statement.format(relation, keyprop, property_stmt)

    return statement
