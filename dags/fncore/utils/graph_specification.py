# coding=utf-8
"""
Graph specification provides a standard interface and format for data about
ingestion pipelines.
"""
import os
from collections import Counter, OrderedDict

from croniter import (CroniterBadCronError, CroniterBadDateError,
                      CroniterNotAlphaError, croniter)
from marshmallow import (Schema, ValidationError, fields, post_dump,
                         validates_schema)

# The location of urlparse varies between Python 2 and 3
try:
    from urllib.parse import unquote
except ImportError:
    from urlparse import unquote


def generate_safe_name(unsafe_name):
    """ creates strings safe for use as parquet tables/columns """
    safe_name = ['fn_'] + [character
                           if character.isalnum()
                           else character.replace(character, '_')
                           for character
                           in unsafe_name.strip()]

    return ''.join(safe_name).lower()


def validate_variable_definition(text):
    """Validate variable definition field."""
    if text not in ['String', 'Price', 'Categorical']:
        message = "Variable definition must be " \
                  "'String', 'Price' or 'Categorical'."
        raise ValidationError(message)


def validate_cron(expression):
    """Validate cron format"""
    try:
        _ = croniter(expression)
    except (CroniterNotAlphaError, CroniterBadDateError, CroniterBadCronError):
        raise ValidationError("Invalid cron expression.")


# pylint: disable=no-init
class NoNullDump(object):  # pylint: disable= too-few-public-methods
    """ Strip null values before dumping """
    # pylint: disable=no-self-use
    @post_dump
    def strip_null(self, data):
        """ Strip null values before dumping """
        return {k: v for (k, v) in data.items() if v is not None}


class ColumnSchema(Schema, NoNullDump):
    """ Basic column schema """
    name = fields.Str(required=True)
    safe_name = fields.Str(dump_only=True)
    friendly_name = fields.Str()
    variable_definition = fields.Str(validate=validate_variable_definition)
    resolution_alias = fields.Str()
    hidden = fields.Bool(default=False)
    use_as_label = fields.Bool(default=False)


class NodeListSchema(Schema, NoNullDump):
    """ NodeList list schema """
    name = fields.Str(required=True)
    safe_name = fields.Str(dump_only=True)
    table_name = fields.Str(required=True)
    safe_table_name = fields.Str(dump_only=True)
    index_column = fields.Nested(ColumnSchema, required=True)
    metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    tags = fields.List(fields.Str())


class EdgeListSchema(Schema, NoNullDump):
    """ Edge list schema """
    name = fields.Str(required=True)
    safe_name = fields.Str(dump_only=True)
    table_name = fields.Str(required=True)
    safe_table_name = fields.Str(dump_only=True)
    index_column = fields.Nested(ColumnSchema, required=False)
    source_column = fields.Nested(ColumnSchema, required=True)
    target_column = fields.Nested(ColumnSchema, required=True)
    weight_column = fields.Nested(ColumnSchema, required=False)
    metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    tags = fields.List(fields.Str())


# pylint: disable=no-self-use
class GraphSchema(Schema, NoNullDump):
    """ Graph specification schema """
    name = fields.Str(required=True)
    data_uri = fields.Str(required=False)
    data_uri_env = fields.Str(required=False)
    graph_uri = fields.Str(required=False)
    graph_uri_env = fields.Str(required=False)
    poll = fields.Str(required=True, validate=validate_cron)
    node_lists = fields.Nested(NodeListSchema, many=True, required=False)
    edge_lists = fields.Nested(EdgeListSchema, many=True, required=True)

    @validates_schema
    def check_uri_and_env(self, data):  # pylint: disable=no-self-use
        """ Additional validation """
        # ensure that not both the URI and environment variables are specified
        # and at least one of them is specified
        for uri, uri_env in [('data_uri', 'data_uri_env'),
                             ('graph_uri', 'graph_uri_env')]:

            if uri in data and uri_env in data:
                raise ValidationError(
                    'Graph specification cannot contain both {uri} and '
                    '{uri_env}. Only one of them should be specified.'
                    .format(uri=uri, uri_env=uri_env))
            elif uri not in data and uri_env not in data:
                raise ValidationError(
                    'Graph specification should contain at least {uri} or '
                    '{uri_env}.'
                    .format(uri=uri, uri_env=uri_env))

    @validates_schema
    def check_unique_names(self, data):  # pylint: disable=no-self-use
        """checks that names in 'node_lists' and 'edge_lists' are unique"""
        names = [spec_item['name']
                 for list_name
                 in ["node_lists", "edge_lists"]
                 for spec_item
                 in data[list_name]]

        if len(names) != len(set(names)):
            name_counts = Counter(names)
            repeated = [name
                        for name, count
                        in name_counts.items()
                        if count > 1]
            raise ValidationError("Names are not unique: {}.".format(repeated))


class ColumnSpec(object):
    """Column specification model"""

    # pylint: disable=too-many-arguments
    def __init__(self,
                 name,
                 friendly_name=None,
                 variable_definition=None,
                 resolution_alias=None,
                 hidden=False,
                 use_as_label=False):
        self.name = name
        self.friendly_name = friendly_name
        self.variable_definition = variable_definition
        self.resolution_alias = resolution_alias
        self.hidden = hidden
        self.use_as_label = use_as_label

    @classmethod
    def from_dict(cls, data):
        """Create column from a formatted dict"""
        if data:
            validated_data, errors = ColumnSchema().load(data)
            if errors:
                raise ValueError(errors)
            return cls(**validated_data)
        else:
            return None

    @property
    def safe_name(self):
        """Generate safe name"""
        return generate_safe_name(self.name)


class GListSpec(object):
    """Base class for node list and edge list specification"""

    # pylint: disable=too-many-arguments
    def __init__(self,
                 name,
                 table_name,
                 index_column,
                 metadata_columns=None,
                 tags=None):
        self.name = name
        self.table_name = table_name
        self.index_column = index_column
        self.name_to_metadata_column = OrderedDict()
        metadata_columns = metadata_columns or []
        for metadata_column in metadata_columns:
            self.add_metadata_column(metadata_column)
        self.tags = tags or []

    @property
    def metadata_columns(self):
        """Returns the list of metadata columns"""
        return self.name_to_metadata_column.values()

    @property
    def safe_name(self):
        """Generate safe name"""
        return generate_safe_name(self.name)

    @property
    def safe_table_name(self):
        """Generate safe table name"""
        return generate_safe_name(self.table_name)

    def add_metadata_column(self, metadata_column):
        """Add a metadata column"""
        self.name_to_metadata_column[metadata_column.name] = \
            metadata_column

    def add_tag(self, tag):
        """Add a tag """
        self.tags.append(tag)


class NodeListSpec(GListSpec):
    """Node list model"""
    # pylint: disable=too-many-arguments
    def __init__(self,
                 name,
                 table_name,
                 index_column,
                 metadata_columns=None,
                 tags=None):

        super(NodeListSpec, self).__init__(
            name, table_name, index_column, metadata_columns, tags
        )


class EdgeListSpec(GListSpec):
    """Edge list model"""
    # pylint: disable=too-many-arguments
    def __init__(self,
                 name,
                 table_name,
                 source_column,
                 target_column,
                 weight_column=None,
                 index_column=None,
                 metadata_columns=None,
                 tags=None):

        super(EdgeListSpec, self).__init__(
            name, table_name, index_column, metadata_columns, tags
        )

        self.source_column = source_column
        self.target_column = target_column
        self.weight_column = weight_column


class GraphSpec(object):
    """Graph specification model"""
    # pylint: disable=too-many-instance-attributes
    def __init__(self,
                 name,
                 data_uri=None,
                 graph_uri=None,
                 poll="0 0 * * *"):
        self.name = name
        self.data_uri = data_uri
        self.graph_uri = graph_uri
        self.poll = poll
        self.name_to_node_list = OrderedDict()
        self.name_to_edge_list = OrderedDict()

    def add_node_list(self, node_list):
        """Add a node list"""
        self.name_to_node_list[node_list.name] = node_list

    def add_edge_list(self, edge_list):
        """Add an edge list"""
        self.name_to_edge_list[edge_list.name] = edge_list

    # pylint: disable=too-many-branches
    @classmethod
    def from_dict(cls, data):
        """Create a graph specification from a formatted dictionary."""
        validated_data, errors = GraphSchema().load(data)
        if errors:
            raise ValueError(errors)

        # if the environment variables are specified, replace the corresponding
        # URI from the environment
        for uri, uri_env in [('data_uri', 'data_uri_env'),
                             ('graph_uri', 'graph_uri_env')]:
            if uri_env in validated_data:
                validated_data[uri] = os.environ[validated_data[uri_env]]
            if uri in validated_data:
                validated_data[uri] = unquote(validated_data[uri])

        graph = cls(
            name=validated_data['name'],
            data_uri=validated_data['data_uri'],
            graph_uri=validated_data['graph_uri'],
            poll=validated_data['poll']
        )

        # add the edge lists
        for item in validated_data.get('edge_lists', []):
            edge_list = EdgeListSpec(
                name=item['name'],
                table_name=item['table_name'],
                source_column=ColumnSpec.from_dict(item['source_column']),
                target_column=ColumnSpec.from_dict(item['target_column']),
                weight_column=ColumnSpec.from_dict(item.get('weight_column')),
                index_column=ColumnSpec.from_dict(item.get('index_column'))
            )

            if 'metadata_columns' in item:
                for column_data in item['metadata_columns']:
                    edge_list.add_metadata_column(
                        ColumnSpec.from_dict(column_data)
                    )

            if 'tags' in item:
                for tag in item['tags']:
                    edge_list.add_tag(tag)

            graph.add_edge_list(edge_list)

        # add the node lists
        for item in validated_data.get('node_lists', []):
            node_list = NodeListSpec(
                name=item['name'],
                table_name=item['table_name'],
                index_column=ColumnSpec.from_dict(item['index_column'])
            )

            if 'metadata_columns' in item:
                for column_data in item['metadata_columns']:
                    node_list.add_metadata_column(
                        ColumnSpec.from_dict(column_data)
                    )

            if 'tags' in item:
                for tag in item['tags']:
                    node_list.add_tag(tag)

            graph.add_node_list(node_list)

        return graph

    def to_dict(self):
        """Reverse of 'from_dict' with additional auto-generated fields."""
        data, errors = GraphSchema().dump(self)
        if errors:
            raise ValueError(errors)
        else:
            return data

    @property
    def node_lists(self):
        """ Returns a list of node lists. """
        return self.name_to_node_list.values()

    @property
    def edge_lists(self):
        """ Returns a list of edge lists. """
        return self.name_to_edge_list.values()

    @staticmethod
    def _node_list_extract_cols(tables, node_list, table_names):
        carry = tables['tables'].get((table_names[0], table_names[1]), None)

        try:
            node_id = node_list.get('index_column', {})
            node_id_column = (node_id['name'], node_id['safe_name'])
        except AttributeError:
            node_id_column = None

        try:
            metadata_columns = [(metadata_column.get('name', None),
                                 metadata_column.get('safe_name', None))
                                for metadata_column
                                in node_list.get('metadata_columns', [])]
        except TypeError:
            metadata_columns = []

        all_cols = set([node_id_column] + metadata_columns)
        all_cols = {col for col in all_cols
                    if col not in [None, '', ('', ''), (None, None)]}
        if carry:
            return carry.union(all_cols)
        else:
            return all_cols

    @staticmethod
    def _edge_list_extract_cols(tables, edge_list, table_names):
        carry = tables['tables'].get((table_names[0], table_names[1]), None)

        try:
            source = edge_list.get('source_column', {})
            source_column = (source.get('name'),
                             source.get('safe_name'))
        except AttributeError:
            source_column = None

        try:
            target = edge_list.get('target_column', {})
            target_column = (target.get('name'),
                             target.get('safe_name'))
        except AttributeError:
            target_column = None

        try:
            edge_id = edge_list.get('index_column', {})
            edge_id_column = (edge_id.get('name'),
                              edge_id.get('safe_name'))
        except AttributeError:
            edge_id_column = None

        try:
            weight = edge_list.get('weight_column', {})
            weight_column = (weight.get('name'),
                             weight.get('safe_name'))
        except AttributeError:
            weight_column = None

        try:
            metadata_columns = [(metadata_column.get('name', None),
                                 metadata_column.get('safe_name', None))
                                for metadata_column
                                in edge_list.get('metadata_columns', [])]
        except TypeError:
            metadata_columns = []

        all_cols = set([edge_id_column,
                        source_column,
                        target_column,
                        weight_column] + metadata_columns)
        all_cols = {col for col in all_cols
                    if col not in [None, '', ('', ''), (None, None)]}
        if carry:
            return carry.union(all_cols)
        else:
            return all_cols

    @property
    def table_details(self):
        """Extract meaningful database table details from graph spec"""
        tables = dict()

        tables['connection'] = self.to_dict().get('data_uri')
        if self.to_dict().get('poll_frequency'):
            tables['poll_frequency'] = \
                self.to_dict()['poll']
        else:
            tables['poll_frequency'] = "0 2 * * *"

        tables['tables'] = {}
        for node_list in self.to_dict().get('node_lists', []):
            table = node_list['table_name']
            safe_table = node_list['safe_table_name']
            cols = self._node_list_extract_cols(
                tables, node_list, (table, safe_table))
            tables['tables'][(table, safe_table)] = cols

        for edge_list in self.to_dict().get('edge_lists', []):
            table = edge_list['table_name']
            safe_table = edge_list['safe_table_name']
            cols = self._edge_list_extract_cols(
                tables, edge_list, (table, safe_table))
            tables['tables'][(table, safe_table)] = cols

        return tables


def get_fields_with_property(node_edge_list, prop='hidden'):
    """
    Return a list of fields that have the given property
    """
    pfields = []

    if 'index_column' in node_edge_list:
        if node_edge_list['index_column'].get(prop, False):
            pfields.append(
                node_edge_list['index_column'].get('safe_name')
            )

    if 'source_column' in node_edge_list:
        if node_edge_list['source_column'].get(prop, False):
            pfields.append(
                node_edge_list['source_column'].get('safe_name')
            )

    if 'target_column' in node_edge_list:
        if node_edge_list['target_column'].get(prop, False):
            pfields.append(
                node_edge_list['target_column'].get('safe_name')
            )

    if 'metadata_columns' in node_edge_list:
        for col in node_edge_list['metadata_columns']:
            if col.get(prop, False):
                pfields.append(col.get('safe_name'))

    return pfields


def get_friendly_name_mapping(node_edge_list):
    """
    Returns a dictionary of mapping of the column names into friendly names
    """
    mapping = {}

    if 'index_column' in node_edge_list:
        mapping[node_edge_list['index_column']['safe_name']] = \
            node_edge_list['index_column'].get(
                'friendly_name',
                node_edge_list['index_column']['safe_name']
            )

    if 'source_column' in node_edge_list:
        mapping[node_edge_list['source_column']['safe_name']] = \
            node_edge_list['source_column'].get(
                'friendly_name',
                node_edge_list['source_column']['safe_name']
            )

    if 'target_column' in node_edge_list:
        mapping[node_edge_list['target_column']['safe_name']] = \
            node_edge_list['target_column'].get(
                'friendly_name',
                node_edge_list['target_column']['safe_name']
            )

    if 'metadata_columns' in node_edge_list:
        for col in node_edge_list['metadata_columns']:
            mapping[col['safe_name']] = \
                col.get('friendly_name', col['safe_name'])

    return mapping
