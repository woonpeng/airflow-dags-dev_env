# coding=utf-8
"""
Graph specification provides a standard interface and format for data about
ingestion pipelines.
"""
import os

from croniter import (CroniterBadCronError, CroniterBadDateError,
                      CroniterNotAlphaError, croniter)
from marshmallow import (Schema, ValidationError, fields, post_dump,
                         validates_schema)
from uuid import uuid4

from fncore_py3.utils.dataframe_tools import add_ticks, strip_ticks


def validate_cron(expression):
    """Validate cron format"""
    try:
        _ = croniter(expression)
    except (CroniterNotAlphaError, CroniterBadDateError, CroniterBadCronError):
        raise ValidationError("Invalid cron expression.")


def validate_variable_definition(text):
    """Validate variable definition field.""" # TODO: Why just these 3???
    if text not in ['String', 'Price', 'Categorical']:
        message = "Variable definition must be " \
                  "'String', 'Price' or 'Categorical'."
        raise ValidationError(message)


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
    friendly_name = fields.Str()
    variable_definition = fields.Str(validate=validate_variable_definition)
    resolution_alias = fields.Str()
    hidden = fields.Bool(default=False)
    use_as_label = fields.Bool(default=False)


class IndexSchema(Schema, NoNullDump):
    """ Basic index column schema, similar to column """
    name = fields.Str(required=True)
    variable_definition = fields.Str(validate=validate_variable_definition)
    resolution_alias = fields.Str()
    index_space = fields.Str()


# FIXME: Why is it called a NodeList???
class NodeListSchema(Schema, NoNullDump):
    """ Node List schema """
    name = fields.Str(required=True)
    table_name = fields.Str(required=True)
    table_path = fields.Str(required=False)
    index_column = fields.Nested(IndexSchema, required=True)
    metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    labels = fields.List(fields.Str(), required=False)


class EdgeListSchema(Schema, NoNullDump):
    """ Edge List schema """
    name = fields.Str(required=True)
    table_name = fields.Str(required=True)
    table_path = fields.Str(required=False)
    merge_same = fields.Bool(default=True)
    directed = fields.Bool(default=True)
    # FIXME: deprecate index column? IDK...
    index_column = fields.Nested(IndexSchema, required=False)
    source_column = fields.Nested(IndexSchema, required=True)
    target_column = fields.Nested(IndexSchema, required=True)
    weight_column = fields.Nested(ColumnSchema, required=False)
    metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    source_metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    target_metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    labels = fields.List(fields.Str())
    source_labels = fields.List(fields.Str())
    target_labels = fields.List(fields.Str())
    edge_category = fields.Str()


class CommunityListSchema(Schema, NoNullDump):
    """ Community List schema """
    name = fields.Str(required=True)
    table_name = fields.Str(required=True)
    table_path = fields.Str(required=False)
    index_column = fields.Nested(IndexSchema, required=True)
    metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    labels = fields.List(fields.Str())
    grouping_columns = fields.Nested(ColumnSchema, many=True, required=True)
    edge_labels = fields.List(fields.Str())
    edge_metadata_columns = fields.Nested(ColumnSchema, many=True, required=False)
    edge_category = fields.Str()

    @validates_schema
    def check_grouping_columns(self, data):  # pylint: disable=no-self-use
        """checks at least one grouping column"""
        if len(data["grouping_columns"]) < 1:
            raise ValidationError("Require 1 or more grouping column(s)")


# pylint: disable=no-self-use
class GraphSchema(Schema, NoNullDump):
    """ Graph specification schema """
    name = fields.Str(required=True)
    table_path = fields.Str(required=False)
    data_uri = fields.Str(required=False)
    data_uri_env = fields.Str(required=False)
    graph_uri = fields.Str(required=False)
    graph_uri_env = fields.Str(required=False)
    poll = fields.Str(required=True, validate=validate_cron)
    node_lists = fields.Nested(NodeListSchema, many=True, required=False)
    edge_lists = fields.Nested(EdgeListSchema, many=True, required=False)
    community_lists = fields.Nested(CommunityListSchema, many=True, required=False)
    default_index_space = fields.Str(required=False)

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


def construct_from_schema(schema):
    """Creates inheritable class with from and to dict methods based on defined schema"""
    class DictConstructableCls(object):
        @classmethod
        def from_dict(cls, data):
            """Generic implementation taking schema-conforming data and making class."""
            if data:
                validated_data, errors = schema().load(data)
                if errors:
                    raise ValueError(errors)
                return cls(**validated_data)
            else:
                return None

        def to_dict(self):
            """Reverse of 'from_dict' with additional auto-generated fields."""
            data, errors = schema().dump(self)
            if errors:
                raise ValueError(errors)
            else:
                return data

    return DictConstructableCls


class ColumnSpec(construct_from_schema(ColumnSchema)):
    """Column specification model"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        self.name = strip_ticks(kwargs["name"])
        self.friendly_name = strip_ticks(kwargs.get("friendly_name"))
        self.variable_definition = kwargs.get("variable_definition")
        self.resolution_alias = kwargs.get("resolution_alias")
        self.hidden = kwargs.get("hidden") or False
        self.use_as_label = kwargs.get("use_as_label") or False

    @property
    def col_name(self):
        return add_ticks(self.name)

    @property
    def col_friendly_name(self):
        return add_ticks(self.friendly_name)

    @classmethod
    def construct(cls, data):
        """ Constructor method from dictionary of entries or the object itself """
        if isinstance(data, ColumnSpec):
            return data
        else:
            return cls.from_dict(data)


class IndexSpec(construct_from_schema(IndexSchema)):
    """Index specification model"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        self.name = strip_ticks(kwargs["name"])
        self.variable_definition = kwargs.get("variable_definition")
        self.resolution_alias = kwargs.get("resolution_alias")
        self.index_space = kwargs.get("index_space")

    @property
    def col_name(self):
        return add_ticks(self.name)

    @classmethod
    def construct(cls, data):
        """ Constructor method from dictionary of entries or the object itself """
        if isinstance(data, IndexSpec):
            return data
        else:
            return cls.from_dict(data)


class BaseListSpec(object):
    """Base list specification"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.table_name = kwargs["table_name"]
        self.table_path = kwargs.get("table_path")
        self.index_column = IndexSpec.construct(kwargs.get("index_column"))
        self.metadata_columns = [
            ColumnSpec.construct(col) for col in kwargs.get("metadata_columns", [])
        ]
        self.labels = kwargs.get("labels", [])

    def update_path(self, table_path):
        """ Updates self.table_path with table_path if None """
        self.table_path = self.table_path or table_path
        return self

    def update_space(self, index_space):
        """ Insert index_space if does not exist """
        self.index_column.index_space = self.index_space or index_space
        return self

    @property
    def index_space(self):
        return self.index_column.index_space

    @property
    def export_id_col(self):
        return "{}:ID({})".format(self.index_space, self.index_space)


class NodeListSpec(BaseListSpec, construct_from_schema(NodeListSchema)):
    """Node list specification"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        super(NodeListSpec, self).__init__(**kwargs)


class EdgeListSpec(BaseListSpec, construct_from_schema(EdgeListSchema)):
    """Edge list specification"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        super(EdgeListSpec, self).__init__(**kwargs)
        # merge_same
        self.directed = kwargs.get("directed", True)
        self.source_column = IndexSpec.construct(kwargs["source_column"])
        self.target_column = IndexSpec.construct(kwargs["target_column"])
        # weight_column
        self.source_metadata_columns = [
            ColumnSpec.construct(col) for col in kwargs.get("source_metadata_columns", [])
        ]
        self.target_metadata_columns = [
            ColumnSpec.construct(col) for col in kwargs.get("target_metadata_columns", [])
        ]
        self.source_labels = kwargs.get("source_labels") or []
        self.target_labels = kwargs.get("target_labels") or []
        self.edge_category = kwargs.get("edge_category", str(uuid4()))

    def update_space(self, index_space):
        """ Insert index_space if does not exist """
        self.source_column.index_space = self.source_index_space or index_space
        self.target_column.index_space = self.target_index_space or index_space
        return self

    @property
    def source_index_space(self):
        return self.source_column.index_space

    @property
    def target_index_space(self):
        return self.target_column.index_space

    @property
    def export_source_col(self):
        return ":START_ID({})".format(self.source_index_space)

    @property
    def export_target_col(self):
        return ":END_ID({})".format(self.target_index_space)


class CommunityListSpec(BaseListSpec, construct_from_schema(CommunityListSchema)):
    """Community list model - list of nodes to the community group they belong to"""
    # pylint: disable=too-many-arguments
    def __init__(self, **kwargs):
        super(CommunityListSpec, self).__init__(**kwargs)
        self.grouping_columns = [
            ColumnSpec.construct(col) for col in kwargs.get("grouping_columns", [])
        ]
        self.edge_labels = kwargs.get("edge_labels", [])
        self.edge_metadata_columns = [
            ColumnSpec.construct(col) for col in kwargs.get("edge_metadata_columns", [])
        ]
        self.edge_category = kwargs.get("edge_category", str(uuid4()))

    @property
    def source_index_space(self):
        return self.index_column.index_space

    @property
    def target_index_space(self):
        return self.index_column.index_space

    @property
    def export_source_col(self):
        return ":START_ID({})".format(self.source_index_space)

    @property
    def export_target_col(self):
        return ":END_ID({})".format(self.target_index_space)


class GraphSpec(construct_from_schema(GraphSchema)):
    """Graph specification model"""
    # pylint: disable=too-many-instance-attributes
    def __init__(self, **kwargs):
        self.name = kwargs["name"]
        self.table_path = kwargs.get("table_path")
        self.data_uri = kwargs.get("data_uri") or os.environ.get(kwargs.get("data_uri_env") or "")
        self.graph_uri = kwargs.get("graph_uri") or os.environ.get(kwargs.get("graph_uri_env") or "")
        self.poll = kwargs.get("poll") or "0 0 * * *"
        self.default_index_space = kwargs.get("default_index_space", "_canonical_id")

        self.node_lists = [
            NodeListSpec.from_dict(d).update_path(self.table_path).update_space(self.default_index_space)
            for d in kwargs.get("node_lists") or []
        ]
        self.edge_lists = [
            EdgeListSpec.from_dict(d).update_path(self.table_path).update_space(self.default_index_space)
            for d in kwargs.get("edge_lists") or []
        ]
        self.community_lists = [
            CommunityListSpec.from_dict(d).update_path(self.table_path).update_space(self.default_index_space)
            for d in kwargs.get("community_lists") or []
        ]

        edge_comm_lists = self.edge_lists + self.community_lists
        self.edge_category_set = set([spec.edge_category for spec in edge_comm_lists])
        # Validate that if same edge category, source spaces and target spaces are same
        for edge_category in self.edge_category_set:
            source_spaces = set([
                spec.source_index_space for spec in edge_comm_lists
                if spec.edge_category == edge_category])
            if len(source_spaces) != 1:
                raise ValidationError("Source space for {} is not unique! {} are defined".format(edge_category, source_spaces))

            target_spaces = set([
                spec.target_index_space for spec in edge_comm_lists
                if spec.edge_category == edge_category])
            if len(target_spaces) != 1:
                raise ValidationError("Target space for {} is not unique! {} are defined".format(edge_category, target_spaces))


def get_friendly_name_mapping(node_edge_list):
    """
    Returns a dictionary of mapping of the column names into friendly names
    """
    mapping = dict()

    for col_type in [
        'index_column', 'source_column', 'target_column'
    ]:
        if col_type in node_edge_list:
            mapping[node_edge_list[col_type]['name']] = \
                node_edge_list[col_type].get(
                    'friendly_name',
                    node_edge_list[col_type]['name']
                )

    for col_type in [
        'metadata_columns', 'source_metadata_columns', 'target_metadata_columns',
        'grouping_columns', 'edge_metadata_columns'
    ]:
        if col_type in node_edge_list:
            for col in node_edge_list[col_type]:
                mapping[col['name']] = \
                    col.get('friendly_name', col['name'])

    return mapping
