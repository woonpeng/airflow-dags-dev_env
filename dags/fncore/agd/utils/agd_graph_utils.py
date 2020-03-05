"""
Utils for agd graph transformations
"""
# pylint: disable=import-error
# pylint: disable=no-name-in-module
# pylint: disable=too-many-lines
import functools
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import add_months, col, lit, to_date, trim, year


def _df_col_names_lower(original_df):
    """
    Converts the column names of a dataframe to all lower case.

    :parameter original_df: a spark dataframe
    :type original_df: pyspark.sql.DataFrame
    :return: `df_lower`, dataframe with all the column names in lower case
    :rtype: pyspark.sql.DataFrame
    """
    df_lower = (
        original_df
        .select(
            [col(col_name).alias(col_name.lower())
             for col_name in original_df.columns]
        )
    )

    return df_lower


def _get_dataframe_from_tables(tables, name, graph_spec):
    """
    Returns the appropriate Dataframe from `tables` given the name.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param name: name of the table
    :type name: str
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :return: dataframe of interest
    :rtype: pyspark.sql.DataFrame
    """
    # try edge_list first
    if name in graph_spec.name_to_edge_list:
        edge_list = graph_spec.name_to_edge_list[name]
        table_name = edge_list.safe_table_name
        src_name = edge_list.source_column.safe_name
        dst_name = edge_list.target_column.safe_name

        return tables[(table_name, src_name, dst_name)]
    else:
        node_list = graph_spec.name_to_node_list[name]
        table_name = node_list.safe_table_name
        index_name = node_list.index_column.safe_name

        return tables[(table_name, index_name)]


def _format_edges_dataframe(edges, col_names, types, bi_directional=True):
    """
    Format dataframe defining edges. Columns are renamed accordingly.
    Metadata such as Category, Type, Source_Type and Target_Type are added.

    :param edges: dataframe defining edges with
               source, destination, and relationship columns
               with the schema as follows:
             | |-- "src_col": str
             | |-- "dst_col": str
             | |-- "rel_col": str
    :type edges: pyspark.sql.DataFrame
    :param col_names: column names of source, destination and
        relationship, respectively
    :type col_names: tuple[str]
    :param types: category or type of source, destination and
        relationship, respectively
    :type types: tuple[str]
    :param bi_directional: indicates whether edges are bidirectional
    :type bi_directional: bool
    :return: `formatted_edges` with the following schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    src_col_name, dst_col_name, rel_col_name = col_names
    src_type, dst_type, rel_type = types

    formatted_edges = (
        edges
        .select(col(src_col_name).alias('src'),
                col(dst_col_name).alias('dst'),
                col(rel_col_name).alias('relationship'))
        .withColumn('Type', lit(rel_type))
        .withColumn('Source_Type', lit(src_type))
        .withColumn('Target_Type', lit(dst_type))
    )

    if bi_directional:
        rel_type = '_'.join(rel_type.split('_')[::-1])
        formatted_edges = (
            formatted_edges
            .unionAll(
                edges
                .select(col(dst_col_name).alias('src'),
                        col(src_col_name).alias('dst'),
                        col(rel_col_name).alias('relationship'))
                .withColumn('Type', lit(rel_type))
                .withColumn('Source_Type', lit(dst_type))
                .withColumn('Target_Type', lit(src_type))
            )
        )

    return formatted_edges


def division_officer_edges(tables, graph_spec, bi_directional=False):
    """
    Construct division to officer edge through cost centre. Each edge
    represents a relation where an officer is related to a division through
    the department that he receives salary from.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param bi_directional: specifies whether edges are bi-directional
    :type bi_directional: bool
    :return: `division_officer_edges`, with the following schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """

    # division to cost centre (rearrange columns for clarity)
    table_name = 'government cost_centre-division edge'
    cc_division = _get_dataframe_from_tables(tables, table_name, graph_spec)
    division_cc = (
        cc_division
        .select(col('_canonical_id_target').alias('division'),
                col('_canonical_id_source').alias('cost_centre'))
        .distinct()
    )

    # cost centre to government officer (rearrange columns for clarity)
    table_name = 'government officer-cost_centre edge'
    officer_cc = _get_dataframe_from_tables(tables, table_name, graph_spec)
    cc_officer = (
        officer_cc
        .select(col('_canonical_id_target').alias('cost_centre'),
                col('_canonical_id_source').alias('officer'))
        .distinct()
    )

    division_officer = division_cc.join(cc_officer, on='cost_centre')

    # cost centre is also the department id
    # (src, dst, relationship)
    col_names = ('division', 'officer', 'cost_centre')
    types = ('Division', 'Officer', 'cost-centre')
    division_officer = _format_edges_dataframe(
        division_officer,
        col_names,
        types,
        bi_directional=bi_directional
    )

    return division_officer


def officer_dependant_edges(tables, graph_spec, bi_directional=True):
    """
    Construct officer to dependant bidirectional edges through medical.
    Each edge represents a relation where an officer is related to
    another person through medical claims.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param bi_directional: specifies whether edges are bi-directional
    :type bi_directional: bool
    :return: `officer_dependant_biedges`, a dataframe with the following
        schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'government officer-dependant edge'
    relationship_col_name = (
        graph_spec
        .name_to_edge_list[table_name]
        .name_to_metadata_column['RELATIONSHIP']
        .safe_name
    )

    original_officer_dependant = _df_col_names_lower(
        _get_dataframe_from_tables(
            tables,
            table_name,
            graph_spec
        )
    )

    original_officer_dependant = (
        original_officer_dependant
        .select(col('_canonical_id_source').alias('officer'),
                col('_canonical_id_target').alias('dependant'),
                col(relationship_col_name).alias('relationship'))
        .distinct()
    )

    # Officer -> Dependant
    # (src, dst, relationship)
    col_names = ('officer', 'dependant', 'relationship')
    types = ('Officer', 'Dependant', 'officer_dependant')
    officer_dependant = _format_edges_dataframe(
        original_officer_dependant,
        col_names,
        types,
        bi_directional=False
    )

    # Dependant -> Officer
    if bi_directional:
        # (src, dst, relationship)
        col_names = ('dependant', 'officer', 'relationship')
        types = ('Dependant', 'Officer', 'dependant_officer')
        dependant_officer = _format_edges_dataframe(
            original_officer_dependant,
            col_names,
            types,
            bi_directional=False
        )

        officer_dependant = officer_dependant.unionAll(dependant_officer)

    return officer_dependant


def officer_officer_edges(tables, graph_spec, bi_directional=True):
    """
    Construct officer to officer edges through a common address.
    Each edge represents a relation where an officer is related to
    another officer through common address.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param bi_directional: specifies whether edges are bi-directional
    :type bi_directional: bool
    :return: `officer_dependant_biedges`, a dataframe with the following
        schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'acra officer-address edge'

    officer_address = (
        _get_dataframe_from_tables(tables, table_name, graph_spec)
        .select(col("_canonical_id_source").alias("officer"),
                col("_canonical_id_target").alias("address"))
        .distinct()
    )

    # Filter officer-address with not more than one officers
    # staying at an address
    officer_address = (
        officer_address
        .join(officer_address.groupby('address').count().where('count > 1'),
              on='address')
        .select('officer', 'address')
    )

    officer1_address = (
        officer_address
        .withColumnRenamed('officer', 'officer1')
    )

    officer2_address = (
        officer_address
        .withColumnRenamed('officer', 'officer2')
    )

    officer_officer = (
        officer1_address
        .join(officer2_address, on='address')
        .where(col('officer1') < col('officer2'))
        .distinct()
    )

    # (src, dst, relationship)
    col_names = ('officer1', 'officer2', 'address')
    types = ('Officer', 'Officer', 'address')
    officer_officer = _format_edges_dataframe(
        officer_officer,
        col_names,
        types,
        bi_directional=bi_directional
    )

    return officer_officer


# pylint: disable=too-many-locals, too-many-arguments
def acra_officer_business_edges(tables,
                                graph_spec,
                                transactions_date=None,
                                appointment_buffer_days=0,
                                withdrawn_buffer_days=0):
    """
    Construct officer to vendor edges for non-shareholders from multiple
    tables. Each edge represents a relation where an officer is
    a directory/manager/owner, etc. of a business entity.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param transactions_date: date of interest, (`date` <= `fn_date_withdrawn`
        or `fn_date_withdrawn` is null) and `date` >= `fn_appt_date`
    :type transactions_date: str
    :param appointment_buffer_days: number of days to buffer for appointment
        date
    :type appointment_buffer_days: int
    :param withdrawn_buffer_days: number of days to buffer for date withdrawn
    :type withdrawn_buffer_days: int
    :return: `officer_dependant_biedges`, a dataframe with the following
        schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'acra officer-entity edge'
    edge_list = graph_spec.name_to_edge_list[table_name]
    position_col_name = (
        edge_list
        .name_to_metadata_column['POSITION_HELD']
        .safe_name
    )
    appt_date_col_name = (
        edge_list
        .name_to_metadata_column['APPT_DATE']
        .safe_name
    )
    date_withdrawn_col_name = (
        edge_list
        .name_to_metadata_column['DATE_WITHDRAWN']
        .safe_name
    )

    acra_officer_business = _df_col_names_lower(
        _get_dataframe_from_tables(tables, table_name, graph_spec)
    )

    if transactions_date is not None:
        transactions_date = datetime.strptime(transactions_date, '%Y-%m-%d')

        # appointment date must be earlier than (or on) the transactions
        # date plus a buffer
        appt_date_max = str(
            (transactions_date + timedelta(days=appointment_buffer_days))
            .date()
        )

        # date withdrawn must be later than (or on) transactions date
        # minus a buffer
        date_withdrawn_min = str(
            (transactions_date + timedelta(days=-withdrawn_buffer_days))
            .date()
        )

        query = "{0} <= '{1}' and ({2} >= '{3}' or {2} is null)".format(
            appt_date_col_name,
            appt_date_max,
            date_withdrawn_col_name,
            date_withdrawn_min
        )
        acra_officer_business = acra_officer_business.where(query)

    acra_officer_business = (
        acra_officer_business
        .select(col('_canonical_id_source').alias('src'),
                col('_canonical_id_target').alias('dst'),
                col(position_col_name).alias('relationship'))
        .withColumn('Type', lit('acra-officer_business'))
        # Source type for acra-officer is now the relationship, e.g, Auditor
        .withColumn('Source_Type', col('relationship'))
        .withColumn('Target_Type', lit('Business'))
        .distinct()
    )

    acra_officer_business = (
        acra_officer_business
        .unionAll(
            acra_officer_business
            .select(col('dst').alias('src'),
                    col('src').alias('dst'),
                    col('relationship'))
            .withColumn('Type', lit('business_acra-officer'))
            .withColumn('Source_Type', lit('Business'))
            # Target type for acra-officer is now the relationship, e.g, Auditor
            .withColumn('Target_Type', col('relationship'))
        )
    )

    return acra_officer_business


def shareholder_business_edges(tables, graph_spec, transactions_date=None,
                               alloted_date_buffer=0, bi_directional=True):
    """
    Aggregate shareholder to business edges. Each edge represents
    a relation where a person is a shareholder of a business entity.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param transactions_date: date to filter share_alloted_date
    :type transactions_date: str
    :param alloted_date_buffer: number of days to buffer for
        share_alloted_date
    :type alloted_date_buffer: int
    :param bi_directional: specifies whether edges are bi-directional
    :type bi_directional: bool
    :return: `officer_dependant_biedges_df`, a dataframe with the following
        schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'acra shareholder-entity edge'
    edge_list = graph_spec.name_to_edge_list[table_name]
    alloted_date_col_name = (
        edge_list
        .name_to_metadata_column['SHARE_ALLOTED_DATE']
        .safe_name
    )
    alloted_percent_col_name = (
        edge_list
        .name_to_metadata_column['SHARE_ALLOTED_PERCENT']
        .safe_name
    )

    shareholder_business = _df_col_names_lower(
        _get_dataframe_from_tables(tables, table_name, graph_spec)
    )

    if transactions_date is not None:
        # alloted date must take place before or on the transactions
        # date + a buffer
        alloted_date_max = str(
            (
                datetime.strptime(transactions_date, '%Y-%m-%d')
                + timedelta(days=alloted_date_buffer)
            ).date()
        )

        query = "{0} <= '{1}'".format(
            alloted_date_col_name,
            alloted_date_max
        )
        shareholder_business = shareholder_business.where(query)

    shareholder_business = (
        shareholder_business
        .select(col('_canonical_id_source').alias('shareholder'),
                col('_canonical_id_target').alias('business'),
                col(alloted_percent_col_name).alias('alloted_percent'))
        .distinct()
    )

    # (src, dst, relationship)
    col_names = ('shareholder', 'business', 'alloted_percent')
    types = ('Shareholder', 'Business', 'alloted-fraction')
    shareholder_business = _format_edges_dataframe(
        shareholder_business,
        col_names,
        types,
        bi_directional=bi_directional
    )

    return shareholder_business


def business_business_edges(tables, graph_spec, bi_directional=True):
    """
    Construct vendor to vendor edges through common bank account.
    Each edge represents a relation where vendor is related to another
    vendor through a common bank account.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param bi_directional: specifies whether edges are bi-directional
    :type bi_directional: bool
    :return: `officer_dependant_biedges_df`, a dataframe with the following
        schema:
            | |-- src: str
            | |-- dst: str
            | |-- relationship: str
            | |-- Type: str
            | |-- Source_Type: str
            | |-- Target_Type: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'vendor entity-bank_account edge'

    edge_list = graph_spec.name_to_edge_list[table_name]
    bank_id_col_name = (
        edge_list
        .name_to_metadata_column['BNK_ID_NBR']
        .safe_name
    )
    branch_id_col_name = (
        edge_list
        .name_to_metadata_column['BRANCH_ID']
        .safe_name
    )

    business_bankaccount = _df_col_names_lower(
        _get_dataframe_from_tables(tables, table_name, graph_spec)
    )

    business_bankaccount = (
        business_bankaccount
        .select(
            col('_canonical_id_source').alias('business'),
            trim(col(bank_id_col_name)).alias('bank'),
            trim(col(branch_id_col_name)).alias('branch'),
            trim(col('_canonical_id_target')).alias('bank_account')
        )
    )

    business_bankaccount = (
        business_bankaccount.filter("bank_account != ''").distinct()
    )

    # select only non-singletons
    join_condition = ['bank', 'branch', 'bank_account']
    business_bankaccount = (
        business_bankaccount
        .join(business_bankaccount.groupby(*join_condition).count().where('count > 1'),
              on=join_condition)
        .drop('count')
    )

    business1_bankaccount = (
        business_bankaccount.withColumnRenamed('business', 'business1')
    )

    business2_bankaccount = (
        business_bankaccount.withColumnRenamed('business', 'business2')
    )

    business_business = (
        business1_bankaccount
        .join(business2_bankaccount, on=join_condition)
        .where(col('business1') < col('business2'))
        .distinct()
    )

    col_names = ('business1', 'business2', 'bank_account')
    types = ('Business', 'Business', 'bank-account')
    business_business = _format_edges_dataframe(
        business_business,
        col_names,
        types,
        bi_directional=bi_directional
    )

    return business_business


def vendor_table(tables, graph_spec):
    """
    Return a dataframe that contains vendor ids and their names.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column`
                        for node list.
    :type tables: dict
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :return: a dataframe that contains vendor, and vendor name.
        The schema is as follows:
            | |-- vendor: str
            | |-- vendor_name: str
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'vendor vendor node'
    node_list = graph_spec.name_to_node_list[table_name]

    vendor = _df_col_names_lower(
        _get_dataframe_from_tables(
            tables,
            table_name,
            graph_spec
        )
    )

    vendor_name_col_name = (
        node_list
        .name_to_metadata_column['VENDOR_FIRSTNAME']
        .safe_name
    )

    vendor = (
        vendor
        .select(
            col('_canonical_id').alias('vendor'),
            col(vendor_name_col_name).alias('vendor_name')
        )
    )

    return vendor


# pylint: disable=invalid-name
def vendor_subbusiness_transactions_table(tables,
                                          transactions_date,
                                          graph_spec):
    """
    Return a dataframe that contains vendor, voucher id, sub-business unit
    for a given transactions date.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column` for
                       node list.
    :type tables: dict
    :param transactions_date: date of the vouchers for checking
    :type transactions_date: str
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :return: a dataframe that contains vendor, voucher id, sub-business unit
        for a given transactions date. The schema is as follows:
            | |-- vendor: str
            | |-- subbusiness_unit: str
            | |-- voucher_id: str
            | |-- gross_amt: decimal(27,3)
            | |-- invoice_dt: date
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'voucher sub_business_unit-entity edge'
    edge_list = graph_spec.name_to_edge_list[table_name]

    voucher_id_col_name = edge_list.index_column.safe_name.lower()
    gross_amt_col_name = (
        edge_list
        .name_to_metadata_column['GROSS_AMT_BSE']
        .safe_name
    )
    invoice_dt_col_name = (
        edge_list
        .name_to_metadata_column['INVOICE_DT']
        .safe_name
    )
    account_code_col_name = (
        edge_list
        .name_to_metadata_column['ACCOUNT']
        .safe_name
    )
    fiscal_year_col_name = (
        edge_list
        .name_to_metadata_column['ACCOUNTING_DATE']
        .safe_name
    )

    # 1. Get vendor to subbusiness unit relations
    # subbusiness_vendor: (source) subbusiness unit, (target) vendor
    # (metadata) voucher_id, gross_amt, invoice_dt, fiscal_year
    subbusiness_vendor = _df_col_names_lower(
        _get_dataframe_from_tables(tables, table_name, graph_spec)
    )

    vendor_subbusiness_transactions = (
        subbusiness_vendor
        .select(
            col('_canonical_id_source').alias('subbusiness_unit'),
            col('_canonical_id_target').alias('vendor'),
            col(voucher_id_col_name).alias('voucher_id'),
            col(account_code_col_name).alias('account_code'),
            col(gross_amt_col_name).alias('gross_amt'),
            to_date(col(invoice_dt_col_name)).alias('invoice_dt'),
            year(add_months(to_date(col(fiscal_year_col_name)), -3)).alias('fiscal_year'))
        .filter("invoice_dt='{}'".format(transactions_date))
        # Remove account codes starting with 1 which are EOM payments
        .filter("substr(account_code, 0, 1) != '1'")
    )

    # 2. Get vendor to vendor_name
    vendor = vendor_table(tables, graph_spec)

    # 3. Join vendor with vendor_subbusiness_transactions
    vendor_subbusiness_transactions = vendor_subbusiness_transactions.join(vendor, on='vendor')

    return vendor_subbusiness_transactions


# pylint: disable=invalid-name
def division_vendor_transactions_table(tables, transactions_date, graph_spec):
    """
    Return a dataframe that contains transactions between vendor and
    division for a given date.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column`
                        for node list.
    :type tables: dict
    :param transactions_date: date of the vouchers for checking
    :type transactions_date: str
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :return: a dataframe that contains division and vendor for transactions
        on given a date. The schema is as follows:
            | |-- division: str
            | |-- vendor: str
            | |-- subbusiness_unit: str
            | |-- voucher_id: str
            | |-- gross_amt: decimal(27,3)
            | |-- invoice_dt: date
    :rtype: pyspark.sql.DataFrame
    """
    # sub-business unit to division relations
    table_name = 'government sub_business_unit-division edge'

    subbusiness_unit_division = _df_col_names_lower(
        _get_dataframe_from_tables(tables, table_name, graph_spec)
    )

    subbusiness_unit_division = (
        subbusiness_unit_division
        .select(
            col('_canonical_id_source').alias('subbusiness_unit'),
            col('_canonical_id_target').alias('division')
        )
    )

    # obtain division to vendor relations for a given transactions date
    # by joining on subbusiness_unit
    # rearrange columns for clarity
    vendor_subbusiness_transactions = vendor_subbusiness_transactions_table(
        tables,
        transactions_date,
        graph_spec
    )

    division_vendor_transactions = (
        vendor_subbusiness_transactions
        .join(subbusiness_unit_division, on='subbusiness_unit')
        .select(
            'division', 'vendor', 'vendor_name', 'subbusiness_unit',
            'account_code', 'voucher_id', 'gross_amt', 'invoice_dt',
            'fiscal_year'
        )
    )

    return division_vendor_transactions


def debarred_vendor_table(tables, transactions_date, graph_spec):
    """
    Return a dataframe that contains vendors who have been debarred on
    the date of transactions.

    :param tables: dictionary that maps (table_name, src_name, dst_name)
                   or (table_name, node_name) to the appropriate DataFrame.
                   - "table_name" is the name of the table.
                   - "src_name"/"dst_name" are the `safe_name` of the
                       `source_column`/`target_column` for the edge list.
                   - "node_name" is the `safe_name` of the `index_column`
                        for node list.
    :type tables: dict
    :param transactions_date: date of the vouchers for checking
    :type transactions_date: str
    :param graph_spec: graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :return: a dataframe that contains vendor, debarred_from and debarred_to.
        The schema is as follows:
            | |-- vendor: str
            | |-- debarred_from_dt: date
            | |-- debarred_to_dt: date
    :rtype: pyspark.sql.DataFrame
    """
    table_name = 'gebiz_debarred vendor node'
    node_list = graph_spec.name_to_node_list[table_name]

    debarred_vendor = _df_col_names_lower(
        _get_dataframe_from_tables(
            tables,
            table_name,
            graph_spec
        )
    )

    debarred_vendor_name_to_col_name = (
        node_list
        .name_to_metadata_column['ENT_NAME']
        .safe_name
    )
    debarred_from_col_name = (
        node_list
        .name_to_metadata_column['SCOD_PERIOD_FROM']
        .safe_name
    )
    debarred_to_col_name = (
        node_list
        .name_to_metadata_column['SCOD_PERIOD_TO']
        .safe_name
    )

    debarred_vendor = (
        debarred_vendor
        .select(
            col('_canonical_id').alias('debarred_vendor'),
            col(debarred_vendor_name_to_col_name).alias('debarred_vendor_name'),
            to_date(col(debarred_from_col_name)).alias('debarred_from_dt'),
            to_date(col(debarred_to_col_name)).alias('debarred_to_dt')
        )
    )

    query = (
        "(debarred_from_dt <= '{0}' or debarred_from_dt is null)"
        "and (debarred_to_dt >= '{0}' or debarred_to_dt is null)"
        .format(transactions_date)
    )
    debarred_vendor = debarred_vendor.where(query)

    return debarred_vendor


def load_agd_coi_edges(tables, graph_spec, transactions_date):
    """
    Loads the relevant edges for agd coi detection.

    :param tables: a dictionary containing the mapping from node/edge
        to dataframe
    :type tables: dict[str, pyspark.sql.DataFrame]
    :param graph_spec: the graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param transactions_date: date of the vouchers for checking
    :type transactions_date: str
    :return: `edges`, the relevant edges to consider for bfs
    :type: pyspark.sql.DataFrame
    """
    list_edges = list()
    list_edges.append(division_officer_edges(tables, graph_spec))
    list_edges.append(officer_dependant_edges(tables, graph_spec))
    list_edges.append(officer_officer_edges(tables, graph_spec))
    list_edges.append(
        acra_officer_business_edges(tables, graph_spec, transactions_date)
    )
    list_edges.append(
        shareholder_business_edges(tables, graph_spec, transactions_date)
    )
    list_edges.append(business_business_edges(tables, graph_spec))

    edges = functools.reduce(DataFrame.unionAll, list_edges)
    edges = edges.dropna().distinct()

    return edges


def load_agd_greylist_edges(tables, graph_spec, transactions_date):
    """
    Loads the relevant edges for agd grey list detection.

    :param tables: a dictionary containing the mapping from node/edge
        to dataframe
    :type tables: dict[str, pyspark.sql.DataFrame]
    :param graph_spec: the graph specification
    :type graph_spec: fncore.utils.graph_specification.GraphSpec
    :param transactions_date: date of the vouchers for checking
    :type transactions_date: str
    :return: `edges`, the relevant edges to consider for bfs
    :type: pyspark.sql.DataFrame
    """
    list_edges = list()
    list_edges.append(officer_dependant_edges(tables, graph_spec))
    list_edges.append(officer_officer_edges(tables, graph_spec))
    list_edges.append(
        acra_officer_business_edges(tables, graph_spec, transactions_date)
    )
    list_edges.append(
        shareholder_business_edges(tables, graph_spec, transactions_date)
    )
    list_edges.append(business_business_edges(tables, graph_spec))

    edges = functools.reduce(DataFrame.unionAll, list_edges)
    edges = edges.dropna().distinct()

    return edges
