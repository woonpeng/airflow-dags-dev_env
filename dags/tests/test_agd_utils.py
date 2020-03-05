"""Unit testing of agd_graph_utils module."""
import datetime

from pyspark.sql.functions import col, lit, struct

from fncore.agd.tasks.coi import _process_coi_viz
from fncore.agd.tasks.grey_list import (_get_direct_transaction,
                                        _process_grey_list_viz)
from fncore.agd.utils.agd_graph_utils import (acra_officer_business_edges,
                                              business_business_edges,
                                              debarred_vendor_table,
                                              division_officer_edges,
                                              division_vendor_transactions_table,
                                              officer_dependant_edges,
                                              officer_officer_edges,
                                              shareholder_business_edges,
                                              vendor_subbusiness_transactions_table)
from fncore.agd.utils.agd_viz_utils import build_url


def test_build_url():
    """Unit test connstr.py module for building url"""
    url_built = build_url(
        entities=[u'Ah Beng', u'Ah"Beng', u"Ah'Beng", u"Ah+Beng"]
    )
    assert url_built == '/?p="Ah%20Beng"+"Ah%22Beng"+"Ah%27Beng"+"Ah%2BBeng"'


def test_coi_viz(sql_context):
    """Unit test for building url for coi result"""
    columns = ['from_id',
               'e0_src', 'e0_dst', 'e0_relationship',
               'e0_Type', 'e0_Source_Type', 'e0_Target_Type',
               'e1_src', 'e1_dst', 'e1_relationship',
               'e1_Type', 'e1_Source_Type', 'e1_Target_Type',
               'e2_src', 'e2_dst', 'e2_relationship',
               'e2_Type', 'e2_Source_Type', 'e2_Target_Type',
               'e3_src', 'e3_dst', 'e3_relationship',
               'e3_Type', 'e3_Source_Type', 'e3_Target_Type',
               'to_id']

    coi_result = sql_context.createDataFrame(
        [('gov tech',
          'gov tech', 'james', 'data_science',
          'cost-centre', 'Division', 'Officer',
          'james', 'thomas', 'clementi',
          'address', 'Officer', 'Officer',
          'thomas', 'tutber', '0.8',
          'alloted-fraction', 'Shareholder', 'Business',
          'tutber', 'grabster', '0110',
          'bank-account', 'Business', 'Business',
          'grabster'
          )],
        columns
    )

    coi_result = (
        coi_result
        .withColumn('from', struct(col('from_id').alias('id')))
        .withColumn('e0', struct(col('e0_src').alias('src'),
                                 col('e0_dst').alias('dst'),
                                 col('e0_relationship').alias('relationship'),
                                 col('e0_Type').alias('Type'),
                                 col('e0_Source_Type').alias('Source_Type'),
                                 col('e0_Target_Type').alias('Target_Type')))
        .withColumn('e1', struct(col('e1_src').alias('src'),
                                 col('e1_dst').alias('dst'),
                                 col('e1_relationship').alias('relationship'),
                                 col('e1_Type').alias('Type'),
                                 col('e1_Source_Type').alias('Source_Type'),
                                 col('e1_Target_Type').alias('Target_Type')))
        .withColumn('e2', struct(col('e2_src').alias('src'),
                                 col('e2_dst').alias('dst'),
                                 col('e2_relationship').alias('relationship'),
                                 col('e2_Type').alias('Type'),
                                 col('e2_Source_Type').alias('Source_Type'),
                                 col('e2_Target_Type').alias('Target_Type')))
        .withColumn('e3', struct(col('e3_src').alias('src'),
                                 col('e3_dst').alias('dst'),
                                 col('e3_relationship').alias('relationship'),
                                 col('e3_Type').alias('Type'),
                                 col('e3_Source_Type').alias('Source_Type'),
                                 col('e3_Target_Type').alias('Target_Type')))
        .withColumn('to', struct(col('to_id').alias('id')))
        .select('from', 'e0', 'e1', 'e2', 'e3', 'to')
    )

    coi_result = coi_result.cache()
    coi_result.count()

    coi_result = coi_result.unionAll(coi_result).cache()
    assert coi_result.count() == 2

    columns = ['account_code', 'voucher_id', 'subbusiness_unit',
               'division', 'vendor', 'vendor_name', 'gross_amt',
               'invoice_dt', 'fiscal_year']
    division_vendor_transactions = sql_context.createDataFrame(
        [('ac123', 'vi123', 'sbu+123',
          'gov tech', 'grabster', 'grabster_name',
          123.34, datetime.date(2017, 3, 4), 2016)],
        columns
    )

    processed_coi_results = _process_coi_viz(
        coi_result,
        '2017-03-04',
        division_vendor_transactions
    ).collect()

    assert len(processed_coi_results) == 1
    processed_coi = processed_coi_results[0]

    assert processed_coi['transactions_date'] == '2017-03-04'
    assert processed_coi['voucher_id'] == 'vi123'
    assert processed_coi['subbusiness_unit'] == 'sbu+123'
    assert processed_coi['division'] == 'gov tech'
    assert processed_coi['vendor'] == 'grabster'
    assert processed_coi['vendor_name'] == 'grabster_name'
    assert processed_coi['gross_amt'] == 123.34
    assert processed_coi['invoice_dt'] == datetime.date(2017, 3, 4)
    assert processed_coi['fiscal_year'] == 2016
    assert processed_coi['degree_separation'] == 4

    cycle_viz = processed_coi['cycle_viz']
    assert cycle_viz == '/?p="grabster"+"sbu%2B123"+"gov%20tech"+"data_science"+"james"+"clementi"+"thomas"+"tutber"+"0110"+"grabster"'

    coi_node_types = processed_coi['node_types']
    assert coi_node_types == ('Subbusiness_Unit, Division, Department, '
                              'Officer, Address, Shareholder, Business, '
                              'BankAccount, Business')


def test_grey_list_viz(sql_context):
    """Unit test for building url for grey list result"""
    columns = ['from_id',
               'e0_src', 'e0_dst', 'e0_relationship',
               'e0_Type', 'e0_Source_Type', 'e0_Target_Type',
               'e1_src', 'e1_dst', 'e1_relationship',
               'e1_Type', 'e1_Source_Type', 'e1_Target_Type',
               'e2_src', 'e2_dst', 'e2_relationship',
               'e2_Type', 'e2_Source_Type', 'e2_Target_Type',
               'e3_src', 'e3_dst', 'e3_relationship',
               'e3_Type', 'e3_Source_Type', 'e3_Target_Type',
               'to_id']
    grey_list_result = sql_context.createDataFrame(
        [('grabster',
          'grabster', 'james', '0.8',
          'alloted-fraction', 'Business', 'Shareholder',
          'james', 'thomas', 'clementi',
          'address', 'Officer', 'Officer',
          'thomas', 'grabtut', 'Partner',
          'acra-officer_business', 'Person', 'Business',
          'grabtut', 'tutber', '0110',
          'bank-account', 'Business', 'Business',
          'tutber'
          )],
        columns
    )
    grey_list_result = (
        grey_list_result
        .withColumn('from', struct(col('from_id').alias('id')))
        .withColumn('e0', struct(col('e0_src').alias('src'),
                                 col('e0_dst').alias('dst'),
                                 col('e0_relationship').alias('relationship'),
                                 col('e0_Type').alias('Type'),
                                 col('e0_Source_Type').alias('Source_Type'),
                                 col('e0_Target_Type').alias('Target_Type')))
        .withColumn('e1', struct(col('e1_src').alias('src'),
                                 col('e1_dst').alias('dst'),
                                 col('e1_relationship').alias('relationship'),
                                 col('e1_Type').alias('Type'),
                                 col('e1_Source_Type').alias('Source_Type'),
                                 col('e1_Target_Type').alias('Target_Type')))
        .withColumn('e2', struct(col('e2_src').alias('src'),
                                 col('e2_dst').alias('dst'),
                                 col('e2_relationship').alias('relationship'),
                                 col('e2_Type').alias('Type'),
                                 col('e2_Source_Type').alias('Source_Type'),
                                 col('e2_Target_Type').alias('Target_Type')))
        .withColumn('e3', struct(col('e3_src').alias('src'),
                                 col('e3_dst').alias('dst'),
                                 col('e3_relationship').alias('relationship'),
                                 col('e3_Type').alias('Type'),
                                 col('e3_Source_Type').alias('Source_Type'),
                                 col('e3_Target_Type').alias('Target_Type')))
        .withColumn('to', struct(col('to_id').alias('id')))
        .select('from', 'e0', 'e1', 'e2', 'e3', 'to')
    )

    grey_list_result = grey_list_result.cache()
    grey_list_result.count()

    grey_list_result = grey_list_result.unionAll(grey_list_result).cache()
    assert grey_list_result.count() == 2

    columns = ['debarred_vendor', 'debarred_vendor_name',
               'debarred_from_dt', 'debarred_to_dt']
    debarred_vendor = sql_context.createDataFrame(
        [('tutber', 'tutber_name',
          datetime.date(2017, 3, 3),
          datetime.date(2017, 3, 5)
          )],
        columns
    )

    columns = ['account_code', 'voucher_id', 'subbusiness_unit',
               'division', 'vendor', 'vendor_name', 'gross_amt',
               'invoice_dt', 'fiscal_year']
    transacted_vendor = sql_context.createDataFrame(
        [('ac123', 'vi123', 'sbu123', 'hive321', 'grabster',
          'grabster_name', 123.34, datetime.date(2017, 3, 4), 2016
          )],
        columns
    )

    processed_grey_list_results = _process_grey_list_viz(
        grey_list_result,
        '2017-03-04',
        transacted_vendor,
        debarred_vendor
    ).collect()

    assert len(processed_grey_list_results) == 1

    processed_grey_list = processed_grey_list_results[0]

    assert processed_grey_list['transactions_date'] == '2017-03-04'
    assert processed_grey_list['voucher_id'] == 'vi123'
    assert processed_grey_list['subbusiness_unit'] == 'sbu123'
    assert processed_grey_list['division'] == 'hive321'
    assert processed_grey_list['vendor'] == 'grabster'
    assert processed_grey_list['vendor_name'] == 'grabster_name'
    assert processed_grey_list['debarred_vendor'] == 'tutber'
    assert processed_grey_list['debarred_vendor_name'] == 'tutber_name'
    assert processed_grey_list['gross_amt'] == 123.34
    assert processed_grey_list['invoice_dt'] == datetime.date(2017, 3, 4)
    assert processed_grey_list['fiscal_year'] == 2016
    assert processed_grey_list['degree_separation'] == 4
    assert processed_grey_list['debarred_from_dt'] == datetime.date(2017, 3, 3)
    assert processed_grey_list['debarred_to_dt'] == datetime.date(2017, 3, 5)

    grey_list_viz = processed_grey_list['cycle_viz']
    assert grey_list_viz == '/?p="sbu123"+"grabster"+"james"+"clementi"+"thomas"+"grabtut"+"0110"+"tutber"'

    grey_list_node_types = processed_grey_list['node_types']
    assert grey_list_node_types == ('Subbusiness_Unit, Business, '
                                    'Officer, Address, Person, Business, '
                                    'BankAccount, Business')


def test_get_direct_transactions(sql_context):
    """ Unit test for direct transactions """
    columns = ['debarred_vendor', 'debarred_vendor_name',
               'debarred_from_dt', 'debarred_to_dt']
    debarred_vendor = sql_context.createDataFrame(
        [('tutber', 'tutber_name',
          datetime.date(2017, 3, 3),
          datetime.date(2017, 3, 5)
          )],
        columns
    )

    columns = ['account_code', 'voucher_id', 'subbusiness_unit',
               'division', 'vendor', 'vendor_name', 'gross_amt',
               'invoice_dt', 'fiscal_year']
    transacted_vendor = sql_context.createDataFrame(
        [('ac123', 'vi123', 'sbu123', 'hive321', 'tutber',
          'tutber_name', 123.34, datetime.date(2017, 3, 4), 2016),
         ('ac321', 'vi321', 'sbu321', 'hive321', 'tutber',
          'tutber_name', 433.12, datetime.date(2017, 3, 4), 2016),
         ],
        columns
    )

    processed_one_hop_results = _get_direct_transaction(
        '2017-03-04',
        transacted_vendor,
        debarred_vendor
    ).collect()

    assert len(processed_one_hop_results) == 2

    for processed_one_hop in processed_one_hop_results:
        if processed_one_hop['voucher_id'] == 'vi123':
            assert processed_one_hop['account_code'] == 'ac123'
            assert processed_one_hop['transactions_date'] == '2017-03-04'
            assert processed_one_hop['voucher_id'] == 'vi123'
            assert processed_one_hop['subbusiness_unit'] == 'sbu123'
            assert processed_one_hop['division'] == 'hive321'
            assert processed_one_hop['vendor'] == 'tutber'
            assert processed_one_hop['vendor_name'] == 'tutber_name'
            assert processed_one_hop['debarred_vendor'] == 'tutber'
            assert processed_one_hop['debarred_vendor_name'] == 'tutber_name'
            assert processed_one_hop['gross_amt'] == 123.34
            assert processed_one_hop['invoice_dt'] == datetime.date(2017, 3, 4)
            assert processed_one_hop['fiscal_year'] == 2016
            assert processed_one_hop['degree_separation'] == 1
            assert processed_one_hop['debarred_from_dt'] == datetime.date(2017, 3, 3)
            assert processed_one_hop['debarred_to_dt'] == datetime.date(2017, 3, 5)

            one_hop_viz = processed_one_hop['cycle_viz']
            assert one_hop_viz == '/?p="sbu123"+"tutber"'

            one_hop_node_types = processed_one_hop['node_types']
            assert one_hop_node_types == 'Subbusiness_Unit, Business'
        else:
            assert processed_one_hop['account_code'] == 'ac321'
            assert processed_one_hop['transactions_date'] == '2017-03-04'
            assert processed_one_hop['voucher_id'] == 'vi321'
            assert processed_one_hop['subbusiness_unit'] == 'sbu321'
            assert processed_one_hop['division'] == 'hive321'
            assert processed_one_hop['vendor'] == 'tutber'
            assert processed_one_hop['vendor_name'] == 'tutber_name'
            assert processed_one_hop['debarred_vendor'] == 'tutber'
            assert processed_one_hop['debarred_vendor_name'] == 'tutber_name'
            assert processed_one_hop['gross_amt'] == 433.12
            assert processed_one_hop['invoice_dt'] == datetime.date(2017, 3, 4)
            assert processed_one_hop['fiscal_year'] == 2016
            assert processed_one_hop['degree_separation'] == 1
            assert processed_one_hop['debarred_from_dt'] == datetime.date(2017, 3, 3)
            assert processed_one_hop['debarred_to_dt'] == datetime.date(2017, 3, 5)

            one_hop_viz = processed_one_hop['cycle_viz']
            assert one_hop_viz == '/?p="sbu321"+"tutber"'

            one_hop_node_types = processed_one_hop['node_types']
            assert one_hop_node_types == 'Subbusiness_Unit, Business'


def test_division_officer_edges(test_tables, test_agd_graph_spec):
    """test `division_officer_edges`"""
    edges = division_officer_edges(test_tables, test_agd_graph_spec)

    assert edges.select('src').distinct().count() == 3
    assert edges.select('relationship').distinct().count() == 5
    assert edges.select('dst').distinct().count() == 20
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]
    assert (
        edges
        .select('Type', 'Source_Type', 'Target_Type')
        .distinct()
        .count() == 1
    )

    division_officer_1 = edges.first()
    assert division_officer_1.Type == 'cost-centre'
    assert division_officer_1.Source_Type == 'Division'
    assert division_officer_1.Target_Type == 'Officer'


def test_officer_dependant_edges(test_tables, test_agd_graph_spec):
    """test `officer_dependant_edges`"""
    edges = officer_dependant_edges(test_tables, test_agd_graph_spec)

    assert edges.select('src', 'dst').distinct().count() == 40
    assert edges.select('relationship').distinct().count() == 3
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]

    edge_types = {
        r.Type
        for r in edges.select('Type').distinct().collect()
    }
    assert (
        edge_types == {u'dependant_officer', u'officer_dependant'}
    )

    source_types = {
        r.Source_Type
        for r in edges.select('Source_Type').distinct().collect()
    }
    assert source_types == {u'Dependant', u'Officer'}

    target_types = {
        r.Target_Type
        for r in edges.select('Target_Type').distinct().collect()
    }
    assert target_types == {u'Dependant', u'Officer'}


def test_officer_officer_edges(test_tables, test_agd_graph_spec):
    """test `officer_officer_edges`"""
    edges = officer_officer_edges(test_tables, test_agd_graph_spec)

    assert edges.select('src', 'dst', 'relationship').distinct().count() == 52
    assert edges.select('relationship').distinct().count() == 6
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]
    assert (
        edges
        .select('Type', 'Source_Type', 'Target_Type')
        .distinct()
        .count() == 1
    )

    division_officer_1 = edges.first()
    assert division_officer_1.Type == 'address'
    assert division_officer_1.Source_Type == 'Officer'
    assert division_officer_1.Target_Type == 'Officer'


# pylint: disable=invalid-name
def test_acra_officer_business_edges(test_tables, test_agd_graph_spec):
    """test `acra_officer_business_edges`"""
    edges = acra_officer_business_edges(
        test_tables,
        test_agd_graph_spec,
        '2017-01-01'
    )

    assert edges.select('src', 'dst').distinct().count() == 14
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]

    relationships = {
        r.relationship
        for r in edges.select('relationship').distinct().collect()
    }
    assert relationships == {u'Auditor',
                             u'Chairman',
                             u'Director',
                             u'Public Accountant Employee',
                             u'Secretary'}

    types = {
        r.Type
        for r in edges.select('Type').distinct().collect()
    }
    assert types == {u'acra-officer_business', u'business_acra-officer'}

    source_types = {
        r.Source_Type
        for r in edges.select('Source_Type').distinct().collect()
    }
    assert source_types == {u'Auditor',
                            u'Business',
                            u'Chairman',
                            u'Director',
                            u'Public Accountant Employee',
                            u'Secretary'}

    target_types = {
        r.Target_Type
        for r in edges.select('Target_Type').distinct().collect()
    }
    assert target_types == {u'Auditor',
                            u'Business',
                            u'Chairman',
                            u'Director',
                            u'Public Accountant Employee',
                            u'Secretary'}


def test_shareholder_business_edges(test_tables, test_agd_graph_spec):
    """test `shareholder_business_edges`"""
    edges = shareholder_business_edges(
        test_tables,
        test_agd_graph_spec,
        '2017-01-01'
    )

    assert edges.select('src', 'dst').count() == 18
    assert edges.where('relationship >= 0.0 and relationship <= 1.0').count() == 18
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]

    types = {
        r.Type
        for r in edges.select('Type').distinct().collect()
    }
    assert types == {u'alloted-fraction'}

    source_types = {
        r.Source_Type
        for r in edges.select('Source_Type').distinct().collect()
    }
    assert source_types == {u'Business', u'Shareholder'}

    target_types = {
        r.Target_Type
        for r in edges.select('Target_Type').distinct().collect()
    }
    assert target_types == {u'Business', u'Shareholder'}


def test_business_business_edges(test_tables, test_agd_graph_spec):
    """test `business_business_edges`"""
    edges = business_business_edges(test_tables, test_agd_graph_spec)
    assert edges.select('src', 'dst').count() == 12
    assert edges.columns == [
        'src', 'dst', 'relationship', 'Type', 'Source_Type', 'Target_Type'
    ]

    business_business_1 = edges.first()
    assert business_business_1.Type == 'bank-account'
    assert business_business_1.Source_Type == 'Business'
    assert business_business_1.Target_Type == 'Business'


def test_vendor_subbusiness_transactions(test_tables, test_agd_graph_spec):
    """test `_vendor_subbusiness_transactions`"""
    vendor_subbusiness_transactions = vendor_subbusiness_transactions_table(
        test_tables, '2017-01-01', test_agd_graph_spec
    )

    assert (
        vendor_subbusiness_transactions
        .select('vendor')
        .distinct()
        .count()
    ) == 7

    assert (
        vendor_subbusiness_transactions
        .select('vendor_name')
        .distinct()
        .count()
    ) == 7

    assert (
        vendor_subbusiness_transactions
        .select('subbusiness_unit')
        .distinct()
        .count()
    ) == 4

    assert (
        vendor_subbusiness_transactions
        .select('voucher_id')
        .distinct()
        .count()
    ) == 7

    assert (
        vendor_subbusiness_transactions
        .select('invoice_dt')
        .where("invoice_dt != '2017-01-01'")
        .count()
    ) == 0

    assert (
        vendor_subbusiness_transactions
        .select('fiscal_year')
        .where("fiscal_year = 2016")
        .count()
    ) == 6


# pylint: disable=invalid-name
def test_division_vendor_transactions_table(test_tables, test_agd_graph_spec):
    """test `division_vendor_transactions_table`"""
    division_vendor_transactions = division_vendor_transactions_table(
        test_tables, '2017-01-01', test_agd_graph_spec
    )
    assert (
        division_vendor_transactions
        .select('division')
        .distinct()
        .count()
    ) == 3

    assert (
        division_vendor_transactions
        .select('vendor')
        .distinct()
        .count()
    ) == 7

    assert (
        division_vendor_transactions
        .select('vendor_name')
        .distinct()
        .count()
    ) == 7

    assert (
        division_vendor_transactions
        .select('subbusiness_unit')
        .distinct()
        .count()
    ) == 4

    assert (
        division_vendor_transactions
        .where("invoice_dt != '2017-01-01'")
        .distinct()
        .count()
    ) == 0

    assert (
        division_vendor_transactions
        .where("fiscal_year = 2016")
        .distinct()
        .count()
    ) == 6


def test_debarred_vendor_table(test_tables, test_agd_graph_spec):
    """test `debarred_vendor_table`"""
    debarred_vendor = debarred_vendor_table(
        test_tables,
        '2017-01-01',
        test_agd_graph_spec
    )

    assert debarred_vendor.count() == 2
    assert debarred_vendor.select('debarred_vendor_name').distinct().count() == 2

    query = "debarred_from_dt <= '2017-01-01' or debarred_from_dt is null"
    assert debarred_vendor.where(query).count() == 2

    query = "debarred_to_dt >= '2017-01-01' or debarred_to_dt is null"
    assert debarred_vendor.where(query).count() == 2
