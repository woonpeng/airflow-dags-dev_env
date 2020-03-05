# coding=utf-8
"""
Setting up py.text fixtures
"""
# pylint: disable=redefined-outer-name
import datetime
import os

import pytest
from pyspark import SparkConf, SQLContext
from pyspark.sql.functions import col
from pyspark.sql.types import *

from fncore.utils.graph_specification import GraphSpec
from fncore.utils.spark_tools import get_spark_context


@pytest.fixture(scope="session")
def spark_context():
    """ fixture for creating a spark context
    """
    conf = (
        SparkConf().setMaster("local[*]")
        .setAppName("pytest-pyspark-local-testing")
        .set("spark.executor.memory", "1g")
        .set("spark.driver.maxResultSize", "1g")
        .set("spark.executor.instances", "1")
        .set("spark.executor.cores", "1")
    )

    with get_spark_context(conf) as sc:
        sc.setLogLevel("ERROR")

        yield sc


@pytest.fixture(scope="session")
def sql_context(spark_context):
    """fixture for sql context"""
    sql_ctx = SQLContext(spark_context)

    return sql_ctx


@pytest.fixture
def test_graph_data(sql_context):  # pylint: disable=redefined-outer-name
    """fixture for test graph data"""
    # Vertex DataFrame
    v = sql_context.createDataFrame([
        ("a", "Alice", 34),
        ("b", "Bob", 36),
        ("c", "Charlie", 30),
        ("d", "David", 29),
        ("e", "Esther", 32),
        ("f", "Fanny", 36),
        ("g", "Gabby", 60)],
        ["id", "name", "age"]
    )
    # Edge DataFrame
    e = sql_context.createDataFrame([
        ("a", "b", "friend"),
        ("b", "c", "follow"),
        ("c", "b", "follow"),
        ("f", "c", "follow"),
        ("e", "f", "follow"),
        ("e", "d", "friend"),
        ("d", "a", "friend"),
        ("a", "e", "friend"),
        ("c", "e", "friend")],
        ["src", "dst", "relationship"]
    )

    return v, e


@pytest.fixture
def test_conflict_graph_spec():
    """fixture for testing specifiying uri using environment in graphspec"""
    test_spec = {
        "name": "test_data",
        "poll": "0 0 * * *",
        "data_uri_env": "data_uri_env",
        "graph_uri_env": "graph_uri_env",
        "data_uri": "data_uri_env",
        "graph_uri": "graph_uri_env",
        "node_lists": [],
        "edge_lists": []
    }

    return test_spec


@pytest.fixture
def test_env_graph_spec():
    """fixture for testing specifiying uri using environment in graphspec"""
    os.environ['data_uri_env'] = 'test_data_uri'
    os.environ['graph_uri_env'] = 'test_graph_uri'
    test_spec = {
        "name": "test_data",
        "poll": "0 0 * * *",
        "data_uri_env": "data_uri_env",
        "graph_uri_env": "graph_uri_env",
        "node_lists": [],
        "edge_lists": []
    }

    return test_spec


@pytest.fixture
def test_graph_spec():
    """fixture for test graphspec"""
    test_spec = {
        "name": "test_data",
        "poll": "0 0 * * *",
        "data_uri": "data_uri_value",
        "graph_uri": "graph_uri_value",
        "node_lists": [
            {
                "table_name": "test_data_chocolate_node_list",
                "name": "chocolate nodes",
                "tags": [
                    "chocolate"
                ],
                "index_column": {
                    "resolution_alias": "chocolate",
                    "variable_definition": "String",
                    "name": "id"
                }
            },
            {
                "table_name": "test_data_sweets_node_list",
                "name": "sweets nodes",
                "tags": [
                    "sweets"
                ],
                "metadata_columns": [
                    {
                        "variable_definition": "String",
                        "friendly_name": "sweetness number",
                        "name": "prop",
                        "use_as_label": "True"
                    }
                ],
                "index_column": {
                    "resolution_alias": "sweets",
                    "variable_definition": "String",
                    "name": "id"
                }
            },
            {
                "table_name": "test_data_toffee_node_list",
                "name": "toffee nodes",
                "tags": [
                    "toffee"
                ],
                "metadata_columns": [
                    {
                        "variable_definition": "String",
                        "friendly_name": "sweetness number",
                        "name": "prop",
                        "use_as_label": "True"
                    },
                    {
                        "variable_definition": "String",
                        "friendly_name": "hidden value",
                        "name": "hide",
                        "hidden": "True"
                    }
                ],
                "index_column": {
                    "resolution_alias": "id",
                    "variable_definition": "String",
                    "name": "id"
                }
            }
        ],
        "edge_lists": [
            {
                "source_column": {
                    "variable_definition": "String",
                    "name": "chocolate_s"
                },
                "name": "chocolate_relations",
                "tags": [
                    "chocolate"
                ],
                "table_name": "test_data_chocolate_edge_list",
                "target_column": {
                    "variable_definition": "String",
                    "name": "chocolate_t"
                }
            },
            {
                "source_column": {
                    "variable_definition": "String",
                    "name": "sweets_s"
                },
                "name": "sweets_relations",
                "tags": [
                    "sweets"
                ],
                "table_name": "test_data_sweets_edge_list",
                "target_column": {
                    "variable_definition": "String",
                    "name": "sweets_t"
                }
            },
            {
                "source_column": {
                    "variable_definition": "String",
                    "name": "toffee_s"
                },
                "name": "toffee_relations",
                "tags": [
                    "toffee"
                ],
                "table_name": "test_data_toffee_edge_list",
                "target_column": {
                    "variable_definition": "String",
                    "name": "toffee_t"
                }
            }
        ]
    }
    return test_spec


@pytest.fixture(scope="module")
def test_agd_graph_spec():
    figov_1 = {
        "name": "test_data",
        "poll": "0 0 * * *",
        "data_uri": "data_uri_value",
        "graph_uri": "graph_uri_value",
        "edge_lists": [{"index_column": {"friendly_name": "Voucher ID",
                                         "name": "VOUCHER_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Account Code",
                                              "name": "ACCOUNT",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Invoice Date",
                                              "name": "INVOICE_DT"},
                                             {"friendly_name": "Accounting Date",
                                              "name": "ACCOUNTING_DATE"},
                                             {"friendly_name": "Due Date",
                                              "name": "DUE_DT"},
                                             {"friendly_name": "Payment Amount",
                                              "name": "GROSS_AMT_BSE",
                                              "variable_definition": "Price"},
                                             {"friendly_name": "Invoice ID",
                                              "name": "INVOICE_ID"}],
                        "name": "voucher sub_business_unit-entity edge",
                        "source_column": {"friendly_name": "Sub Business Unit",
                                          "name": "SUB_BUSINESS_UNIT",
                                          "variable_definition": "String"},
                        "table_name": "FT_ACCOUNTING",
                        "tags": ["Paid"],
                        "target_column": {"friendly_name": "Vendor ID",
                                          "name": "VENDOR_ID",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "government officer-cost_centre edge",
                        "source_column": {"friendly_name": "NRIC",
                                          "name": "PERNO",
                                          "variable_definition": "String"},
                        "table_name": "LU_PAC_PERSON",
                        "tags": ["Receives salary from"],
                        "target_column": {"friendly_name": "Cost Centre",
                                          "name": "CC_CHRG_TO",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "government sub_business_unit-division edge",
                        "source_column": {"friendly_name": "Sub Business Unit",
                                          "name": "SUB_BUSINESS_UNIT",
                                          "variable_definition": "String"},
                        "table_name": "NOTE_SUBBU_DIV",
                        "tags": ["Mapped to"],
                        "target_column": {"friendly_name": "Division",
                                          "name": "DIVISION",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [{"friendly_name": "Position Held",
                                              "name": "POSITION_HELD",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Date Appointed",
                                              "name": "APPT_DATE"},
                                             {"friendly_name": "Date Withdrawn",
                                              "name": "DATE_WITHDRAWN"}],
                        "name": "acra officer-entity edge",
                        "source_column": {"friendly_name": "ID",
                                          "name": "PERSON_ID_NBR",
                                          "variable_definition": "String"},
                        "table_name": "LU_ACRA_OFFICERS",
                        "tags": ["Officer of"],
                        "target_column": {"friendly_name": "UEN",
                                          "name": "UEN",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [{"friendly_name": "Share Alloted Date",
                                              "name": "SHARE_ALLOTED_DATE"},
                                             {"friendly_name": "Shareholdings (%)",
                                              "name": "SHARE_ALLOTED_PERCENT"}],
                        "name": "acra shareholder-entity edge",
                        "source_column": {"friendly_name": "Shareholder ID",
                                          "name": "PERSON_ID_NBR",
                                          "variable_definition": "String"},
                        "table_name": "LU_ACRA_SHAREHOLDERS",
                        "tags": ["Shareholder of"],
                        "target_column": {"friendly_name": "UEN",
                                          "name": "UEN",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [{"friendly_name": "Relationship",
                                              "name": "RELATIONSHIP",
                                              "variable_definition": "String"}],
                        "name": "government officer-dependant edge",
                        "source_column": {"friendly_name": "NRIC",
                                          "name": "OFFICER_PERNO",
                                          "variable_definition": "String"},
                        "table_name": "LU_PAC_DEPENDANT",
                        "tags": ["Related to"],
                        "target_column": {"friendly_name": "Dependent ID",
                                          "name": "DEPENDANT_ID",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [{"friendly_name": "Country",
                                              "name": "COUNTRY"},
                                             {"friendly_name": "Bank ID No.",
                                              "name": "BNK_ID_NBR"},
                                             {"friendly_name": "Branch ID",
                                              "name": "BRANCH_ID"}],
                        "name": "vendor entity-bank_account edge",
                        "source_column": {"friendly_name": "Vendor ID",
                                          "name": "VENDOR_ID",
                                          "variable_definition": "String"},
                        "table_name": "STG_NFS_PS_VNDR_BANK_ACCT",
                        "tags": ["Bank account"],
                        "target_column": {"friendly_name": "Bank Account No.",
                                          "name": "BANK_ACCOUNT_NUM",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "acra officer-address edge",
                        "source_column": {"friendly_name": "ID",
                                          "name": "PERSON_ID_NBR",
                                          "variable_definition": "String"},
                        "table_name": "LU_ACRA_OFFICERS",
                        "tags": ["Official address"],
                        "target_column": {"friendly_name": "Officer Address",
                                          "name": "PERSON_ADDRESS",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "acra shareholder-address edge",
                        "source_column": {"friendly_name": "Shareholder ID",
                                          "name": "PERSON_ID_NBR",
                                          "variable_definition": "String"},
                        "table_name": "LU_ACRA_SHAREHOLDERS",
                        "tags": ["Official address"],
                        "target_column": {"friendly_name": "Shareholder Address",
                                          "name": "PERSON_ADDRESS",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "government cost_centre-division edge",
                        "source_column": {"friendly_name": "Cost Centre",
                                          "name": "DEPTID",
                                          "variable_definition": "String"},
                        "table_name": "LU_DIVISION",
                        "tags": ["Mapped to"],
                        "target_column": {"friendly_name": "Division",
                                          "name": "DIVISION_LONG_DESC",
                                          "variable_definition": "String"}},
                       {"metadata_columns": [],
                        "name": "acra entity-address edge",
                        "source_column": {"friendly_name": "UEN",
                                          "name": "UEN",
                                          "variable_definition": "String"},
                        "table_name": "LU_ACRA_ENTITIES",
                        "tags": ["Located at"],
                        "target_column": {"friendly_name": "Entity Address",
                                          "name": "REGISTERED_ADDRESS",
                                          "variable_definition": "String"}}],
        "node_lists": [{"index_column": {"friendly_name": "Vendor ID",
                                         "name": "ENT_CODE",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Vendor Name",
                                              "name": "ENT_NAME",
                                              "use_as_label": True,
                                              "variable_definition": "String"},
                                             {"friendly_name": "Debar Start Date",
                                              "name": "SCOD_PERIOD_FROM",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Debar End Date",
                                              "name": "SCOD_PERIOD_TO",
                                              "variable_definition": "String"}],
                        "name": "gebiz_debarred vendor node",
                        "table_name": "STG_GEBIZ_DEBARRED_CASES",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "Vendor ID",
                                         "name": "VENDOR_ID",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Vendor Name",
                                              "name": "VENDOR_FIRSTNAME",
                                              "use_as_label": True,
                                              "variable_definition": "String"}],
                        "name": "vendor vendor node",
                        "table_name": "LU_VENDOR",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "UEN",
                                         "name": "UEN",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Entity Name",
                                              "name": "ENTITY_NAME",
                                              "use_as_label": True,
                                              "variable_definition": "String"},
                                             {"friendly_name": "Entity Address",
                                              "name": "REGISTERED_ADDRESS",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Entity Status",
                                              "name": "ENTITY_STATUS",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Entity Type",
                                              "name": "ENTITY_TYPE",
                                              "variable_definition": "String"}],
                        "name": "acra vendor node",
                        "table_name": "LU_ACRA_ENTITIES",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "UEN",
                                         "name": "UEN",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "acra officer vendor node",
                        "table_name": "LU_ACRA_OFFICERS",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "UEN",
                                         "name": "UEN",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "acra shareholder vendor node",
                        "table_name": "LU_ACRA_SHAREHOLDERS",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "Vendor ID",
                                         "name": "VENDOR_ID",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "bank_account vendor node",
                        "table_name": "STG_NFS_PS_VNDR_BANK_ACCT",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "Vendor ID",
                                         "name": "RESPONDING_SUPPLIER_CODE",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itq vendor node",
                        "table_name": "STG_GEBIZ_ITQ_RESPONSE",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "Vendor ID",
                                         "name": "RESPONDING_SUPPLIER_CODE",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itt vendor node",
                        "table_name": "STG_GEBIZ_ITT_RESPONSE",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "Vendor ID",
                                         "name": "VENDOR_ID",
                                         "resolution_alias": "VENDOR_UEN",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "voucher vendor node",
                        "table_name": "FT_ACCOUNTING",
                        "tags": ["Vendor"]},
                       {"index_column": {"friendly_name": "ID",
                                         "name": "PERSON_ID_NBR",
                                         "resolution_alias": "PERSON_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Position Held",
                                              "name": "POSITION_HELD",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Officer Address",
                                              "name": "PERSON_ADDRESS",
                                              "variable_definition": "String"},
                                             {"friendly_name": "UEN",
                                              "name": "UEN",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Nationality",
                                              "name": "NATIONALITY_DESC",
                                              "variable_definition": "String"}],
                        "name": "acra officer node",
                        "table_name": "LU_ACRA_OFFICERS",
                        "tags": ["Person", "Vendor Officer"]},
                       {"index_column": {"friendly_name": "Shareholder ID",
                                         "name": "PERSON_ID_NBR",
                                         "resolution_alias": "PERSON_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Shareholder Name",
                                              "name": "SHAREHOLDER_NAME1",
                                              "use_as_label": True,
                                              "variable_definition": "String"},
                                             {"friendly_name": "Nationality",
                                              "name": "NATIONALITY_DESC",
                                              "variable_definition": "String"}],
                        "name": "acra shareholder node",
                        "table_name": "LU_ACRA_SHAREHOLDERS",
                        "tags": ["Shareholder"]},
                       {"index_column": {"friendly_name": "NRIC",
                                         "name": "PERNO",
                                         "resolution_alias": "PERSON_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Department Code",
                                              "hidden": True,
                                              "name": "CC_OWNER_DEPT",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Cost Centre",
                                              "hidden": True,
                                              "name": "CC_CHRG_TO",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Payroll Code",
                                              "hidden": True,
                                              "name": "PAYRC",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Date of Birth",
                                              "name": "BIRTH_DTE",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Officer Name",
                                              "name": "OFFICER_NAME",
                                              "use_as_label": True,
                                              "variable_definition": "String"},
                                             {"friendly_name": "Nationality",
                                              "name": "NATIONALITY",
                                              "variable_definition": "String"}],
                        "name": "government officer node",
                        "table_name": "LU_PAC_PERSON",
                        "tags": ["Person", "Government Officer"]},
                       {"index_column": {"friendly_name": "NRIC",
                                         "name": "OFFICER_PERNO",
                                         "resolution_alias": "PERSON_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "dependant officer node",
                        "table_name": "LU_PAC_DEPENDANT",
                        "tags": ["Person", "Dependent"]},
                       {"index_column": {"friendly_name": "Patient ID",
                                         "name": "DEPENDANT_ID",
                                         "resolution_alias": "PERSON_ID",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Officer Name",
                                              "name": "DEPENDANT_NAME",
                                              "use_as_label": True,
                                              "variable_definition": "String"}],
                        "name": "dependant dependant node",
                        "table_name": "LU_PAC_DEPENDANT",
                        "tags": ["Person", "Dependent"]},
                       {"index_column": {"friendly_name": "Officer Address",
                                         "name": "PERSON_ADDRESS",
                                         "resolution_alias": "ADDRESS",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "acra officer address node",
                        "table_name": "LU_ACRA_OFFICERS",
                        "tags": ["Address"]},
                       {"index_column": {"friendly_name": "Shareholder Address",
                                         "name": "PERSON_ADDRESS",
                                         "resolution_alias": "ADDRESS",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "acra shareholder address node",
                        "table_name": "LU_ACRA_SHAREHOLDERS",
                        "tags": ["Address"]},
                       {"index_column": {"friendly_name": "Entity Address",
                                         "name": "REGISTERED_ADDRESS",
                                         "resolution_alias": "ADDRESS",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "acra vendor address node",
                        "table_name": "LU_ACRA_ENTITIES",
                        "tags": ["Address"]},
                       {"index_column": {"friendly_name": "Bank Account No.",
                                         "name": "BANK_ACCOUNT_NUM",
                                         "variable_definition": "String"},
                        "metadata_columns": [{"friendly_name": "Description",
                                              "name": "DESCR",
                                              "variable_definition": "String"},
                                             {"friendly_name": "Bank Account Type",
                                              "name": "BANK_ACCT_TYPE",
                                              "variable_definition": "String"}],
                        "name": "vendor bank_account node",
                        "table_name": "STG_NFS_PS_VNDR_BANK_ACCT",
                        "tags": ["Bank Account"]},
                       {"index_column": {"friendly_name": "Division",
                                         "name": "DIVISION_LONG_DESC",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "division division node",
                        "table_name": "LU_DIVISION",
                        "tags": ["Division"]},
                       {"index_column": {"friendly_name": "Division",
                                         "name": "DIVISION",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "sub_business_unit division node",
                        "table_name": "NOTE_SUBBU_DIV",
                        "tags": ["Division"]},
                       {"index_column": {"friendly_name": "ITQ Code",
                                         "name": "TQ_CODE",
                                         "resolution_alias": "QUOTATION_NUMBER",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itq_response quotation node",
                        "table_name": "STG_GEBIZ_ITQ_RESPONSE",
                        "tags": ["Quotation Number"]},
                       {"index_column": {"friendly_name": "ITQ Code",
                                         "name": "QUOTATION_NO",
                                         "resolution_alias": "QUOTATION_NUMBER",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itq_notice quotation node",
                        "table_name": "STG_GEBIZ_ITQ_NOTICE",
                        "tags": ["Quotation Number"]},
                       {"index_column": {"friendly_name": "ITT Code",
                                         "name": "ITT_CODE",
                                         "resolution_alias": "TENDER_NUMBER",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itt_response quotation node",
                        "table_name": "STG_GEBIZ_ITT_RESPONSE",
                        "tags": ["Tender Number"]},
                       {"index_column": {"friendly_name": "ITT Code",
                                         "name": "ITT_CODE",
                                         "resolution_alias": "TENDER_NUMBER",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itt_notice quotation node",
                        "table_name": "STG_GEBIZ_ITT_NOTICE",
                        "tags": ["Tender Number"]},
                       {"index_column": {"friendly_name": "Sub Business Unit",
                                         "name": "SUB_BUSINESS_UNIT",
                                         "resolution_alias": "SUB_BUSINESS_UNIT",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "voucher sub_business_unit node",
                        "table_name": "FT_ACCOUNTING",
                        "tags": ["Sub Business Unit"]},
                       {"index_column": {"friendly_name": "Sub Business Unit",
                                         "name": "SUB_BUSINESS_UNIT",
                                         "resolution_alias": "SUB_BUSINESS_UNIT",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "sub_business_unit sub_business_unit node",
                        "table_name": "NOTE_SUBBU_DIV",
                        "tags": ["Sub Business Unit"]},
                       {"index_column": {"friendly_name": "Sub Business Unit",
                                         "name": "SUB_BUSINESS_UNIT",
                                         "resolution_alias": "SUB_BUSINESS_UNIT",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itq sub_business_unit node",
                        "table_name": "STG_GEBIZ_ITQ_NOTICE",
                        "tags": ["Sub Business Unit"]},
                       {"index_column": {"friendly_name": "Sub Business Unit",
                                         "name": "SUB_BUSINESS_UNIT",
                                         "resolution_alias": "SUB_BUSINESS_UNIT",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "gebiz_itt sub_business_unit node",
                        "table_name": "STG_GEBIZ_ITT_NOTICE",
                        "tags": ["Sub Business Unit"]},
                       {"index_column": {"friendly_name": "Cost Centre",
                                         "name": "DEPTID",
                                         "resolution_alias": "COST_CENTRE",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "division cost_centre node",
                        "table_name": "LU_DIVISION",
                        "tags": ["Cost Centre"]},
                       {"index_column": {"friendly_name": "Cost Centre",
                                         "name": "CC_CHRG_TO",
                                         "resolution_alias": "COST_CENTRE",
                                         "variable_definition": "String"},
                        "metadata_columns": [],
                        "name": "government cost_centre node",
                        "table_name": "LU_PAC_PERSON",
                        "tags": ["Cost Centre"]}]}

    graph_spec = GraphSpec.from_dict(figov_1)

    return graph_spec


@pytest.fixture(scope='module')
def test_govt_officer_data(sql_context):
    """fixture for test government officer data"""
    columns = [
        'officer', 'cost_centre', 'dependant', 'relationship', 'address'
    ]
    govt_officer_data = sql_context.createDataFrame(
        [('7125751', 'RON', '3438384', 'H', '63409'),
         ('5642028', 'RUB', '2456543', 'S', '63409'),
         ('4208835', 'RON', '1008549', 'H', '33581'),
         ('4016131', 'RON', '6548104', 'W', '77311'),
         ('4186305', 'XDR', '9087516', 'S', '63409'),
         ('5424449', 'RWF', '2746497', 'C', '56988'),
         ('0077954', 'XDR', '3991755', 'H', '22286'),
         ('6782633', 'RUB', '4030586', 'S', '56988'),
         ('1884328', 'SPL', '1366858', 'C', '97640'),
         ('3010327', 'RWF', '5615675', 'C', '77311'),
         ('4021096', 'RWF', '7038136', 'C', '11043'),
         ('2653989', 'XDR', '5521461', 'W', '77311'),
         ('6513965', 'SPL', '4129517', 'W', '11043'),
         ('6868160', 'XDR', '7283638', 'H', '33581'),
         ('0715480', 'SPL', '0042010', 'S', '77311'),
         ('8740303', 'RWF', '4905197', 'S', '33581'),
         ('5107190', 'RON', '4991657', 'C', '56988'),
         ('1979037', 'RON', '9093837', 'C', '22286'),
         ('5891323', 'RWF', '7027433', 'W', '77311'),
         ('6685666', 'RON', '3886456', 'H', '77311')],
        columns
    )

    return govt_officer_data


@pytest.fixture(scope='module')
def test_cc_division_data(sql_context):
    """fixture for test cost-centre division table"""
    columns = ['cost_centre', 'division']
    cc_division_data = sql_context.createDataFrame(
        [('RON', '7382738'),
         ('RUB', '6437291'),
         ('XDR', '6437291'),
         ('RWF', '2487381'),
         ('SPL', '7382738')],
        columns
    )

    return cc_division_data


@pytest.fixture(scope='module')
def test_business_data(sql_context):
    """fixture for test vendor data"""
    columns = [
        'shareholder', 'business', 'bank', 'branch',
        'bank_account', 'alloted_percent', 'alloted_date'
    ]
    business_data = sql_context.createDataFrame(
        [('8402301', '5923330',
          '2718278', '2918928', '2981929',
          0.6, datetime.date(2013, 1, 8)),
         ('1213134', '9010441',
          '2718278', '2918928', '2981929',
          0.3, datetime.date(2016, 3, 13)),
         ('5190855', '2375643',
          '8849328', '2819873', '6748298',
          0.7, datetime.date(2015, 1, 8)),
         ('0655794', '5923330',
          '8849328', '2819873', '6748298',
          0.25, datetime.date(2013, 1, 8)),
         ('2781728', '1775383',
          '7364561', '7346212', '8271827',
          0.7, datetime.date(2013, 7, 16)),
         ('0647211', '2375643',
          '7364561', '7346212', '8271827',
          0.7, datetime.date(2013, 1, 8)),
         ('2295026', '2375643',
          '6273784', '8436328', '8287421',
          0.6, datetime.date(2014, 1, 8)),
         ('8346859', '9010441',
          '7878727', '7387873', '7467282',
          0.1, datetime.date(2017, 6, 8)),
         ('2761883', '1775383',
          '7878727', '7387873', '7467282',
          0.5, datetime.date(2012, 8, 28)),
         ('4427347', '8514988',
          '7878727', '7387873', '7467282',
          0.6, datetime.date(2014, 10, 13))],
        columns
    )

    return business_data


# pylint: disable=invalid-name
@pytest.fixture(scope='module')
def test_acra_officer_business_data(sql_context):
    """fixture for test vendor data"""
    columns = [
        'acra_officer', 'business', 'position', 'appt_date', 'date_withdrawn'
    ]
    acra_officer_business_data = sql_context.createDataFrame(
        [('8402301', '5923330', 'Secretary',
          datetime.date(2013, 1, 8), None),  # 1
         ('5190855', '2375643', 'Director',
          datetime.date(2015, 1, 8), datetime.date(2017, 5, 31)),  # 2
         ('1212345', '1245678', 'Auditor',
          datetime.date(2016, 1, 1), datetime.date(2017, 1, 2)),  # 3
         ('2291234', '1775383', 'Chairman',
          datetime.date(2013, 7, 16), datetime.date(2015, 1, 8)),
         ('2295026', '2375643', 'Secretary',
          datetime.date(2014, 1, 8), datetime.date(2014, 11, 30)),
         ('8346859', '9010441', 'Chairman',
          datetime.date(2016, 11, 8), None),  # 4
         ('1139753', '9010441', 'Public Accountant Employee',
          datetime.date(2016, 3, 13), datetime.date(2017, 6, 8)),  # 5
         ('0283105', '2375643', 'Director',
          datetime.date(2013, 1, 8), datetime.date(2013, 1, 8)),
         ('0655794', '5923330', 'Chairman',
          datetime.date(2013, 1, 8), datetime.date(2013, 1, 8)),
         ('2985425', '1775383', 'Public Accountant Employee',
          datetime.date(2012, 8, 28), None),  # 6
         ('4452472', '8514988', 'Chairman',
          datetime.date(2014, 10, 13), None)],  # 7
        columns
    )

    return acra_officer_business_data


@pytest.fixture(scope='module')
def test_vendor_data(sql_context):
    """fixture for test vendor data"""
    columns = ['vendor', 'vendor_name']
    vendor_data = sql_context.createDataFrame(
        [('7214833', 'ABC'),
         ('2770145', 'BCD'),
         ('1204496', 'CDE'),
         ('8639520', 'DEF'),
         ('6131434', 'EFG'),
         ('8049906', 'FGH'),
         ('0931790', 'GHI'),
         ('4899204', 'HIJ'),
         ('0229956', 'IJK'),
         ('4890283', 'JKL')],
        columns
    )

    return vendor_data


@pytest.fixture(scope='module')
def test_voucher_data(sql_context):
    """fixture for test voucher data"""
    columns = [
        'vendor', 'subbusiness_unit', 'voucher_id',
        'account_code', 'gross_amt', 'invoice_dt',
        'fiscal_year'
    ]
    voucher_data = sql_context.createDataFrame(
        [('7214833', 'RNO', '6088458', '1fkdj', 72119.54, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('2770145', 'RBU', '6439607', 'ujnhy', 46718.74, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('1204496', 'RNO', '1964505', 'abcvd', 64050.93, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('8639520', 'RNO', '3024587', 'imdjs', 51599.43, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('6131434', 'XRD', '6835414', 'eyjcv', 64457.12, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('8049906', 'RFW', '6863683', 'uekvs', 24196.37, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 1, 1)),
         ('0931790', 'XRD', '5330923', 'uxmcu', 65229.77, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 3, 31)),
         ('4899204', 'RBU', '8525261', 'uemjx', 90917.66, datetime.datetime(2017, 1, 1), datetime.datetime(2017, 4, 1)),
         ('0229956', 'SLP', '8808143', 'uvhye', 44962.30, datetime.datetime(2015, 6, 1), datetime.datetime(2015, 6, 1)),
         ('4890283', 'RFW', '2145335', 'poiux', 55850.25, datetime.datetime(2015, 3, 1), datetime.datetime(2015, 3, 1))],
        columns
    )

    return voucher_data


@pytest.fixture(scope='module')
def test_subbusiness_unit_division_data(sql_context):
    """fixture for test voucher data"""
    columns = ['subbusiness_unit', 'division']
    subbusiness_division_data = sql_context.createDataFrame(
        [('RNO', '7382738'),
         ('RBU', '7382738'),
         ('XRD', '6437291'),
         ('RFW', '2487381'),
         ('SLP', '6437291')],
        columns
    )

    return subbusiness_division_data


@pytest.fixture(scope='module')
def test_debarred_vendor_data(sql_context):
    """fixture for test debarred vendor"""
    columns = ['debarred_vendor', 'debarred_vendor_name',
               'debarred_from_dt', 'debarred_to_dt']
    debarred_vendor_data = sql_context.createDataFrame(
        [('7214833', 'ABC', datetime.date(2013, 1, 8), datetime.date(2017, 3, 1)),
         ('7287123', 'KLM', None, None),
         ('3647562', 'LMN', datetime.date(2015, 7, 8), datetime.date(2015, 12, 31)),
         ('3647562', 'LMN', datetime.date(2017, 4, 8), datetime.date(2017, 12, 31))],
        columns
    )

    return debarred_vendor_data


@pytest.fixture(scope='module')
def test_tables(test_govt_officer_data,
                test_cc_division_data,
                test_acra_officer_business_data,
                test_business_data,
                test_vendor_data,
                test_voucher_data,
                test_subbusiness_unit_division_data,
                test_debarred_vendor_data,
                test_agd_graph_spec):
    """fixture for test tables"""
    def _table_src_dst_names(name):
        if name in test_agd_graph_spec.name_to_edge_list:
            edge_list = test_agd_graph_spec.name_to_edge_list[name]
            table_name = edge_list.safe_table_name
            src_name = edge_list.source_column.safe_name
            dst_name = edge_list.target_column.safe_name

            return table_name, src_name, dst_name
        else:
            node_list = test_agd_graph_spec.name_to_node_list[name]
            table_name = node_list.safe_table_name
            index_name = node_list.index_column.safe_name

            return table_name, index_name

    tables = dict()

    # 1. division_officer_edges
    # a. cost centre to division
    table_key = _table_src_dst_names('government cost_centre-division edge')

    tables[table_key] = (
        test_cc_division_data
        .select(col('cost_centre').alias('_canonical_id_source'),
                col('division').alias('_canonical_id_target'))
    )

    # b. government officer to cost centre
    table_key = _table_src_dst_names('government officer-cost_centre edge')
    tables[table_key] = (
        test_govt_officer_data
        .select(col('officer').alias('_canonical_id_source'),
                col('cost_centre').alias('_canonical_id_target'))
    )

    # 2. officer_dependant_edges
    table_name = 'government officer-dependant edge'
    table_key = _table_src_dst_names(table_name)
    relationship_col_name = (
        test_agd_graph_spec
        .name_to_edge_list[table_name]
        .name_to_metadata_column['RELATIONSHIP']
        .safe_name
    )
    tables[table_key] = (
        test_govt_officer_data
        .select(col('officer').alias('_canonical_id_source'),
                col('dependant').alias('_canonical_id_target'),
                col('relationship').alias(relationship_col_name))
    )

    # 3. officer_officer_edges
    table_key = _table_src_dst_names('acra officer-address edge')
    tables[table_key] = (
        test_govt_officer_data
        .select(col('officer').alias('_canonical_id_source'),
                col('address').alias('_canonical_id_target'))
    )

    # 4. acra_officer_business_edges
    table_name = 'acra officer-entity edge'
    table_key = _table_src_dst_names(table_name)
    edge_list = test_agd_graph_spec.name_to_edge_list[table_name]
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

    tables[table_key] = (
        test_acra_officer_business_data
        .select(col('acra_officer').alias('_canonical_id_source'),
                col('business').alias('_canonical_id_target'),
                col('position').alias(position_col_name),
                col('appt_date').alias(appt_date_col_name),
                col('date_withdrawn').alias(date_withdrawn_col_name))
    )

    # 5. shareholder_business_edges
    table_name = 'acra shareholder-entity edge'
    table_key = _table_src_dst_names(table_name)
    edge_list = test_agd_graph_spec.name_to_edge_list[table_name]
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

    tables[table_key] = (
        test_business_data
        .select(col('shareholder').alias('_canonical_id_source'),
                col('business').alias('_canonical_id_target'),
                col('alloted_percent').alias(alloted_percent_col_name),
                col('alloted_date').alias(alloted_date_col_name))
    )

    # 6. business_business_edges
    table_name = 'vendor entity-bank_account edge'
    table_key = _table_src_dst_names(table_name)
    edge_list = test_agd_graph_spec.name_to_edge_list[table_name]
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

    tables[table_key] = (
        test_business_data
        .select(col('business').alias('_canonical_id_source'),
                col('bank_account').alias('_canonical_id_target'),
                col('bank').alias(bank_id_col_name),
                col('branch').alias(branch_id_col_name))
    )

    # 7. vendor table
    table_name = 'vendor vendor node'
    table_key = _table_src_dst_names(table_name)
    node_list = test_agd_graph_spec.name_to_node_list[table_name]
    vendor_name_col_name = (
        node_list
        .name_to_metadata_column['VENDOR_FIRSTNAME']
        .safe_name
    )
    tables[table_key] = (
        test_vendor_data
        .select(col('vendor').alias('_canonical_id'),
                col('vendor_name').alias(vendor_name_col_name))
    )

    # 8. division_vendor_transactions_table
    table_name = 'voucher sub_business_unit-entity edge'
    table_key = _table_src_dst_names(table_name)
    edge_list = test_agd_graph_spec.name_to_edge_list[table_name]
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

    tables[table_key] = (
        test_voucher_data
        .select(col('vendor').alias('_canonical_id_target'),
                col('subbusiness_unit').alias('_canonical_id_source'),
                col('voucher_id').alias(voucher_id_col_name),
                col('account_code').alias(account_code_col_name),
                col('gross_amt').alias(gross_amt_col_name),
                col('invoice_dt').alias(invoice_dt_col_name),
                col('fiscal_year').alias(fiscal_year_col_name))
    )

    # 9. subbusiness unit to division table
    table_name = 'government sub_business_unit-division edge'
    table_key = _table_src_dst_names(table_name)

    tables[table_key] = (
        test_subbusiness_unit_division_data
        .select(col('subbusiness_unit').alias('_canonical_id_source'),
                col('division').alias('_canonical_id_target'))
    )

    # 10. debarred vendor table
    table_name = 'gebiz_debarred vendor node'
    table_key = _table_src_dst_names(table_name)
    node_list = test_agd_graph_spec.name_to_node_list[table_name]
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
    tables[table_key] = (
        test_debarred_vendor_data
        .select(col('debarred_vendor').alias('_canonical_id'),
                col('debarred_vendor_name').alias(debarred_vendor_name_to_col_name),
                col('debarred_from_dt').alias(debarred_from_col_name),
                col('debarred_to_dt').alias(debarred_to_col_name))
    )

    return tables


@pytest.fixture(scope='module')
def test_src_dst_fixed_empty(sql_context, spark_context):
    """fixture for testing empty data for fixed src to dst nodes"""
    schema = StructType([StructField('start', StringType(), False),
                         StructField('end', StringType(), True)])

    src_dst_fixed_data = sql_context.createDataFrame(
        spark_context.emptyRDD(),
        schema
    ).cache()

    return src_dst_fixed_data


@pytest.fixture(scope='module')
def test_edges_empty(sql_context, spark_context):
    """fixture for test empty data for edges"""
    schema = StructType([StructField('src', StringType(), False),
                         StructField('dst', StringType(), False),
                         StructField('Source_Type', StringType(), False),
                         StructField('Target_Type', StringType(), False)])
    edges_data = sql_context.createDataFrame(
        spark_context.emptyRDD(),
        schema
    ).cache()

    return edges_data


@pytest.fixture(scope='module')
def test_src_dst_fixed1(sql_context):
    """fixture for fixed src to dst nodes
        where previous bfs algorithm was unable to detect
    """
    columns = ['start', 'end']
    src_dst_fixed_data = sql_context.createDataFrame(
        [('A\'"-#', 'D'),
         ('B', 'C')],
        columns
    ).cache()

    return src_dst_fixed_data


@pytest.fixture(scope='module')
def test_edges1(sql_context):
    """fixture for edges
        where previous bfs algorithm was unable to detect
    """
    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    edges_data = sql_context.createDataFrame(
        [('A\'"-#', 'C', 'OFFICER', 'VENDOR'),
         ('C', 'D', 'VENDOR', 'VENDOR'),
         ('B', 'D', 'OFFICER', 'VENDOR'),
         ('D', 'C', 'VENDOR', 'VENDOR')],
        columns
    ).cache()

    return edges_data


@pytest.fixture(scope='module')
def test_src_dst_fixed2(sql_context):
    """fixture for fixed src to dst nodes
        where previous bfs algorithm computed the degree
        incorrectly
    """
    columns = ['start', 'end']
    src_dst_fixed_data = sql_context.createDataFrame(
        [('A', 'B'),
         ('A', 'C'),
         ('A', 'D'),
         ('A', 'E')],
        columns
    ).cache()

    return src_dst_fixed_data


@pytest.fixture(scope='module')
def test_edges2(sql_context):
    """fixture for edges
        where previous bfs algorithm computed the degree
        incorrectly
    """
    columns = ['src', 'dst', 'Source_Type', 'Target_Type']
    edges_data = sql_context.createDataFrame(
        [('A', 'B', 'OFFICER', 'VENDOR'),
         ('B', 'C', 'VENDOR', 'VENDOR'),
         ('C', 'D', 'VENDOR', 'VENDOR'),
         ('D', 'E', 'VENDOR', 'VENDOR')],
        columns
    ).cache()

    return edges_data
