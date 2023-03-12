# %%
## Get Raw Data from Snowflake ##

# from multiprocessing import allow_connection_pickling
import snowflake.connector
import json
import os
from os.path import expanduser
import sys

file_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(file_dir)

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    home_dir,
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    gdrive_path,
    gdrive_path_for_shared,
    data_dir,
    query_dir,
)

from utils.display_tools import print_logger, pprint_df, pprint_ls
from utils.doc_tools import log_data_pipeline


# %%
## Roles ##

dict_roles = {
    "people_insights_user": "US_PEOPLE_INSIGHTS_USER",
    "people_insights_python_role": "SRV_US_PEOPLE_INSIGHTS_PYTHON_ROLE",
    "prodtech": "US_PRODTECH_USER",
    "scm_user": "SCM_DATA_USER",
}


def get_role(role_type_or_role_name):
    if role_type_or_role_name in dict_roles.keys():
        return dict_roles[role_type_or_role_name]
    else:
        return role_type_or_role_name


# %%
## User Password Auth ##


def get_credentials(account_type, role_type):
    role_to_use = get_role(role_type)

    if account_type == "personal":
        cred_file_path = os.path.join(
            expanduser("~"),
            "credentials",
            "personal",
            "snowflake_creds_personal.json",
        )
        with open(
            cred_file_path,
            "r",
        ) as f:
            creds = json.load(f)

        ctx = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            authenticator=creds["authenticator"],
            warehouse=creds["warehouse"],
            role=role_to_use,
        )

    elif account_type == "people_insights_service_account":
        cred_file_path = os.path.join(
            expanduser("~"),
            "credentials",
            "team",
            "snowflake_creds_people_insights_service_account.json",
        )
        with open(
            cred_file_path,
            "r",
        ) as f:
            creds = json.load(f)

        ctx = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=creds["warehouse"],
            role=role_to_use,
        )

    elif account_type == "prod_tech_service_account":
        cred_file_path = os.path.join(
            expanduser("~"),
            "credentials",
            "team",
            "snowflake_creds_prod_tech_service_account.json",
        )
        with open(
            os.path.join(cred_file_path),
            "r",
        ) as f:
            creds = json.load(f)

        ctx = snowflake.connector.connect(
            user=creds["user"],
            password=creds["password"],
            account=creds["account"],
            warehouse=creds["warehouse"],
            role=role_to_use,
        )

    print(
        f"Got Credentials with:\nUser: {creds['user']}\nAccount: {creds['account']}\nWarehouse:{creds['warehouse']}\nRole: {role_to_use}\nFrom file: {cred_file_path}"
    )

    return ctx


# %%
## Get Raw Data from Snowflake ##


def list_tables(account_type, role_type):
    role_to_use = get_role(role_type)

    ctx = get_credentials(account_type, role_to_use)

    cs = ctx.cursor()

    try:
        cs.execute("SHOW TABLES")
        tables = cs.fetchall()
    finally:
        cs.close()
    ctx.close()

    print(type(tables))

    return tables


def query_snowflake(
    account_type,
    role_type,
    query_to_run,
    script_path="",
    function_name="",
    database_table_path="",
):
    print(f"Running query: {query_to_run}")
    ctx = get_credentials(account_type, role_type)

    cs = ctx.cursor()
    try:
        cs.execute(query_to_run)
        query_output = ctx.cursor().execute(query_to_run).fetch_pandas_all()
    finally:
        cs.close()
    ctx.close()

    log_data_pipeline(
        script_path=script_path,
        function_name=function_name,
        input_output="input",
        resource_type="database_table",
        spreadsheet_id="",
        spreadsheet_name="",
        sheet_name="",
        domo_table_name="",
        domo_table_id="",
        file_path="",
        database_table_path=database_table_path.upper(),
    )

    return query_output


def executeScriptsFromFile(
    account_type,
    role_type,
    filename,
    script_path="",
    function_name="",
    database_table_path="",
):
    # Open and read the file as a single buffer
    fd = open(os.path.join(query_dir, filename), "r")
    sqlFile = fd.read()
    fd.close()

    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(";")

    # Execute every command from the input file
    for command in sqlCommands:
        result = query_snowflake(account_type, role_type, command)

    log_data_pipeline(
        script_path=script_path,
        function_name=function_name,
        input_output="input",
        resource_type="database_table",
        spreadsheet_id="",
        spreadsheet_name="",
        sheet_name="",
        domo_table_name="",
        domo_table_id="",
        file_path="",
        database_table_path=database_table_path.upper(),
    )

    return result


# %%


if __name__ == "__main__":
    #######################################################################################
    # query_to_run = """
    # SELECT * FROM "DEMO_DB"."PUBLIC"."DS_D4_DELIVERY_DATES" LIMIT 50
    # """
    # pprint_df(query_snowflake('people_insights_service_account', 'people_insights_python_role', query_to_run))
    #######################################################################################
    # query_to_run = """
    # SELECT current_version()
    # """
    # pprint_df(query_snowflake('people_insights_service_account', 'people_insights_python_role', query_to_run))
    #######################################################################################
    # query_to_run = """
    # SELECT * FROM US_PEOPLE_INSIGHTS.LAYER_ANALYTICS.VIEW_ADP_ACTIVE_PROFILE_FLAT_VIEW
    # LIMIT 100
    # """
    # test_df = query_snowflake('people_insights_service_account', 'people_insights_python_role', query_to_run)
    # pprint_df(test_df)
    # WriteToSheets('TestApp', 'TestSnowflakeOutputService', test_df, set_note='DT')
    #######################################################################################
    # query_to_run = """
    # SELECT * FROM US_PRODTECH.LIGHTNING_PICK.ORDER_COMPLETE_EVENTS
    # WHERE HELLOFRESH_WEEK='2022-W22'
    # LIMIT 100
    # """
    # test_df = query_snowflake('personal', 'prodtech', query_to_run)
    # pprint_df(test_df)

    #######################################################################################

    # query_to_run = """
    # select * from us_people_insights.layer_analytics.VIEW_ADP_PUNCH_SOURCE_REPORT_TRAILING_3_DAYS LIMIT 100
    # """
    # test_df = query_snowflake("people_insights_service_account", "people_insights_python_role", query_to_run)
    # pprint_df(test_df)

    #######################################################################################

    # query_to_run = """
    # SELECT *
    # FROM SCM_DATA.FACT.SHIP_HYBRID_NEEDS_TABLEAU_VW
    # WHERE DELIVERY_WEEK='2023-W01'
    # AND LANE LIKE '%REMAKE'
    # """
    # test_df = query_snowflake("prod_tech_service_account", "prodtech", query_to_run)
    # pprint_df(test_df.head(20))
    # print(test_df.info())

    #######################################################################################

    # works
    query_to_run = """
    select *
    from US_OPS_ANALYTICS.FORECAST.MV_OP_FULFILLMENT_LINE_RATE
    """
    test_df = query_snowflake("prod_tech_service_account", "prodtech", query_to_run)
    pprint_df(test_df.head(20))
    print(test_df.info())

    #######################################################################################

    # need more access
    query_to_run = """
    select *
    from US_PEOPLE_INSIGHTS.DOMO.DAILY_ROSTER_DOMO_TIMEZONES
    limit 50
    """
    test_df = query_snowflake("people_insights_service_account", "public", query_to_run)
    pprint_df(test_df.head(20))
    print(test_df.info())

    #######################################################################################
    pass


# %%
