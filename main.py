import sys
import datetime
import pprint

import dbtc
import snowflake.connector


class DBTCloudError(Exception):
    pass


class DBTCloudProjectAlreadyExists(DBTCloudError):
    pass


class SnowflakeError(Exception):
    pass


FOUNDATIONAL_DB_SQL_STATEMENTS = [       
    "use role accountadmin;",

    "create database if not exists foundational_db;",
    "create schema if not exists foundational_db.prod;",
    "create or replace warehouse foundational_wh with warehouse_size = xsmall;",

    "create role if not exists foundational_role;",
    "create role if not exists foundational_pii_reader_role;",
    "grant role foundational_pii_reader_role to role foundational_role;",

    "grant usage on database foundational_db to role foundational_role;",
    "grant usage on schema foundational_db.prod to role foundational_role;",
    "grant usage on warehouse foundational_wh to role foundational_role;",
    "grant create schema on database foundational_db to role foundational_role;",
    "grant create table on schema foundational_db.prod to role foundational_role;",
    "grant create view on schema foundational_db.prod to role foundational_role;",

    "grant create tag on schema foundational_db.prod to role foundational_role;",
    "grant create masking policy on schema foundational_db.prod to role foundational_role;",
    "grant apply masking policy on account to role foundational_role;",
    "grant apply tag on account to role foundational_role;",
]

FINANCE_DB_SQL_STATEMENTS = [       
    "use role accountadmin;",

    "create database if not exists finance_db;",
    "create schema if not exists finance_db.prod;",
    "create or replace warehouse finance_wh with warehouse_size = xsmall;",

    "create role if not exists finance_role;",

    "grant usage on warehouse finance_wh to role finance_role;",
    "grant usage on database finance_db to role finance_role;",
    "grant usage on schema finance_db.prod to role finance_role;",
    "grant select on all tables in schema finance_db.prod to role finance_role;",

    "grant create schema on database finance_db to role finance_role;",
    "grant create table on schema finance_db.prod to role finance_role;",
    "grant create view on schema finance_db.prod to role finance_role;",

    "grant usage on database foundational_db to role finance_role;",
    "grant usage on schema foundational_db.prod to role finance_role;",
]

OTHER_SQL_STATEMENTS = [       
    "use role accountadmin;",

    "grant role foundational_role to user {snowflake_username};",
    "grant role foundational_pii_reader_role to user {snowflake_username};",
    "grant role finance_role to user {snowflake_username};",
]


def setup_snowflake(
        snowflake_account,
        snowflake_username,
        snowflake_password,
):
    print(f"Setting up Snowflake {snowflake_account}...")
    con = snowflake.connector.connect(
        user=snowflake_username,
        password=snowflake_password,
        account=snowflake_account,
        login_timeout=10,
    )

    cursor = con.cursor()
    
    for statement in FOUNDATIONAL_DB_SQL_STATEMENTS:
        cursor.execute(statement)

    for statement in FINANCE_DB_SQL_STATEMENTS:
        cursor.execute(statement)

    for statement in OTHER_SQL_STATEMENTS:
        cursor.execute(statement.replace("{snowflake_username}", snowflake_username))

    cursor.close()


def validate_response(response):
    if response['status']['code'] not in [200, 201]:
        raise Exception(pprint.pformat(response['status']))


def _create_account_connection(
        dbt_cloud: dbtc.dbtCloudClient,
        dbt_cloud_account_id, connection_name, snowflake_account_name, database_name, warehouse_name, role_name,
):
    # docs: https://docs.getdbt.com/dbt-cloud/api-v3#/operations/Create%20Account%20Connection

    payload = {
        'name': connection_name,
        'account_id': dbt_cloud_account_id,
        'adapter_version': 'snowflake_v0',
        'config': {
            'account': snowflake_account_name,
            'database': database_name,
            'warehouse': warehouse_name,
            'role': role_name,
        },
    }

    # Usually set via a decorator, hacking it in!
    dbt_cloud.cloud._path = "/api/v3/"

    response = dbt_cloud.cloud._simple_request(
        path=f'accounts/{dbt_cloud_account_id}/connections/',
        method='post',
        json=payload,
    )

    validate_response(response)

    return response


def setup_dbt_cloud(
        dbt_cloud: dbtc.dbtCloudClient,
        dbt_cloud_account_id,
        snowflake_account,
        snowflake_username,
        snowflake_password,
        project_name,
        database_name,
        warehouse_name,
        role_name,
):
    print(f"Setting up dbt Cloud {project_name}...")

    # Check if the target dbt Cloud project name already exists. If so, ask user to delete it first
    response = dbt_cloud.cloud.list_projects(account_id=dbt_cloud_account_id)
    validate_response(response)
    existing_project_names = [datum['name'] for datum in response['data']]

    if project_name in existing_project_names:
        raise DBTCloudProjectAlreadyExists(f'dbt Cloud project "{project_name}" already exists in your account! Please delete it within dbt Cloud before running this script')

    response = _create_account_connection(
        dbt_cloud=dbt_cloud,
        dbt_cloud_account_id=dbt_cloud_account_id,
        connection_name=project_name,
        snowflake_account_name=snowflake_account,
        database_name=database_name,
        warehouse_name=warehouse_name,
        role_name=role_name,
    )

    validate_response(response)
    dbt_cloud_connection_id = response['data']['id']

    response = dbt_cloud.cloud.create_project(
        account_id=dbt_cloud_account_id,
        payload={
            'name': project_name,
            'description': 'Project created for the "Build Data Products and a Data Mesh with dbt Cloud" Snowflake Quickstart guide',
            'connection_id': dbt_cloud_connection_id,
        },
    )

    validate_response(response)
    dbt_cloud_project_id = response['data']['id']

    timestamp = int(datetime.datetime.timestamp(datetime.datetime.now()))

    response = dbt_cloud.cloud.create_managed_repository(
        account_id=dbt_cloud_account_id,
        project_id=dbt_cloud_project_id,
        payload={
            # timestamp for repo uniqueness
            'name': f"repo-{timestamp}",
        },
    )

    validate_response(response)
    dbt_cloud_repository_id = response['data']['id']

    response = dbt_cloud.cloud.update_project(
        account_id=dbt_cloud_account_id,
        project_id=dbt_cloud_project_id,
        payload={
            'name': project_name,
            'description': 'Project created for the "Build Data Products and a Data Mesh with dbt Cloud" Snowflake Quickstart guide',
            'repository_id': dbt_cloud_repository_id,
        },
    )

    validate_response(response)

    # payload example: {"id":null,"account_id":58,"project_id":6111,"type":"snowflake","state":1,"threads":4,"auth_type":"password","user":"sean_m","password":"xyz","schema":"dbt_smcintyre_prod","role":"transformer","database":"analytics","warehouse":"transforming"}
    response = dbt_cloud.cloud.create_credentials(
        account_id=dbt_cloud_account_id,
        project_id=dbt_cloud_project_id,
        payload={
            'type': 'snowflake',
            'user': snowflake_username,
            'password': snowflake_password,
            'schema': 'prod',
            'state': 1,
            'threads': 4,
            'auth_type': 'password',
            'role': role_name,
            'database': database_name,
            'warehouse': warehouse_name,
        },
    )

    validate_response(response)
    dbt_cloud_credentials_id = response['data']['id']

    # payload example: {"id":null,"type":"deployment","deployment_type":"production","name":"Production","account_id":58,"project_id":6111,"connection_id":3593,"state":1,"use_custom_branch":false,"custom_branch":null,"dbt_version":"versionless","supports_docs":false,"credentials_id":20482}
    response = dbt_cloud.cloud.create_environment(
        account_id=dbt_cloud_account_id,
        project_id=dbt_cloud_project_id,
        payload={
            'connection_id': dbt_cloud_connection_id,
            'credentials_id': dbt_cloud_credentials_id,
            'name': 'Production',
            'dbt_version': 'versionless',
            'type': 'deployment',
            'deployment_type': 'production',
            'use_custom_branch': False,
            'supports_docs': True,  # What is this?
        },
    )

    validate_response(response)

    response = dbt_cloud.cloud.create_environment(
        account_id=dbt_cloud_account_id,
        project_id=dbt_cloud_project_id,
        payload={
            'connection_id': dbt_cloud_connection_id,
            'credentials_id': dbt_cloud_credentials_id,
            'name': 'Development',
            'dbt_version': 'versionless',
            'type': 'development',
            'use_custom_branch': False,
            'supports_docs': False,
        },
    )

    validate_response(response)

    # TODO: consider adding in a job creation call here too


def validate_snowflake_account(snowflake_account):
    return snowflake_account


def deploy(
        snowflake_account,
        snowflake_username,
        snowflake_password,
        dbt_cloud_service_token,
        dbt_cloud_account_id,
        dbt_cloud_host,
):
    snowflake_account = validate_snowflake_account(snowflake_account)

    try:
        setup_snowflake(
            snowflake_account=snowflake_account,
            snowflake_username=snowflake_username,
            snowflake_password=snowflake_password,
        )
    except Exception as e:
        raise SnowflakeError(e) from e

    dbt_cloud = dbtc.dbtCloudClient(
        service_token=dbt_cloud_service_token,
        host=dbt_cloud_host,
    )

    try:
        setup_dbt_cloud(
            dbt_cloud=dbt_cloud,
            dbt_cloud_account_id=dbt_cloud_account_id,
            snowflake_account=snowflake_account,
            snowflake_username=snowflake_username,
            snowflake_password=snowflake_password,
            project_name='SFQuickstart: Foundational Project',
            database_name='foundational_db',
            role_name='foundational_role',
            warehouse_name='foundational_wh',
        )

        setup_dbt_cloud(
            dbt_cloud=dbt_cloud,
            dbt_cloud_account_id=dbt_cloud_account_id,
            snowflake_account=snowflake_account,
            snowflake_username=snowflake_username,
            snowflake_password=snowflake_password,
            project_name='SFQuickstart: Finance Project',
            database_name='finance_db',
            role_name='finance_role',
            warehouse_name='finance_wh',
        )
    except Exception as e:
        raise DBTCloudError(e) from e


if __name__ == '__main__':
    deploy(*sys.argv[1:])
