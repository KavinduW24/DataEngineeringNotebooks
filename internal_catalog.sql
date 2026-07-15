CREATE DATABASE IF NOT EXISTS internal_catalog;
CREATE SCHEMA IF NOT EXISTS internal_catalog.documentation;

CREATE OR REPLACE TABLE internal_catalog.documentation.data_dictionary (
    asset_name STRING,
    database_name STRING,
    schema_name STRING,
    description STRING,
    owner_team STRING,
    business_domain STRING
);

CREATE OR REPLACE VIEW internal_catalog.documentation.all_tables_metadata AS
SELECT 
    table_catalog AS database_name,
    table_schema AS schema_name,
    table_name,
    table_type,
    created,
    last_altered
FROM snowflake.account_usage.tables;
