# Import libraries
from google.cloud import bigquery
import pandas as pd

# Set variables
project_id = "your-project-id"
fivetran_datasets = ['facebook_ads',
                     'facebook_pages',
                     'fivetran_log',
                     'google_ads',
                     'google_search_console',
                     'instagram_business',
                     'microsoft_ads',
                     'salesforce_marketing_cloud',
                     'zendesk']

# Loop through provided datasets
for dataset in fivetran_datasets:
    # Get tables in dataset
    client = bigquery.Client(project_id)
    tables = client.list_tables(dataset)
    # Add table names to list
    tables_currently_in_destination_dataset = []
    for table in tables:
        tables_currently_in_destination_dataset.append(table.table_id)
    # loop through list of tables in dataset and get column names from each table
    for table in tables_currently_in_destination_dataset:
        query = """
        SELECT
          column_name
        FROM (
          SELECT
            *
          FROM
            {dataset_name}.INFORMATION_SCHEMA.COLUMNS )
        WHERE
          table_name = "{table_name}"
        """.format(dataset_name = dataset, table_name = table)
        client = bigquery.Client(project_id)
        table_column_names = client.query(query).to_dataframe()
        # If a table has a _fivetran_synced column, we want to partition the table by it
        # Check Fivetran guide about the code to partition a table: https://fivetran.com/docs/destinations/bigquery/partition-table
        if table_column_names['column_name'].str.contains('_fivetran_synced').any():
            query = """
            create or replace table {dataset_name}.copy_{table_name}
            partition by date( _fivetran_synced )  as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
            create or replace table {dataset_name}.{table_name}
            partition by date( _fivetran_synced ) as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
            """.format(dataset_name = dataset, table_name = table)
            response = client.query(query)
            print('{dataset_name}.{table_name} has now been partitioned'.format(dataset_name = dataset, table_name = table))
        else:
            print('{} does not contain _fivetran_synced and is unchanged'.format(table))
    
