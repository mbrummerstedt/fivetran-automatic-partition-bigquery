# Import libraries
from google.cloud import bigquery
import pandas as pd

# Set variables
project_id = "your-project-id"
# Add your fivetran source datasets to the list
fivetran_datasets = [
    'criteo',
    'facebook_ads',
    'google_ads',
    'google_search_console',
    'instagram_business',
    'mysql',
    'segment',
    'snapchat_ads',
    'tiktok_ads'
]

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
        # Get column names from the table in or to check for the column we want to partition by 
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
        if table_column_names['column_name'].str.contains('_fivetran_synced').any():
            primary_keys_first_priority = []
            primary_keys_second_priority = []
            primary_keys_third_priority = []
            # Fivetran ads a _fivetran_id to some tables
            if table_column_names['column_name'].str.contains('_fivetran_id').any():
                primary_keys_first_priority.append('_fivetran_id')
            # Else Fivetran could also use other id columns
            elif table_column_names['column_name'].str.contains('_id').any():
                # Add all column names that contains _id
                primary_keys_third_priority.extend(table_column_names[table_column_names['column_name'].str.contains('_id')]['column_name'])
            # Fivetran also uses columns called id as the primary key for upsert merges sometimes
            if len(table_column_names[table_column_names['column_name'] == "id"]) > 0:
                primary_keys_second_priority.append('id')
            # If the table contained id and/or _fivetran_id they will be used as primary keys
            if len(primary_keys_first_priority + primary_keys_second_priority) > 0:
                primary_keys = primary_keys_first_priority + primary_keys_second_priority
            # Else if we have columns containing _id we will use them in alphabetical order.
            elif len(primary_keys_third_priority) > 0:
                primary_keys = sorted(primary_keys_third_priority)
            # Else we assign an empty list
            else:
                primary_keys = []
            # Save it as a comma seperated string - We make sure a max of 4 keys are selected as BigQuery allows max 4 cluster keys 
            cluster_by_keys = ", ".join(primary_keys[0:4])
            # If the table has any primary keys
            if len(cluster_by_keys) > 0:
                # Create the table as a partitioned and clusered by table
                query = """
                create or replace table {dataset_name}.copy_{table_name}
                partition by date( _fivetran_synced )  
                cluster by {cluster_by_keys} as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
                create or replace table {dataset_name}.{table_name}
                partition by date( _fivetran_synced )  
                cluster by {cluster_by_keys} as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
                """.format(dataset_name = dataset, table_name = table, cluster_by_keys=cluster_by_keys)
                response = client.query(query)
                print('{dataset_name}.{table_name} has now been partitioned and clustered by {cluster_by_keys}'.format(dataset_name = dataset, table_name = table, cluster_by_keys=cluster_by_keys))
            else:
                # If no primary keys were found for the table, then just partition it by _fivetran_synced
                query = """
                create or replace table {dataset_name}.copy_{table_name}
                partition by date( _fivetran_synced )  as select * from {dataset_name}.{table_name}; drop table {dataset_name}.{table_name};
                create or replace table {dataset_name}.{table_name}
                partition by date( _fivetran_synced ) as select * from {dataset_name}.copy_{table_name}; drop table {dataset_name}.copy_{table_name};
                """.format(dataset_name = dataset, table_name = table)
                response = client.query(query)
                print('{dataset_name}.{table_name} has now been partitioned'.format(dataset_name = dataset, table_name = table))
            
        else:
            print('{dataset_name}.{table_name} does not contain _fivetran_synced and is unchanged'.format(dataset_name = dataset, table_name = table))
print('All datasets has now been processed')
