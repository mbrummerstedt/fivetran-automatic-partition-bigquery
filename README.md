# Partition all Fivetran tables automaticly for BigQuery

### About:

Changing your Fivetran tables to partitioned tables using the _fivetran_synced fields will save you a lot of money, but it is boring and time consuming to setup if you have a lot of tables. 

This notebooks automates the process. All you have to do is to enter your project_id, your Fivetran datasets and press start. 

The script can also handle datasets and tables, that has not been created by Fivetran, which makes it safe to use.

For info about getting started with the Python BigQuery library read [here](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries)
