# Assignment4-USA-CBP-Import-Exports-Analysis
CICD enabled Snowflake based data pipelines for USA CBP imports and exports data. End user application that shows analytical dashboards on this supply chain dataset and AI enabled SQL query engine to do adhoc analysis


# Steps to run this prroject locally:
Download SnowSQL CLI from this website
https://developers.snowflake.com/snowsql/

Pre-requisites:
1. Anaconda Installation
2. VS Code
3. Snowflake VS Code extension

Create a conda environment by running
```
conda env create -f environment.yml
```

Activate the conda environment
```
conda activate snowflake-demo
```

Point your VS Code workspace to use this newly created conda environment python interpreter

Update the config file at `~/.snowsql/config` by adding the following snippet
[connections.example] will be present already edit it, but add [connections.dev]

```
[connections.example]
accountname = <Your SF Account Name>
username = <Your SF Username>
password = <<Your SF Password>

[connections.dev]
accountname = <Your SF Account Name>
username = <Your SF Username>
password = <<Your SF Password>
rolename = ROLE_DAMG7245_TEAM7
warehousename = WH_DAMG7245_TEAM7
dbname = DB_DAMG7245_TEAM7
```

To test connectivity to snowflake using snowsql cli use the command in your terminal:
```
snowsql -c example
```

 If you have setup MFA for Snowflake, please complete the authorisation to successfully login to Snowflake using CLI

## Step 1
Run the `01_setup_snowflake.sql` to create warehouse, db and roles configured in `~/.snowsql/config` -> [connections.dev]

To test connectivity to newly created artifacts using snowsql cli use the command in your terminal:
```
snowsql -c dev
```

## Step 2
Use the below website to 'Get' the free dataset into your snowflake account
Note: While configuring the database into your snowflake keep the default settings like database name, schema name etc.

*Dataset 1: Trademo, US Imports Bill of Lading*

https://app.snowflake.com/marketplace/listing/GZT1ZVEJFJ/trademo-us-imports-bill-of-lading?search=imports

*Dataset 2: Resilinc, EventWatch AI*

https://app.snowflake.com/marketplace/listing/GZSTZO0V7VR/resilinc-eventwatch-ai?search=supply%20chain&pricing=free

*Dataset 3: Similarweb Ltd, Amazon and E-commerce Websites Product Views and Purchases*

https://app.snowflake.com/marketplace/listing/GZT1ZA3NK6/similarweb-ltd-amazon-and-e-commerce-websites-product-views-and-purchases

Run the script `02_setup_sfmarketplace_data.sql` to set the alias for these snowflake marketplace datasets

## Step 3




