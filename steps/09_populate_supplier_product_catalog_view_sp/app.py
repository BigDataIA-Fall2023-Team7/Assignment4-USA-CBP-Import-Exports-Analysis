from snowflake.snowpark import Session
import pandas as pd
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import split

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pyarrow.pandas_compat")

def delete_table_if_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    if exists: 
        _ = session.sql(f'TRUNCATE TABLE DB_DAMG7245_TEAM7.{schema}.{name}').collect()
        print(f'TRUNCATE TABLE DB_DAMG7245_TEAM7.{schema}.{name}')

def create_supplier_product_catalog_view(session):
    # session.use_schema('HARMONIZED')
    suppliers = session.table("USA_CBP.SUPPLIERS").select(F.col("Product Headline").alias("PRODUCT_HEADLINE"),\
                                                    F.col("Product Description").alias("PRODUCT_DESCRIPTION"),\
                                                    F.col("Product URL").alias("PRODUCT_URL"),\
                                                    F.col("Company ID").alias("COMPANY_ID"),\
                                                    F.col("Company Name").alias("COMPANY_NAME"),\
                                                    F.col("Company Legal Name").alias("COMPANY_LEGAL_NAME"),\
                                                    F.col("Company Main Country").alias("COMPANY_MAIN_COUNTRY"),\
                                                    F.col("Company Main Region").alias("COMPANY_MAIN_REGION"),\
                                                    F.col("Company Main City").alias("COMPANY_MAIN_CITY"),\
                                                    F.col("Company Description").alias("COMPANY_DESCRIPTION"),\
                                                    F.col("Company Website").alias("COMPANY_WEBSITE"),\
                                                    F.col("Company Keywords").alias("COMPANY_KEYWORDS"),\
                                                    F.col("Company Emails").alias("COMPANY_EMAILS"),\
                                                    F.col("Company Phones").alias("COMPANY_PHONES"),\
                                                    F.col("Company Facebook").alias("COMPANY_FACEBOOK"),\
                                                    F.col("Company Linkedin").alias("COMPANY_LINKEDIN"),\
                                                    F.col("Company Twitter").alias("COMPANY_TWITTER"),\
                                                    F.col("Company Instagram").alias("COMPANY_INSTAGRAM"),\
                                                    F.col("Company Number of Employees").alias("COMPANY_NUMBER_OF_EMPLOYEES"))
    print(suppliers.show)
    
    df = suppliers.toPandas(block = True)
    df['PRODUCT_KEYWORDS']=  df['PRODUCT_HEADLINE'] + " " +df['COMPANY_KEYWORDS']
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.replace(r'\s*([^a-zA-Z0-9\s])\s*', '|', regex=True)
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.replace(" ","|")
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.split('|')
    df = df.explode('PRODUCT_KEYWORDS', ignore_index=True)
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.replace(r'\s+', ' ', regex=True).str.lower()
    df = df.dropna(subset=['PRODUCT_KEYWORDS']).reset_index()
    print ("Processing...") 

    imports_df = session.write_pandas(df,table_name = 'SUPPLIER_PRODUCT_CATALOG', schema = 'HARMONIZED', auto_create_table=True) #creating temp table
    
    imports_df.create_or_replace_view("v_SUPPLIER_PRODUCT_CATALOG")

def main(session: Session) -> str:
    # Create the DAILY_CITY_METRICS table if it doesn't exist
    session.use_database('DB_DAMG7245_TEAM7')
    session.use_schema('ANALYTICS')

    delete_table_if_exists(session, schema='HARMONIZED', name='SUPPLIER_PRODUCT_CATALOG') # creating temp table
    create_supplier_product_catalog_view(session)

    return f"Successfully processed v_SUPPLIER_PRODUCT_CATALOG"                     

if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()

    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    
    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))

    session.close()
