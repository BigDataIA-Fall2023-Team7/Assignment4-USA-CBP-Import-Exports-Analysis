from snowflake.snowpark import Session
import pandas as pd
import snowflake.snowpark.functions as F
from snowflake.snowpark.functions import split

import warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="pyarrow.pandas_compat")

def delete_table_if_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    if exists: 
        _ = session.sql(f'TRUNCATE TABLE USA_CBP.{schema}.{name}').collect()
        print(f'TRUNCATE TABLE USA_CBP.{schema}.{name}')

def create_import_view(session):
    # session.use_schema('HARMONIZED')
    imports = session.table("USA_CBP.IMPORTS").select(F.col("RUN_DATE"),\
                                                    F.col("MASTER_BILL_OF_LADING_NUMBER"),\
                                                    F.col("HOUSE_BILL_OF_LADING_NUMBER"),\
                                                    F.col("PRODUCT_KEYWORDS"))
    df = imports.toPandas(block = True)
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.split(' ')
    df = df.explode('PRODUCT_KEYWORDS').reset_index()
    df['PRODUCT_KEYWORDS'] = df['PRODUCT_KEYWORDS'].str.replace(",","").str.lower()
    print ("Processing...")
    imports_df = session.write_pandas(df,table_name = 'IMPORTS_PRODUCT_CATALOG', schema = 'HARMONIZED', auto_create_table=True)
    print("Public Table created")
    imports_df.create_or_replace_view("v_IMPORTS_PRODUCT_CATALOG")
    
def create_pos_view_stream(session):
    # session.use_schema('HARMONIZED')
    _ = session.sql('CREATE OR REPLACE STREAM v_IMPORTS_PRODUCT_CATALOG_STREAM \
                        ON VIEW v_IMPORTS_PRODUCT_CATALOG \
                        SHOW_INITIAL_ROWS = TRUE').collect()

def main(session: Session) -> str:
    # Create the DAILY_CITY_METRICS table if it doesn't exist
    session.use_database('DB_DAMG7245_TEAM7')
    session.use_schema('ANALYTICS')

    delete_table_if_exists(session, schema='HARMONIZED', name='IMPORTS_PRODUCT_CATALOG') # creating temp table
    create_import_view(session)

    return f"Successfully processed v_IMPORTS_PRODUCT_CATALOG"                     

if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()

    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()
    print(session)
    
    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))

    session.close()
