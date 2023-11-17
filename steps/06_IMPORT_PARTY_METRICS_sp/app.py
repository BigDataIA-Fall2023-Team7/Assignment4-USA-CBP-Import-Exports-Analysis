import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


def table_exists(session, schema='', name=''):
    exists = session.sql("SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}') AS TABLE_EXISTS".format(schema, name)).collect()[0]['TABLE_EXISTS']
    return exists

def create_imports_party_metrics_table(session):
    SHARED_COLUMNS= [
                        T.StructField('actual_date_of_arrival', T.DateType()),
                        T.StructField('master_bill_of_lading_number', T.StringType()),
                        T.StructField('house_bill_of_lading_number', T.StringType()),
                        T.StructField('bill_of_lading_type', T.StringType()),
                        T.StructField('estimated_arrival_date', T.DateType()),
                        T.StructField('estimated_shipment_value', T.FloatType()),
                        T.StructField('place_of_receipt', T.StringType()),
                        T.StructField('product_keywords', T.StringType()),
                        T.StructField('foreign_port_of_destination', T.StringType()),
                        T.StructField('foreign_port_of_lading', T.StringType()),
                        T.StructField('port_of_destination', T.StringType()),
                        T.StructField('port_of_unlading', T.StringType()),
                        T.StructField('voyage_number', T.StringType()),
                        T.StructField('vessel_name', T.StringType()),
                        T.StructField('vessel_country', T.StringType()),
                        T.StructField('manifest_quantity', T.FloatType()),
                        T.StructField('manifest_quantity_unit', T.StringType()),
                        T.StructField('teu', T.FloatType()),
                        T.StructField('total_weight', T.FloatType()),
                        T.StructField('total_weight_unit', T.StringType()),

                        T.StructField('shipper_name', T.StringType()),
                        T.StructField('shipper_standardised_name', T.StringType()),
                        T.StructField('shipper_address', T.StringType()),
                        T.StructField('shipper_city', T.StringType()),
                        T.StructField('shipper_state_province', T.StringType()),
                        T.StructField('shipper_country', T.StringType()),
                        T.StructField('shipper_zip_code', T.StringType()),
                        T.StructField('consignee_name', T.StringType()),
                        T.StructField('consignee_standardised_name', T.StringType()),
                        T.StructField('consignee_address', T.StringType()),
                        T.StructField('consignee_city', T.StringType()),
                        T.StructField('consignee_state', T.StringType()),
                        T.StructField('consignee_zip_code', T.StringType()),

                        # Metrics Columns in ANALYTICS.IMPORTS_PARTY_METRICS
                        T.StructField('M_price_per_manifest_quantity_unit', T.FloatType()),
                        T.StructField('M_total_weight_in_pounds', T.FloatType()),
                    ]
    IMPORTS_PARTY_METRICS_COLUMNS = [*SHARED_COLUMNS, T.StructField("META_UPDATED_AT", T.TimestampType())]
    IMPORTS_PARTY_METRICS_SCHEMA = T.StructType(IMPORTS_PARTY_METRICS_COLUMNS)

    dcm = session.create_dataframe([[None]*len(IMPORTS_PARTY_METRICS_SCHEMA.names)], schema=IMPORTS_PARTY_METRICS_SCHEMA) \
                        .na.drop() \
                        .write.mode('overwrite').save_as_table('ANALYTICS.IMPORTS_PARTY_METRICS')
    dcm = session.table('ANALYTICS.IMPORTS_PARTY_METRICS')


def merge_imports_party_metrics(session):
    try:
        _ = session.sql('ALTER WAREHOUSE WH_DAMG7245_TEAM7 SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE').collect()

        print("{} records in view".format(session.table('HARMONIZED.IMPORTS_PARTY_REL_V').count()))
        
        imports_party_metrics = session.table('HARMONIZED.IMPORTS_PARTY_REL_V') \
                                .with_column("M_total_weight_in_pounds",  F.call_udf("ANALYTICS.KG_TO_POUNDS_UDF", F.col("TOTAL_WEIGHT"))) \
                                .with_column("M_price_per_manifest_quantity_unit",  F.call_udf("ANALYTICS.PRICE_PER_MANIFEST_UDF", (F.col("ESTIMATED_SHIPMENT_VALUE"), (F.col("MANIFEST_QUANTITY"))))) \

        imports_party_metrics[['M_price_per_manifest_quantity_unit', 'M_total_weight_in_pounds']].limit(5).show()

        cols_to_update = {c: imports_party_metrics[c] for c in imports_party_metrics.schema.names}
        metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
        updates = {**cols_to_update, **metadata_col_to_update}
        for i in updates.items():
            print(i)

        ipm = session.table('ANALYTICS.IMPORTS_PARTY_METRICS')

        print('SCHEMA for ANALYTICS.IMPORTS_PARTY_METRICS\n')
        for i in ipm.schema.names:
            print(i, ipm[i])

        print('-----------')    
        ipm.merge(imports_party_metrics, \
                (ipm['MASTER_BILL_OF_LADING_NUMBER'] == imports_party_metrics['MASTER_BILL_OF_LADING_NUMBER']) \
                & (ipm['HOUSE_BILL_OF_LADING_NUMBER'] == imports_party_metrics['HOUSE_BILL_OF_LADING_NUMBER']),\
                [F.when_matched().update(updates), F.when_not_matched().insert(updates)])
    except Exception as e:
        print(e)
        raise Exception('ERROR: While Merging Data from HARMONIZED.IMPORTS_PARTY_REL_V to ANALYTICS.IMPORTS_PARTY_METRICS')
    finally:
        _ = session.sql('ALTER WAREHOUSE WH_DAMG7245_TEAM7 SET WAREHOUSE_SIZE = XSMALL').collect()

def main(session: Session) -> str:
    # Create the IMPORT_PARTY_METRICS table if it doesn't exist
    if not table_exists(session, schema='ANALYTICS', name='IMPORT_PARTY_METRICS'):
        create_imports_party_metrics_table(session)
        print("SUCCESS: ANALYTICS.IMPORT_PARTY_METRICS Created")
    
    merge_imports_party_metrics(session)
    print("SUCCESS: ANALYTICS.IMPORT_PARTY_METRICS Data Inserted")
#    session.table('ANALYTICS.DAILY_CITY_METRICS').limit(5).show()

    return f"Successfully processed IMPORT_PARTY_METRICS"


# For local debugging
# Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_parent_dir = os.path.dirname(os.path.dirname(current_dir))
    sys.path.append(parent_parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    if len(sys.argv) > 1:
        print(main(session, *sys.argv[1:]))  # type: ignore
    else:
        print(main(session))  # type: ignore

    session.close()
