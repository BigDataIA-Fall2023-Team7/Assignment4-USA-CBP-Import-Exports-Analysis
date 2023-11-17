from snowflake.snowpark import Session
#import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F


SQL_IMPORTS_PARTY_REL_V = """

create or replace view IMPORTS_PARTY_REL_V as
with 
ranked_imports as
(
    SELECT
      USA_CBP.imports.run_date as i_run_date,
      USA_CBP.imports.actual_date_of_arrival,
      USA_CBP.imports.master_bill_of_lading_number,
      USA_CBP.imports.house_bill_of_lading_number,
      USA_CBP.imports.bill_of_lading_type,
      NULLIF(TO_DATE(USA_CBP.imports.estimated_arrival_date), NULL) as estimated_arrival_date,
      NULLIF(CAST(USA_CBP.imports.estimated_shipment_value AS DOUBLE), NULL) as estimated_shipment_value ,
      USA_CBP.imports.place_of_receipt,
      USA_CBP.imports.product_keywords,
      USA_CBP.imports.foreign_port_of_destination,
      USA_CBP.imports.foreign_port_of_lading,
      USA_CBP.imports.port_of_destination,
      USA_CBP.imports.port_of_unlading,
      USA_CBP.imports.voyage_number,
      USA_CBP.imports.vessel_name,
      USA_CBP.imports.vessel_country,
      NULLIF(CAST(USA_CBP.imports.manifest_quantity AS DOUBLE), NULL) as manifest_quantity ,
      USA_CBP.imports.manifest_quantity_unit,
      NULLIF(CAST(USA_CBP.imports.teu AS DOUBLE), NULL) as teu,
      NULLIF(CAST(USA_CBP.imports.total_weight AS DOUBLE), NULL) as total_weight,
      USA_CBP.imports.total_weight_unit,
      RANK() OVER (PARTITION BY USA_CBP.imports.master_bill_of_lading_number, USA_CBP.imports.house_bill_of_lading_number ORDER BY USA_CBP.imports.run_date desc) AS i_ranking
    FROM
      USA_CBP.imports
),
ranked_party as
(
    SELECT
      USA_CBP.party.run_date as p_run_date,
      USA_CBP.party.master_bill_of_lading_number as p_master_bill_of_lading_number,
      USA_CBP.party.house_bill_of_lading_number as p_house_bill_of_lading_number,
      USA_CBP.party.shipper_name,
      USA_CBP.party.shipper_standardised_name,
      USA_CBP.party.shipper_address,
      USA_CBP.party.shipper_city,
      USA_CBP.party.shipper_state_province,
      USA_CBP.party.shipper_country,
      USA_CBP.party.shipper_zip_code,
      USA_CBP.party.consignee_name,
      USA_CBP.party.consignee_standardised_name,
      USA_CBP.party.consignee_address,
      USA_CBP.party.consignee_city,
      USA_CBP.party.consignee_state,
      USA_CBP.party.consignee_zip_code,
      RANK() OVER (PARTITION BY USA_CBP.party.master_bill_of_lading_number, USA_CBP.party.house_bill_of_lading_number ORDER BY USA_CBP.party.run_date desc) AS p_ranking
    FROM
      USA_CBP.party
),
latest_imports as
(
    select * from ranked_imports where i_ranking=1
),
latest_party as
(
    select * from ranked_party where p_ranking=1
),
matching_imports_party as(
    select * from latest_imports inner join latest_party
    on latest_imports.master_bill_of_lading_number = latest_party.p_master_bill_of_lading_number and
    latest_imports.house_bill_of_lading_number = latest_party.p_house_bill_of_lading_number  
),
mwithnoh as (
    select master_bill_of_lading_number as m from latest_imports group by master_bill_of_lading_number having count(house_bill_of_lading_number)=0
),
nonmatching_imports_party as(
    select * from latest_imports inner join latest_party
    on latest_imports.master_bill_of_lading_number = latest_party.p_master_bill_of_lading_number
    where latest_imports.master_bill_of_lading_number in (select m from mwithnoh)
)
select * exclude(i_run_date, i_ranking, p_run_date, p_master_bill_of_lading_number, p_house_bill_of_lading_number, p_ranking) from matching_imports_party
union
select * exclude(i_run_date, i_ranking, p_run_date, p_master_bill_of_lading_number, p_house_bill_of_lading_number, p_ranking) from nonmatching_imports_party;


"""

## Creating HARMONIZED.IMPORTS_PARTY_REL_V
def create_harmonized_imports_party_view(session):
    session.use_schema('HARMONIZED')
    _ = session.sql(SQL_IMPORTS_PARTY_REL_V).collect()

## CANNOT CREATE STREAM ON HARMONIZED.IMPORTS_PARTY_REL_V because it uses union operation
# def create_harmonized_imports_party_view_stream(session):
#     session.use_schema('HARMONIZED')
#     _ = session.sql('CREATE OR REPLACE STREAM IMPORTS_PARTY_REL_V_STREAM \
#                         ON VIEW IMPORTS_PARTY_REL_V \
#                         SHOW_INITIAL_ROWS = TRUE').collect()

# For local debugging
if __name__ == "__main__":
    # Add the utils package to our path and import the snowpark_utils function
    import os, sys
    current_dir = os.getcwd()
    parent_dir = os.path.dirname(current_dir)
    sys.path.append(parent_dir)

    from utils import snowpark_utils
    session = snowpark_utils.get_snowpark_session()

    create_harmonized_imports_party_view(session)
    print("Success: HARMONIZED.IMPORTS_PARTY_REL_V Created")
    # create_harmonized_imports_party_view_stream(session)
    # test_pos_view(session)

    session.close()
