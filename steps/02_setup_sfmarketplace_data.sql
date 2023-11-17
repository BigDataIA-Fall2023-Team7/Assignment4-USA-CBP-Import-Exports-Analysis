use database DB_DAMG7245_TEAM7;
create or replace schema usa_cbp;
use schema usa_cbp;

create or replace view imports as select * from us_imports_bill_of_lading.public.us_imports_bol_primary_details_sample;
create or replace view party as select * from us_imports_bill_of_lading.public.us_imports_bol_party_details_sample;
create or replace view container as select * from us_imports_bill_of_lading.public.us_imports_bol_container_details_sample;
create or replace view cargo as select * from us_imports_bill_of_lading.public.us_imports_bol_cargo_details_sample;
create or replace view hazmat as select * from us_imports_bill_of_lading.public.us_imports_bol_hazmat_description_sample;
create or replace view events as select * from eventwatch_ai.public.eventwatch_all_sample_v2;
create or replace view suppliers as select * from amazon_and_ecommerce_websites_product_views_and_purchases.datafeeds.product_views_and_purchases;