---Creating the databse for the project ---
drop database if exists hotel_db;
create database hotel_db;
--using the database and the public schema ---
use database hotel_db;
use database public;

---Step1 Creating the file foramt object as we are loading  the csv file with some paramters ----
create or replace file format file_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
--Checking evryting got right ----
describe file format file_format;

--step2 Creating an  internal stage  for the data to be loded by creating the stage obj in the public schema----
create or replace stage data_stage
file_format= (format_name = file_format)

--checking the stage propertires and data can be acessed or not   ---
describe stage  data_stage;
list @data_stage;

---Creating an broze layer table  where actual raw data are stored from the stage by pre defining schema ---
CREATE TABLE bronze_layer (
    booking_id STRING,
    hotel_id STRING,
    hotel_city STRING,
    customer_id STRING,
    customer_name STRING,
    customer_email STRING,
    check_in_date STRING,
    check_out_date STRING,
    room_type STRING,
    num_guests STRING,
    total_amount STRING,
    currency STRING,
    booking_status STRING
);
----Insetring teh data from the stage using copyt inot command bulk load ----
copy into bronze_layer 
from @data_stage
ON_ERROR = 'CONTINUE';

--checking the bronze layer table and eroros if occu --
select * from bronze_layer limit 50;
SELECT * FROM INFORMATION_SCHEMA.LOAD_HISTORY WHERE TABLE_NAME = 'BRONZE_LAYER';

--creating the silver table ---
-- Here we updated the datatypes of the table  as per the correctness---

CREATE TABLE silver_layer (
    booking_id VARCHAR,
    hotel_id VARCHAR,
    hotel_city VARCHAR,
    customer_id VARCHAR,
    customer_name VARCHAR,
    customer_email VARCHAR,
    check_in_date DATE,
    check_out_date DATE,
    room_type VARCHAR,
    num_guests INTEGER,
    total_amount FLOAT,
    currency VARCHAR,
    booking_status VARCHAR
);
describe table silver_layer;

-- Checking for errors--

--Cheking the invalid customer emails ---
SELECT customer_email
FROM bronze_layer
WHERE customer_email NOT LIKE '%@%'
   OR customer_email IS NULL;
--there count
select count(customer_email) as total_invalid_cust_emails from bronze_layer
where customer_email like '%invalid%';

--Total amount shld not be -ve so filter those customer_email asn total_amount---
select customer_email,total_amount from bronze_layer
where try_to_number(total_amount)<0;

--Check out date is not less than the check in date---

select check_out_date,check_in_date from bronze_layer
where try_to_date(check_out_date)<try_to_date(check_in_date)

--checking  erros the booking_status  ---
select distinct booking_status from bronze_layer ;
--We found the confirmeeed is an duplaicaetd with onfireme we should repalce the things ---

---- by removing the above check and repalceing with crct ones inserting the cleaned values to  the silver table ---
insert into silver_layer
select 
booking_id ,
hotel_id,
initcap(trim(hotel_city)) as hotel_city,
customer_id,
initcap(trim(customer_name)) as customer_name,
case when customer_email like '%@%.%' then lower(trim(customer_email))
else null end as customer_email,
try_to_date(NULLIF(check_in_date, '')) AS check_in_date,
try_to_date(NULLIF(check_out_date, '')) AS check_out_date,
room_type,
num_guests,
ABS(TRY_TO_NUMBER(total_amount)) AS total_amount, currency,
case when  booking_status like '%Confirmeeed%' then 'Confirmed' else booking_status
 END AS booking_status
  FROM bronze_layer
    WHERE
        TRY_TO_DATE(check_in_date) IS NOT NULL
        AND TRY_TO_DATE(check_out_date) IS NOT NULL
        AND TRY_TO_DATE(check_out_date) >= TRY_TO_DATE(check_in_date);
        
select distinct booking_status from silver_layer
limit 30;
---Clering the objectives from the silver table ---
CREATE TABLE GOLD_AGG_DAILY_BOOKING 
as 
select month(check_in_date) as month ,count(*) as total_bookings ,sum(total_amount) as total_cost
from silver_layer
group by month
order by month asc

-----top revenue genrating cities--
create table gold_agg_hotel_city as
select  hotel_city ,sum(total_amount) as total_revenue from silver_layer
group by hotel_city order by total_revenue desc

--- total booking by status and type 
select booking_status ,room_type ,count(*) as total_bookings from silver_layer
group by booking_status,room_type 

----creating the gold table 

CREATE TABLE gold_layer AS
SELECT
    booking_id,
    hotel_id,
    hotel_city,
    customer_id,
    customer_name,
    customer_email,
    check_in_date,
    check_out_date,
    room_type,
    num_guests,
    total_amount,
    currency,
    booking_status
FROM silver_layer;

select * from gold_layer limit 50;
