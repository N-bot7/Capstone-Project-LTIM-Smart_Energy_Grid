//Creating warehouese named capstone for the project

CREATE OR REPLACE WAREHOUSE capstone
WITH WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 3
SCALING_POLICY = 'ECONOMY';

//Creating database for the project
create or replace database all_files;

//creating schema for the project
create or replace schema capstone_schema;

//creating table for renewable_generation.csv data
CREATE OR REPLACE TABLE RENEWABLE_GENERATION(
RegionID int,
GenDate date,
Source varchar(255),	
GeneratedMWh float,	
InstalledCapacityMW float,	
CapacityFactorPct float);

//creating table for region_hierarchy data
CREATE OR REPLACE TABLE REGION_HIERARCHY(
RegionID int,
Country varchar(255),
State varchar(255),
Zone varchar(255),
Branch varchar(255));

//creating table for customer_profile data

create or replace table customer_profile(
CustomerID int,
CustomerName varchar(30),
Email varchar(30),
Phone varchar(20),
ConnectionType varchar(30) ,
TariffCategory varchar(30),
ContractKVA float,
RegionID int 
);

//creating csv file format
create or replace file format my_csv_format
type = 'csv'
skip_header = 1,
field_delimiter = ',';

list @~;

//copying data from user stage
copy into renewable_generation from @~/Renewable_Generation.csv
file_format = 'my_csv_format';

select * from renewable_generation;

copy into region_hierarchy from @~/Region_Hierarchy.csv
file_format = 'my_csv_format';

select * from region_hierarchy;

copy into customer_profile from @~/Customer_Profile.csv
file_format = 'my_csv_format';

select * from customer_profile;

CREATE OR REPLACE TABLE stg_substation_load (
    RegionID INT,
    Timestamp TIMESTAMP,
    LoadMW DECIMAL(10, 2),
    UtilizationPct DECIMAL(5, 2),
    TransformerCount INT,
    LossesPct DECIMAL(5, 2)
);

select * from stg_substation_load;

-- task 4 - snowpipe
create or replace storage integration capstone_s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::049903362104:role/ayushyadav-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://ayushyadav-bkt/Capstone/')
   COMMENT = 'This an optional comment' ;

desc integration capstone_s3_int;

CREATE OR REPLACE stage capstone_s3_stage
URL = 's3://ayushyadav-bkt/Capstone/'
STORAGE_INTEGRATION = capstone_s3_int
FILE_FORMAT = my_csv_format;

list @capstone_s3_stage;

CREATE or replace TABLE SmartMeter_Readings (
    CustomerID INT,
    RegionID INT,
    Timestamp string,
    kWh DECIMAL(10, 3),
    AvgKW DECIMAL(5, 2)
);

CREATE OR REPLACE pipe capstone_pipe
auto_ingest = TRUE
AS
COPY INTO SMARTMETER_READINGS
FROM @capstone_s3_stage
ON_ERROR = 'Continue';

show pipes;

select * from smartmeter_readings;

list @capstone_s3_stage;

-- weather_data table
//auto ingested
select * from weather_data;

show tables;

ALTER TABLE weather_Data
RENAME COLUMN temperature TO temperaturec;

select * from renewable_generation;

