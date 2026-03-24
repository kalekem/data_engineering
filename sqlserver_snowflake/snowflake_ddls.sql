-- create snowflake to S3 storage integration
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<your_aws_role_arn>'
  STORAGE_ALLOWED_LOCATIONS = ('<your_s3_bucket>');

  describe integration s3_integration;

  CREATE OR REPLACE STAGE my_s3_stage
  STORAGE_INTEGRATION = s3_integration
  URL = '<s3_bucket>'
  FILE_FORMAT = (TYPE = 'PARQUET'); 

  LIST @my_s3_stage;

  CREATE OR REPLACE FILE FORMAT my_parquet_format
  TYPE = 'PARQUET'
  SNAPPY_COMPRESSION = TRUE
  USE_LOGICAL_TYPE = TRUE; --respect Parquet's logical type 

--Create a table using INFER_SCHEMA() snowflake function
CREATE OR REPLACE TABLE Customer
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(
      INFER_SCHEMA(
        LOCATION=>'<location_of_your_table>',
        FILE_FORMAT=>'my_parquet_format'
      )
    )
  );

--load to snowflake
COPY INTO Customer
FROM <@your_stage/path_to_file>
FILE_FORMAT = 'my_parquet_format'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE