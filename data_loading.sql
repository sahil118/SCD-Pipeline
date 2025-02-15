// Creating external stage (create your own bucket)
CREATE OR REPLACE STAGE SCD_DEMO.SCD2.customer_ext_stage
    url='s3://snowflake-data-handson/stream_data/'
    credentials=(aws_key_id='' aws_secret_key='');

   LIST  @customer_ext_stage;

   SHOW STAGES;

   CREATE OR REPLACE FILE FORMAT SCD_DEMO.SCD2.csv_file
   type =csv
   skip_header = 1
   field_delimiter = ",";


   CREATE OR REPLACE PIPE SCD_DEMO.SCD2.customer_s3_pipe
   auto_ingest = TRUE
   as
   copy into SCD_DEMO.SCD2.CUSTOMER_RAW
   from @customer_ext_stage
   file_format = SCD_DEMO.SCD2.csv_file;