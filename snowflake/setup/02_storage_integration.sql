CREATE STORAGE INTEGRATION s3_airbnb_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-airbnb-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://chicago-airbnb-raw-ananya/raw/');

DESC INTEGRATION s3_airbnb_int;