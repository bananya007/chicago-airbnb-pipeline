create database if not exists airbnb;
create schema if not exists airbnb.raw;
create schema if not exists airbnb.audit;

create warehouse if not exists transform_wh
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = TRUE
    initially_suspended = TRUE;
