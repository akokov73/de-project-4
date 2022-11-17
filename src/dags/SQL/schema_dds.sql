create schema if not exists dds;

-- drop table if exists dds.dm_api_couriers  ;

CREATE TABLE if not EXISTS dds.dm_api_couriers (
    id serial primary key, 
    _id varchar not null, 
    name varchar not null
);

CREATE TABLE if not EXISTS dds.dm_api_restaurants (
    id serial primary key, 
    restaurant_id VARCHAR not null, 
    name text not null  
);

CREATE TABLE if not EXISTS dds.dm_api_orders (
    id serial primary key, 
    order_id varchar not null, 
    order_ts TIMESTAMP not null  
);

-- drop table if exists dds.dm_api_deliveries ;

CREATE TABLE if not EXISTS dds.dm_api_deliveries (
    id serial primary key, 
    order_id  varchar not null, 
    order_ts  TIMESTAMP not null ,
    delivery_id  varchar not null, 
    courier_id varchar not null,
    address VARCHAR not null,
    delivery_ts TIMESTAMP not null ,
    sum NUMERIC(14,2)  not null default 0 check (sum>=0),
    tip_sum  NUMERIC(14,2)  not null default 0 check (tip_sum>=0),
    rate integer not NULL
);

create table if not exists dds.dm_couriers (
    id serial primary key, 
    courier_id varchar not null, 
    courier_name  varchar not null, 
    active_from timestamp not null, 
    active_to timestamp not null
    );



CREATE TABLE if not exists  dds.dm_orders ( 
    id integer NOT NULL,
    courier_id integer NOT NULL,
    user_id integer NOT NULL,
    restaurant_id integer NOT NULL,
    timestamp_id integer NOT NULL,
    order_key varchar NOT NULL,
    order_status varchar NOT NULL
    );

 

create table if not exists dds.fct_courier_tips (
    id serial primary key, 
    order_id integer not null, 
    tip_sum numeric(14,5) not null default 0 check (tip_sum>=0)
    );


create table if not exists dds.fct_order_rates (
    id serial primary key, 
    order_id integer not null, 
    order_sum numeric(14,5) not null default 0 check (order_sum >=0)
    );

-- drop table if  exists dds.fct_api_sales;

create table if not exists dds.fct_api_sales
(
id serial not null primary key,
order_id integer not null,
delivery_id  varchar not null,
order_sum numeric (12,2) not null,
tip_sum numeric (12,2) not null,
rate smallint not null
);
