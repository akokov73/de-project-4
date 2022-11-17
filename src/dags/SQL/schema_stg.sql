create schema if not exists stg;

-- DROP TABLE if EXISTS  stg.api_restaurants ;

create table if not exists  stg.api_restaurants (
    id serial primary key, 
    content text unique, 
    load_ts timestamp);

-- DROP TABLE if EXISTS  stg.api_couriers ;

create table if not exists  stg.api_couriers (
    id serial primary key, 
    content text unique, 
    load_ts timestamp);

-- DROP TABLE if EXISTS  stg.api_deliveries ;

create table if not exists  stg.api_deliveries (
    id serial primary key, 
    content text unique, 
    load_ts timestamp
    );

-- DROP TABLE if EXISTS  stg.api_orders ;

create table if not exists  stg.api_orders (
    id serial primary key, 
    content text unique, 
    load_ts timestamp
    );

-- DROP TABLE if EXISTS  stg.api_users ;


create table if not exists  stg.api_users (
    id serial primary key, 
    content text unique, 
    load_ts timestamp
    );

-- DROP TABLE if EXISTS  stg.deliverysystem_couriers ;

create table if not exists stg.deliverysystem_couriers (
    id serial primary key, 
    courier_id varchar not null unique, 
    courier_json varchar not null, 
    load_dt date not null
    );

-- DROP TABLE if EXISTS  stg.deliverysystem_deliveries ;

create table if not exists stg.deliverysystem_deliveries (
    id serial primary key, 
    order_id varchar not null unique, 
    order_ts timestamp not null, 
    delivery_id varchar not null, 
    courier_id varchar not null, 
    courier_json varchar not null, 
    address varchar not null, 
    delivery_ts timestamp not null, 
    tip_sum numeric(14,5) not null default 0 check (tip_sum>=0) , 
    rate integer not null default 0 check (rate >= 1 and rate <= 5)
    );

-- DROP TABLE if EXISTS  stg.bonussystem_ranks ;

CREATE TABLE if not exists stg.bonussystem_ranks (
	id serial NOT NULL PRIMARY KEY,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) NOT NULL DEFAULT 0,
	min_payment_threshold numeric(19, 5) NOT NULL DEFAULT 0,
	CONSTRAINT ranks_bonus_percent_check CHECK ((bonus_percent >= (0)::numeric)),
	CONSTRAINT ranks_bonus_percent_check1 CHECK ((bonus_percent >= (0)::numeric)) 
);

-- DROP TABLE if EXISTS  stg.bonussystem_users ;

CREATE TABLE if not exists stg.bonussystem_users (
	id serial NOT NULL PRIMARY KEY,
	order_user_id text NOT NULL
);


DROP TABLE if EXISTS  stg.bonussystem_events ;


CREATE TABLE if not exists stg.bonussystem_events (
	id serial NOT NULL PRIMARY KEY,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL 
);
 


 