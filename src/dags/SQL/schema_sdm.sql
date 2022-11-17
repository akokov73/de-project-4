-- create schema if not exists sdm;


drop table if exists sdm.dm_courier_ledger;

create table if not exists  sdm.dm_courier_ledger 
    (
    id  serial primary key,
    courier_id varchar not null,
    courier_name  varchar not null,
    settlement_year  smallint not null check (settlement_year >= 2022 and settlement_year  < 2500 ),
    settlement_month smallint not null check (settlement_month  >= 1 and settlement_month  <=12 ),
    orders_count integer not null check (orders_count >= 0 ),
    orders_total_sum numeric (14,2) not null default 0  check (orders_total_sum >= 0 ),
    rate_avg numeric (14,5) not null default 0  check (rate_avg >= 0 ),
    order_processing_fee numeric (14,2) not null default 0  check (order_processing_fee >= 0 ),
    courier_order_sum numeric (14,2) not null default 0  check (courier_order_sum >= 0 ),
    courier_tips_sum numeric (14,2) not null default 0  check (courier_tips_sum >= 0 ),
    courier_reward_sum numeric (14,2) not null default 0  check (courier_reward_sum >= 0 ),
    unique (courier_id , settlement_year , settlement_month) 
    );
