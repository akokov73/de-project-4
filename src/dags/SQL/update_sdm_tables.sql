-- Заполняем таблицу sdm.dm_courier_ledger

with t_table as (
select
	dad.courier_id as courier_id,
	dac."name" as courier_name ,
	extract (year from dad.delivery_ts ) as settlement_year , 
	extract (month from dad.delivery_ts ) as settlement_month,
	count(*) as orders_count,
	sum(dad.sum) as orders_total_sum ,
	avg(dad.rate) as rate_avg ,
	sum(dad.tip_sum) * 0.25 as order_processing_fee ,
	sum(dad.tip_sum) as courier_tips_sum
from
	dds.dm_api_deliveries dad
join 
dds.dm_api_couriers dac on
	dad.courier_id = dac."_id"
group by
	dad.courier_id ,
	dac."name" ,
	extract (year from dad.delivery_ts ) ,
	extract (month from dad.delivery_ts )   
),
snd_table as (
select
	tt.courier_id,
	tt.courier_name ,
	tt.settlement_year ,
	tt.settlement_month,
	tt.orders_count,
	tt.orders_total_sum ,
	tt.rate_avg ,
	tt.order_processing_fee ,
	tt.courier_tips_sum,
	case
		when (tt.rate_avg < 4 )
			and (tt.orders_total_sum * 0.05 > 100) then tt.orders_total_sum * 0.05
			when (tt.rate_avg < 4 )
				and (tt.orders_total_sum * 0.05 <= 100) then 100
				when (tt.rate_avg >= 4 )
					and (tt.rate_avg < 4.7 )
						and (tt.orders_total_sum * 0.07 > 150) then tt.orders_total_sum * 0.07
						when (tt.rate_avg >= 4 )
							and (tt.rate_avg < 4.7 )
								and (tt.orders_total_sum * 0.07 <= 150) then 150
								when (tt.rate_avg >= 4.7 )
									and (tt.rate_avg < 4.9 )
										and (tt.orders_total_sum * 0.08 > 175) then tt.orders_total_sum * 0.08
										when (tt.rate_avg >= 4.7 )
											and (tt.rate_avg < 4.9 )
												and (tt.orders_total_sum * 0.08 <= 175) then 175
												when (tt.rate_avg >= 4.9 )
													and (tt.orders_total_sum * 0.1 > 200) then tt.orders_total_sum * 0.1
													when (tt.rate_avg >= 4.9 )
														and (tt.orders_total_sum * 0.1 <= 200) then 200
														else '00'
													end as courier_order_sum
												from
													t_table tt
 )
insert
	into
	sdm.dm_courier_ledger
(courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_reward_sum)
select 
	stt.courier_id,
	stt.courier_name ,
	stt.settlement_year ,
	stt.settlement_month,
	stt.orders_count,
	stt.orders_total_sum ,
	stt.rate_avg ,
	stt.order_processing_fee ,
	stt.courier_tips_sum,
	stt.courier_order_sum,
	stt.courier_order_sum + stt.courier_tips_sum * 0.95 as courier_reward_sum
from
	snd_table stt 
;
