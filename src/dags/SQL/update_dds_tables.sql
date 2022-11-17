INSERT INTO dds.dm_api_orders
(order_id, order_ts)
select dd.order_id , dd.order_ts 
from dds.dm_api_deliveries dd
group by 1,2;

