SELECT t2.order_pay_date
        ,COUNT(t2.order_id) AS pay_order_num
        ,SUM(CASE WHEN t2.is_aim_order = 'yes' THEN 1 ELSE 0 END) AS aim_order_num
        ,SUM(t2.org_goods_num) AS pay_goods_num
        ,SUM(t2.demand_org_num) AS demand_goods_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN 1
                  ELSE 0
             END) AS peihuo_order_num
        ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 0 THEN 1 ELSE 0 END) AS t0_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 0
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t0_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 1 THEN 1 ELSE 0 END) AS t1_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 1
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t1_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 2 THEN 1 ELSE 0 END) AS t2_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 2
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t2_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 3 THEN 1 ELSE 0 END) AS t3_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 3
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t3_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 4 THEN 1 ELSE 0 END) AS t4_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 4
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t4_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 5 THEN 1 ELSE 0 END) AS t5_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 5
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t5_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 6 THEN 1 ELSE 0 END) AS t6_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) = 6
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t6_peihuo_duration
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) >= 7 THEN 1 ELSE 0 END) AS t7_peihuo_order_num
        ,SUM(CASE WHEN DATEDIFF(t2.outing_stock_time, t2.order_pay_time) >= 7
                  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
                  ELSE 0
             END) AS t7_peihuo_duration
        ,SUM(CASE WHEN is_shiped = 1 THEN 1 ELSE 0 END) AS ship_order_num
        ,SUM(t2.ship_goods_num) AS ship_goods_num
FROM zybiro.neo_pur_lock_orders AS t2
WHERE t2.order_pay_date < '2018-01-09'
GROUP BY t2.order_pay_date
ORDER BY t2.order_pay_date
;
