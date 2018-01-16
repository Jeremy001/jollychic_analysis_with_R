SELECT p1.order_pay_date
        -- create_2_push:小时：0/2/4/6/8/10+
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE p1.org_num
             END) AS push_goods_num
        ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) * p1.org_num /3600
             END) AS push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 2
                  THEN p1.org_num
                  ELSE 0
             END) AS t0_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 2
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t0_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=2
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 4
                  THEN p1.org_num
                  ELSE 0
             END) AS t2_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=2
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 4
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t2_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=4
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 6
                  THEN p1.org_num
                  ELSE 0
             END) AS t4_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=4
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 6
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t4_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=6
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 8
                  THEN p1.org_num
                  ELSE 0
             END) AS t6_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=6
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 8
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t6_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=8
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 10
                  THEN p1.org_num
                  ELSE 0
             END) AS t8_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=8
                   AND (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 < 10
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t8_push_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=10
                  THEN p1.org_num
                  ELSE 0
             END) AS t10_push_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600 >=10
                  THEN (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(p1.demand_create_time))/3600
                  ELSE 0
             END) AS t10_push_goods_duration
        -- push_2_send：小时：0/12/24/36/48/60/72+
        ,SUM(p1.org_num - p1.oos_num) AS need_send_num
        ,SUM(p1.oos_num) AS oos_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE p1.send_num
             END) AS real_send_num
        ,SUM(CASE WHEN p1.pur_send_time IS NULL OR p1.pur_send_time < p1.demand_push_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time)) * p1.send_num / 3600
             END) AS send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 12
                  THEN p1.send_num
                  ELSE 0
             END) AS t0_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 12
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t0_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 24
                  THEN p1.send_num
                  ELSE 0
             END) AS t12_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 24
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t12_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 36
                  THEN p1.send_num
                  ELSE 0
             END) AS t24_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 36
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t24_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 48
                  THEN p1.send_num
                  ELSE 0
             END) AS t36_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 48
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t36_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 60
                  THEN p1.send_num
                  ELSE 0
             END) AS t48_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 60
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t48_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 72
                  THEN p1.send_num
                  ELSE 0
             END) AS t60_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 < 72
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t60_send_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=72
                  THEN p1.send_num
                  ELSE 0
             END) AS t72_send_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600 >=72
                  THEN (UNIX_TIMESTAMP(p1.pur_send_time) - UNIX_TIMESTAMP(p1.demand_push_time))/3600
                  ELSE 0
             END) AS t72_send_goods_duration
        -- send_2_receipt：小时：0/12/24/36/48/60/72+
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE p1.send_num
             END) AS receipt_num
        ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
                  THEN 0
                  ELSE (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time)) * p1.send_num / 3600
             END) AS receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 12
                  THEN p1.send_num
                  ELSE 0
             END) AS t0_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 > 0
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 12
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t0_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 24
                  THEN p1.send_num
                  ELSE 0
             END) AS t12_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=12
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 24
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t12_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 36
                  THEN p1.send_num
                  ELSE 0
             END) AS t24_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=24
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 36
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t24_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 48
                  THEN p1.send_num
                  ELSE 0
             END) AS t36_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=36
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 48
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t36_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 60
                  THEN p1.send_num
                  ELSE 0
             END) AS t48_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=48
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 60
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t48_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 72
                  THEN p1.send_num
                  ELSE 0
             END) AS t60_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=60
                   AND (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 < 72
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t60_receipt_goods_duration
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=72
                  THEN p1.send_num
                  ELSE 0
             END) AS t72_receipt_goods_num
        ,SUM(CASE WHEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600 >=72
                  THEN (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time))/3600
                  ELSE 0
             END) AS t72_receipt_goods_duration
FROM zybiro.neo_pur_lock_detail AS p1
WHERE p1.depot_id IN (4, 5, 14)
  AND p1.order_pay_date < '2018-01-09'
GROUP BY p1.order_pay_date
ORDER BY p1.order_pay_date
;
