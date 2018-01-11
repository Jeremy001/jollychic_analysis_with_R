
# 0.load library =================================================
library(tidyverse)
library(implyr)
library(odbc)

# 1.连impala =====================================================
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = "Cloudera ODBC Driver for Impala",
  dsn = 'jolly_impala_64'
)

# 2.查询数据 =====================================================

## 2.1 统计每天的订单量、商品数量等绝对数据
daily_amount <- dbGetQuery(
  impala,
  "SELECT p1.order_pay_date
  ,COUNT(DISTINCT p1.order_id) AS order_num
  ,SUM(p1.original_goods_number) AS org_goods_num
  ,SUM(p1.goods_number) AS ship_goods_num
  ,SUM(p1.original_goods_number - NVL(p1.org_num, 0)) AS aim_goods_num
  ,SUM(p1.original_goods_number - NVL(p1.org_num, 0)) / SUM(p1.original_goods_number) AS aim_goods_rate
  ,SUM(p1.original_goods_number) - SUM(p1.original_goods_number - NVL(p1.org_num, 0)) AS pur_goods_num
  ,1 - SUM(p1.original_goods_number - NVL(p1.org_num, 0)) / SUM(p1.original_goods_number) AS pur_goods_rate
  ,SUM(CASE WHEN p1.demand_push_time IS NULL THEN 0 ELSE NVL(p1.org_num, 0) END) AS push_goods_num
  ,SUM(p1.oos_num) AS oos_goods_num
  ,SUM(p1.send_num) AS send_goods_num
  FROM zybiro.neo_pur_demand_receipt_peihuo AS p1
  WHERE p1.depot_id IN (4, 5, 14)
  AND p1.order_pay_date < '2018-01-10'
  GROUP BY p1.order_pay_date
  ORDER BY p1.order_pay_date
  ;"
)

## 2.2 推送时长
push_distribute <- dbGetQuery(
  impala, 
  "SELECT p1.order_pay_date
  ,p1.order_id
  ,p1.demand_create_time
  ,p1.demand_push_time
  ,(UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) / 60 AS push_duration
  FROM zybiro.neo_pur_demand_receipt_peihuo AS p1
  WHERE p1.depot_id IN (4, 5, 14)
  AND p1.order_pay_date < '2018-01-10'
  AND p1.demand_push_time IS NOT NULL
  GROUP BY p1.order_pay_date
  ,p1.order_id
  ,p1.demand_create_time
  ,p1.demand_push_time
  ORDER BY RAND()
  LIMIT 50000
  ;"
)

push_distribute$order_id <- as.character(push_distribute$order_id)

str(push_distribute)

summary(push_distribute)

push_distribute %>% 
  filter(order_pay_date < '2017-10-13') %>% 
  summary()



push_distribute %>% 
  filter(order_pay_date < '2017-10-13') %>% 
  ggplot(aes(x = push_duration)) + 
  geom_histogram(binwidth = 1)

ggplot(push_distribute,
       aes(x = push_duration)) +
  geom_histogram()


pur_outing_detail %>% 
  filter(order_id == 27077099) %>% 
  View()

pur_outing_detail %>% 
  summarise(a = avg(send_num))

min(pur_outing_detail$order_id, na.rm = TRUE)
