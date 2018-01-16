
# 0.load library =================================================
library(tidyverse)
library(implyr)
library(odbc)
library(recharts)
source("E:/R/echartR.R",encoding="utf-8")

# 1.连impala =====================================================
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = "Cloudera ODBC Driver for Impala",
  dsn = 'jolly_impala_64'
)

# 2.查询数据 =====================================================

## 2.0 说明：
### 2.0.1 数据报告期：2017-09-12 —— 2017-10-11 and 2017-12-12 —— 2018-01-11, 共61天
### 2.0.2 数据查询日期为2018-01-12，由于近期订单仍在采购或配货中，因此时效参考价值不太大

## 2.1 统计每天的订单量、商品数量等绝对数据

### 2.1.0 获取订单级别数据, 订单级别数据存放在zybiro.neo_pur_lock_orders表中

### 2.1.1 汇总统计黑五前后的数据, 订单级别

black_friday <- dbGetQuery(
  impala, 
  "SELECT t2.black_friday
  ,COUNT(t2.order_id) AS pay_order_num
  ,SUM(t2.org_goods_num) AS pay_goods_num
  ,SUM(CASE WHEN t2.is_aim_order = 'yes' THEN 1 ELSE 0 END) AS aim_order_num
  ,SUM(t2.demand_org_num) AS demand_goods_num
  ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
  THEN 1
  ELSE 0
  END) AS peihuo_order_num
  ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
  ELSE 0
  END) AS peihuo_duration
  ,SUM(CASE WHEN is_shiped = 1 THEN 1 ELSE 0 END) AS ship_order_num
  ,SUM(t2.ship_goods_num) AS ship_goods_num
  FROM zybiro.neo_pur_lock_orders AS t2
  WHERE t2.order_pay_date < '2018-01-09'
  GROUP BY t2.black_friday
  ;"
)
str(black_friday)
black_friday[, 2:9] <- lapply(black_friday[, 2:9], as.numeric)

black_friday2 <- black_friday %>% 
  mutate(aim_order_rate = aim_order_num / pay_order_num, 
         mean_peihuo_duration = peihuo_duration / peihuo_order_num, 
         demand_goods_rate = demand_goods_num / pay_goods_num, 
         peihuo_order_rate = peihuo_order_num / pay_order_num, 
         ship_order_rate = ship_order_num / pay_order_num, 
         ship_goods_rate = ship_goods_num / pay_goods_num) %>% 
  select(black_friday,  
         pay_order_num, 
         aim_order_num, 
         aim_order_rate, 
         pay_goods_num, 
         demand_goods_num, 
         demand_goods_rate, 
         peihuo_order_num, 
         peihuo_order_rate, 
         mean_peihuo_duration, 
         ship_order_num, 
         ship_order_rate, 
         ship_goods_num, 
         ship_goods_rate)
View(black_friday2)

black_friday2 %>% 
  ggplot(aes(x = black_friday, 
             y = aim_order_rate, 
             fill = black_friday)) +
  geom_col()

### 2.1.2 订单——汇总每一天

order_daily <- dbGetQuery(
  impala,
  "SELECT t2.black_friday
  ,t2.order_pay_date
  ,COUNT(t2.order_id) AS pay_order_num
  ,SUM(t2.org_goods_num) AS pay_goods_num
  ,SUM(CASE WHEN t2.is_aim_order = 'yes' THEN 1 ELSE 0 END) AS aim_order_num
  ,SUM(t2.demand_org_num) AS demand_goods_num
  ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
  THEN 1
  ELSE 0
  END) AS peihuo_order_num
  ,SUM(CASE WHEN t2.outing_stock_time IS NOT NULL AND t2.outing_stock_time > t2.order_pay_time
  THEN (UNIX_TIMESTAMP(t2.outing_stock_time) - UNIX_TIMESTAMP(t2.order_pay_time)) / 3600
  ELSE 0
  END) AS peihuo_duration
  ,SUM(CASE WHEN is_shiped = 1 THEN 1 ELSE 0 END) AS ship_order_num
  ,SUM(t2.ship_goods_num) AS ship_goods_num
  FROM zybiro.neo_pur_lock_orders AS t2
  GROUP BY t2.black_friday
  ,t2.order_pay_date
  ;"
)
str(order_daily)
order_daily[, 3:10] <- lapply(order_daily[, 3:10], as.numeric)

order_daily2 <- order_daily %>% 
  filter(order_pay_date < '2018-01-09') %>% 
  mutate(aim_order_rate = aim_order_num / pay_order_num, 
         mean_peihuo_duration = peihuo_duration / peihuo_order_num, 
         demand_goods_rate = demand_goods_num / pay_goods_num, 
         peihuo_order_rate = peihuo_order_num / pay_order_num, 
         ship_order_rate = ship_order_num / pay_order_num, 
         ship_goods_rate = ship_goods_num / pay_goods_num) %>% 
  select(black_friday,  
         order_pay_date, 
         pay_order_num, 
         aim_order_num, 
         aim_order_rate, 
         pay_goods_num, 
         demand_goods_num, 
         demand_goods_rate, 
         peihuo_order_num, 
         peihuo_order_rate, 
         mean_peihuo_duration, 
         ship_order_num, 
         ship_order_rate, 
         ship_goods_num, 
         ship_goods_rate)

str(order_daily2)

order_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = mean_peihuo_duration, 
             color = black_friday)) + 
  geom_point() + 
  ylim(0, 80)


### 2.1.3 汇总每天各个节点的平均时效
duration_daily <- dbGetQuery(
  impala, 
  "SELECT p1.order_pay_date
  -- create_2_push_duration
  ,SUM(p1.org_num) AS demand_goods_num
  ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
  THEN 0
  ELSE p1.org_num
  END) AS push_goods_num
  ,SUM(CASE WHEN p1.demand_push_time IS NULL OR p1.demand_push_time < p1.demand_create_time
  THEN 0
  ELSE (UNIX_TIMESTAMP(p1.demand_push_time) - UNIX_TIMESTAMP(P1.demand_create_time)) * p1.org_num /3600
  END) AS push_goods_duration
  -- push_2_send_duration
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
  -- send_2_receipt_duration
  ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
  THEN 0
  ELSE p1.send_num
  END) AS receipt_num
  ,SUM(CASE WHEN p1.receipt_time IS NULL OR p1.receipt_time < p1.pur_send_time
  THEN 0
  ELSE (UNIX_TIMESTAMP(p1.receipt_time) - UNIX_TIMESTAMP(p1.pur_send_time)) * p1.send_num / 3600
  END) AS receipt_goods_duration
  FROM zybiro.neo_pur_lock_detail AS p1
  WHERE p1.depot_id IN (4, 5, 14)
  GROUP BY p1.order_pay_date
  ORDER BY p1.order_pay_date
  ;"
)
str(duration_daily)
duration_daily[, 2:10] <- lapply(duration_daily[, 2:10], as.numeric)

duration_daily2 <- duration_daily %>% 
  mutate(mean_push_duration = push_goods_duration / push_goods_num, 
         mean_send_duration = send_goods_duration / real_send_num, 
         mean_receipt_duration = receipt_goods_duration / real_send_num, 
         push_goods_rate = push_goods_num / demand_goods_num, 
         oos_rate = oos_num / push_goods_num, 
         need_send_rate = 1 - oos_rate, 
         real_send_rate = real_send_num / need_send_num)
str(duration_daily2)
summary(duration_daily2)
### 2.1.4 join order_daily and all_duration
#### join
order_duration_daily <- order_daily2 %>% 
  left_join(duration_daily2, by = 'order_pay_date')
#### 修改日期格式
order_duration_daily[, 2] <- as.Date(order_duration_daily[, 2])
#### 汇总各个节点的时间, 剔除1月9日之前的数据
order_duration_daily <- order_duration_daily %>% 
  filter(order_pay_date < '2018-01-09') %>% 
  mutate(mean_demand_duration = mean_push_duration + 
           mean_send_duration + 
           mean_receipt_duration)
#### summary
summary(order_duration_daily)

str(order_duration_daily)

#### 推送时长分布
order_duration_daily %>% 
  ggplot(aes(x = black_friday, y = mean_push_duration)) + 
  geom_boxplot()

#### 发货时长分布
order_duration_daily %>% 
  ggplot(aes(x = black_friday, y = mean_send_duration)) + 
  geom_boxplot()

#### 发货至到货签收时长分布
order_duration_daily %>% 
  ggplot(aes(x = black_friday, y = mean_receipt_duration)) + 
  geom_boxplot()

#### 需求处理总时长分布
order_duration_daily %>% 
  ggplot(aes(x = black_friday, y = mean_demand_duration)) + 
  geom_boxplot()

str(order_duration_daily)

order_duration_daily %>% 
  filter(black_friday == 'before' & order_pay_date < '2018-01-09') %>% 
  ggplot(aes(x = mean_demand_duration, 
             y = mean_peihuo_duration)) + 
  geom_point()

order_duration_daily %>% 
  filter(black_friday == 'after' & order_pay_date < '2018-01-09') %>% 
  echartr(x = ~mean_demand_duration, 
          y = ~mean_peihuo_duration, 
          type = 'point')

summary(order_duration_daily$mean_peihuo_duration)


# 3.预测tn期配货完成订单量 =======================================
## 3.1 数据整理
### 3.1.1 订单配货：tn期完成配货订单数
order_daily_tn <- dbGetQuery(
  impala, 
  "SELECT t2.black_friday
  ,t2.order_pay_date
  ,COUNT(t2.order_id) AS pay_order_num
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
  FROM zybiro.neo_pur_lock_orders AS t2
  GROUP BY t2.black_friday
  ,t2.order_pay_date
  ORDER BY t2.order_pay_date
  ;"
)
#### 查看数据集属性
str(order_daily_tn)
#### 修改数据集字段类型
order_daily_tn[, 3:19] = lapply(order_daily_tn[, 3:19], as.numeric)

### 3.1.2 采购需求各环节时效和量
duration_daily_tn <- dbGetQuery(
  impala, 
  "SELECT p1.order_pay_date
  -- create_2_push:小时：0/2/4/6/8/10+
  ,SUM(p1.org_num) AS demand_goods_num
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
  GROUP BY p1.order_pay_date
  ORDER BY p1.order_pay_date
  ;"
)
#### 查看数据集属性
str(duration_daily_tn)
#### 修改字段类型
duration_daily_tn[, 2:46] <- lapply(duration_daily_tn[, 2:46], as.numeric)


### 3.1.3 join两个表
order_duration_daily_tn <- order_daily_tn %>% 
  
  left_join(duration_daily_tn, by = 'order_pay_date')

View(order_duration_daily_tn)


order_duration_daily_tn %>% 
  ggplot(aes(x = pay_order_num, y = t0_peihuo_order_num)) + 
  geom_point()









