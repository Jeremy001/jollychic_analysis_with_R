
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

# 2.数据处理 =====================================================

## 2.0 数据说明：
##     报告期：2017-09-12 — 2017-10-11 and 2017-12-12 — 2018-01-08, 共58天
##     数据查询日期为2018-01-12

## 2.1 统计每天的订单类数据 ======================================
##     订单级别数据存放在zybiro.neo_pur_lock_orders表中

#### 读入sql文件，查询order_daily
order_daily_sql <- read_file('./pur_demand_vs_outing_stock/order_daily.sql')
order_daily <- dbGetQuery(impala, order_daily_sql)

#### 把字段类型转成numeric
order_daily[, 2:25] <- lapply(order_daily[, 2:25], as.numeric)

#### 计算字段，结果保存为order_daily2
order_daily2 <- order_daily %>% 
  mutate(aim_order_rate = aim_order_num / pay_order_num, 
         demand_goods_rate = demand_goods_num / pay_goods_num,
         mean_peihuo_duration = peihuo_duration / peihuo_order_num, 
         peihuo_order_rate = peihuo_order_num / pay_order_num, 
         ship_order_rate = ship_order_num / pay_order_num, 
         ship_goods_rate = ship_goods_num / pay_goods_num)

#### 平均配货时长
order_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = mean_peihuo_duration)) + 
  geom_point() + 
  ylim(0, 80)


## 2.2 统计每天采购需求各节点数据（处理量和时效）===============

#### 读入sql文件，查询duration_daily
duration_daily_sql <- read_file('./pur_demand_vs_outing_stock/duration_daily.sql')
duration_daily <- dbGetQuery(impala, duration_daily_sql)

#### 把字段类型转成numeric
duration_daily[, 2:49] <- lapply(duration_daily[, 2:49], as.numeric)

#### 计算字段，结果保存为duration_daily2
duration_daily2 <- duration_daily %>% 
  mutate(mean_push_duration = push_goods_duration / push_goods_num, 
         mean_send_duration = send_goods_duration / real_send_num, 
         mean_receipt_duration = receipt_goods_duration / real_send_num, 
         oos_rate = oos_num / push_goods_num, 
         need_send_rate = 1 - oos_rate, 
         real_send_rate = real_send_num / need_send_num)

#### 平均发货时长
#### 发现黑五前平均发货时长比黑五之后更高，是因为黑五前几乎是实时推送
#### 然而，实时推送后，供应商一般是在固定时间段处理
#### 比如晚上推送的只能等到第二天早上处理；
duration_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = mean_send_duration)) + 
  geom_point() + 
  ylim(0, 35)


## 2.3 join order_daily and duration_daily ======================

#### join, 得到order_duration_daily
order_duration_daily <- order_daily2 %>% 
  left_join(duration_daily2, by = 'order_pay_date')

#### 各个节点平均处理时间
order_duration_daily %>% 
  ggplot(aes(x = order_pay_date)) + 
  geom_point(aes(y = mean_peihuo_duration, 
                 color = I('black'))) + 
  geom_point(aes(y = mean_push_duration, 
                 color = I('red'))) + 
  geom_point(aes(y = mean_send_duration, 
                 color = I('blue'))) + 
  geom_point(aes(y = mean_receipt_duration, 
                 color = I('green'))) +
  ylim(0, 80)

# 3.预测tn期配货完成订单量 =======================================

## 3.1 预测付款订单中多少订单在当天(t0)完成配货 ==================
## 目标变量：t0_peihuo_order_num
## 特征变量：1.pay_order_num, pay_goods_num, aim_order_rate,demand_goods_rate,
##           2.各时间段推送数量:tn_push_goods_num
##           3.24h内发货数量：t0_send_goods_num, t12_send_goods_num
##           4.24h内签收数量：t0_receipt_goods_num, t12_receipt_goods_num

t0_order_duration <- order_duration_daily %>% 
  select(t0_peihuo_order_num, 
         pay_order_num, 
         pay_goods_num, 
         aim_order_rate,
         demand_goods_rate,
         t0_push_goods_num, 
         t2_push_goods_num, 
         t4_push_goods_num, 
         t6_push_goods_num, 
         t8_push_goods_num, 
         t10_push_goods_num, 
         t0_send_goods_num, 
         t12_send_goods_num, 
         t0_receipt_goods_num, 
         t12_receipt_goods_num)

t0_order_duration %>% 
  ggplot(aes(x = aim_order_rate, 
             y = t0_peihuo_order_num)) + 
  geom_point()











# 4.时效分布 ===================================================
#### 查看数据集属性
str(order_daily_tn)
#### 修改数据集字段类型
order_daily_tn[, 3:19] = lapply(order_daily_tn[, 3:19], as.numeric)

order_daily_tn2 <- order_daily_tn %>% 
  select(order_pay_date, 
         t0_peihuo_order_num, 
         t1_peihuo_order_num, 
         t2_peihuo_order_num, 
         t3_peihuo_order_num, 
         t4_peihuo_order_num, 
         t5_peihuo_order_num, 
         t6_peihuo_order_num, 
         t7_peihuo_order_num)
colnames(order_daily_tn2) <- c('order_pay_date', 
                               't0', 
                               't1', 
                               't2', 
                               't3',
                               't4', 
                               't5', 
                               't6', 
                               't7')
order_daily_tn3 <- melt(order_daily_tn2, 
                        id.vars = 'order_pay_date', 
                        variable.name = 'peihuo_tn', 
                        value.name = 'peihuo_order_num')

head(order_daily_tn3)
ggplot(order_daily_tn3, aes(x = order_pay_date, 
                            y = peihuo_order_num, 
                            fill = peihuo_tn)) + 
  geom_col(position = 'fill')



#### 查看数据集属性
str(duration_daily_tn)
#### 修改字段类型
duration_daily_tn[, 2:46] <- lapply(duration_daily_tn[, 2:46], as.numeric)


#### 推送时长分布
push_duration_daily <- duration_daily_tn %>% 
  select(order_pay_date, 
         t0_push_goods_num, 
         t2_push_goods_num, 
         t4_push_goods_num, 
         t6_push_goods_num, 
         t8_push_goods_num, 
         t10_push_goods_num)

colnames(push_duration_daily) <- c('order_pay_date', 
                                   't0', 
                                   't2', 
                                   't4', 
                                   't6', 
                                   't8', 
                                   't10')
push_duration_daily2 <- melt(push_duration_daily, 
                             id.vars = 'order_pay_date', 
                             variable.name = 'push_tn', 
                             value.name = 'goods_num')


head(push_duration_daily2)

push_duration_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = goods_num, 
             fill = push_tn)) + 
  geom_col(position = 'fill')

#### 发货时长分布
send_duration_daily <- duration_daily_tn %>% 
  select(order_pay_date, 
         t0_send_goods_num, 
         t12_send_goods_num, 
         t24_send_goods_num, 
         t36_send_goods_num, 
         t48_send_goods_num, 
         t60_send_goods_num, 
         t72_send_goods_num)

colnames(send_duration_daily) <- c('order_pay_date', 
                                   't0', 
                                   't12', 
                                   't24', 
                                   't36', 
                                   't48', 
                                   't60', 
                                   't72')
send_duration_daily2 <- melt(send_duration_daily, 
                             id.vars = 'order_pay_date', 
                             variable.name = 'send_tn', 
                             value.name = 'goods_num')


head(send_duration_daily2)

send_duration_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = goods_num, 
             fill = send_tn)) + 
  geom_col(position = 'fill')




#### 到货时长分布
receipt_duration_daily <- duration_daily_tn %>% 
  select(order_pay_date, 
         t0_receipt_goods_num, 
         t12_receipt_goods_num, 
         t24_receipt_goods_num, 
         t36_receipt_goods_num, 
         t48_receipt_goods_num, 
         t60_receipt_goods_num, 
         t72_receipt_goods_num)

colnames(receipt_duration_daily) <- c('order_pay_date', 
                                     't0', 
                                     't12', 
                                     't24', 
                                     't36', 
                                     't48', 
                                     't60', 
                                     't72')
receipt_duration_daily2 <- melt(receipt_duration_daily, 
                               id.vars = 'order_pay_date', 
                               variable.name = 'receipt_tn', 
                               value.name = 'goods_num')


head(receipt_duration_daily2)

receipt_duration_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = goods_num, 
             fill = receipt_tn)) + 
  geom_col(position = 'fill')


### 3.1.3 join两个表
order_duration_daily_tn <- order_daily_tn %>% 
  left_join(order_daily2, by = 'order_pay_date') %>% 
  left_join(duration_daily_tn, by = 'order_pay_date') %>% 
  View()
  select(order_pay_date, 
         pay_order_num, 
         t0_peihuo_order_num, 
         t1_peihuo_order_num, 
         t2_peihuo_order_num, 
         t3_peihuo_order_num, 
         t4_peihuo_order_num, 
         t5_peihuo_order_num, 
         t6_peihuo_order_num, 
         t7_peihuo_order_num) %>% 
    

View(order_duration_daily_tn)


order_duration_daily_tn %>% 
  ggplot(aes(x = pay_order_num, y = t0_peihuo_order_num)) + 
  geom_point()









