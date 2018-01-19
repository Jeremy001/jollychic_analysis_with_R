
# 0.load library =================================================
library(tidyverse)
library(implyr)
library(odbc)
library(recharts)
library(corrplot)
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
# order_daily_sql <- read_file('./pur_demand_vs_outing_stock/order_daily.sql')
# order_daily <- dbGetQuery(impala, order_daily_sql)

#### 把字段类型转成numeric
# order_daily[, 2:25] <- lapply(order_daily[, 2:25], as.numeric)

#### 计算字段，结果保存为order_daily2
# order_daily2 <- order_daily %>% 
#   mutate(aim_order_rate = aim_order_num / pay_order_num, 
#          demand_goods_rate = demand_goods_num / pay_goods_num,
#          mean_peihuo_duration = peihuo_duration / peihuo_order_num, 
#          peihuo_order_rate = peihuo_order_num / pay_order_num, 
#          ship_order_rate = ship_order_num / pay_order_num, 
#          ship_goods_rate = ship_goods_num / pay_goods_num)

#### 把结果保存到项目本地，csv文件
# write.csv(order_daily2, 
#           './pur_demand_vs_outing_stock/order_daily.csv', 
#           quote = FALSE, 
#           row.names = FALSE)

#### 从本地读取order_daily
order_daily2 <- read.csv('./pur_demand_vs_outing_stock/order_daily.csv', 
                         header = TRUE, 
                         sep = ',', 
                         stringsAsFactors = FALSE)

#### 平均配货时长
order_daily2 %>% 
  ggplot(aes(x = order_pay_date, 
             y = mean_peihuo_duration)) + 
  geom_point() + 
  ylim(0, 80)


## 2.2 统计每天采购需求各节点数据（处理量和时效）===============

#### 读入sql文件，查询duration_daily
# duration_daily_sql <- read_file('./pur_demand_vs_outing_stock/duration_daily.sql')
# duration_daily <- dbGetQuery(impala, duration_daily_sql)

#### 把字段类型转成numeric
# duration_daily[, 2:49] <- lapply(duration_daily[, 2:49], as.numeric)

#### 计算字段，结果保存为duration_daily2
# duration_daily2 <- duration_daily %>% 
#   mutate(mean_push_duration = push_goods_duration / push_goods_num, 
#          mean_send_duration = send_goods_duration / real_send_num, 
#          mean_receipt_duration = receipt_goods_duration / real_send_num, 
#          oos_rate = oos_num / push_goods_num, 
#          need_send_rate = 1 - oos_rate, 
#          real_send_rate = real_send_num / need_send_num)

#### 把结果保存到项目本地，csv文件
# write.csv(duration_daily2, 
#           './pur_demand_vs_outing_stock/duration_daily.csv', 
#           quote = FALSE, 
#           row.names = FALSE)

#### 从本地读取duration_daily
duration_daily2 <- read.csv('./pur_demand_vs_outing_stock/duration_daily.csv', 
                           header = TRUE, 
                           sep = ',', 
                           stringsAsFactors = FALSE)

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
# order_duration_daily <- order_daily2 %>% 
#   left_join(duration_daily2, by = 'order_pay_date')

#### 把结果保存到项目本地，csv文件
# write.csv(order_duration_daily, 
#           './pur_demand_vs_outing_stock/order_duration_daily.csv', 
#           quote = FALSE, 
#           row.names = FALSE)

#### 从本地读取duration_daily
order_duration_daily <- read.csv('./pur_demand_vs_outing_stock/order_duration_daily.csv', 
                                 header = TRUE, 
                                 sep = ',', 
                                 stringsAsFactors = FALSE)

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

## 3.0 说明
##     预测t0期的下单订单中，在tn(n=0, 1, 2, ...)天完成配货的订单量
##     特征变量：订单量、商品量、命中率、推送|发货|签收量和时效等

## 3.1 预测付款订单中多少订单在当天(t0)完成配货 ==================
## 目标变量：t0_peihuo_order_num
## 特征变量：1.pay_order_num, pay_goods_num, aim_order_rate,demand_goods_rate,
##           2.各时间段推送数量:tn_push_goods_num
##           3.24h内发货数量：t0_send_goods_num, t12_send_goods_num
##           4.24h内签收数量：t0_receipt_goods_num, t12_receipt_goods_num

### 3.1.1 筛选特征变量，构建数据集
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
  # filter(aim_order_rate < 0.1) %>% 
  ggplot(aes(x = aim_order_rate, 
             y = t0_peihuo_order_num)) + 
  geom_point()

#### 相关系数
#### 1.图
t0_corr <- cor(t0_order_duration)
corrplot(corr = t0_corr)
#### 发现t0_peihuo_order_num与aim_order_rate的相关系数最大
#### 2.值
cor(x = t0_order_duration[, 2:15], 
    y = t0_order_duration[, 1])

### 3.1.2 建模
model_lm <- lm(t0_peihuo_order_num~., 
               data = t0_order_duration)
##### 建模结果
summary(model_lm)

### 3.1.3 
model_lm_step <- step(model_lm)

summary(model_lm_step)

mean(t0_order_duration$t0_peihuo_order_num)

#### 残差
residuals(model_lm_step)

#### 残差图
plot(residuals(model_lm_step))

#### 残差/实际单数
res_minus <- residuals(model_lm_step)/t0_order_duration$t0_peihuo_order_num
plot(res_minus)
max(res_minus)
min(res_minus[1:30])
#### 发现黑五前的预测效果还是很不错的，但是黑五后效果就不够好


t0_order_duration[29,] %>% 
  View()
residuals(model_lm_step)[29]

t1 <- as.data.frame(as.matrix(lapply(t0_order_duration, mean)))

t1$test_col <- row.names(t1)
t2 <- as.data.frame(coefficients(model_lm_step))
t2$test_col <- row.names(t2)
colnames(t2) <- c('coef', 'test_col')

t12 <- t2 %>% 
  inner_join(t1, by = 'test_col')

write.csv(t12, "./test.csv")

#### 均方误差
mean(residuals(model_lm_step)^2)

#### 均方根误差
sqrt(mean(residuals(model_lm_step)^2))

#### 再次建模，筛选变量
t0_order_duration2 <- order_duration_daily %>% 
  select(t0_peihuo_order_num, 
         aim_order_rate, 
         demand_goods_rate)

model_lm2 <- lm(t0_peihuo_order_num ~ ., 
                data = t0_order_duration2)

summary(model_lm2)

#### 模型效果
plot(residuals(model_lm2))
res_minus2 <- residuals(model_lm2) / t0_order_duration2$t0_peihuo_order_num
plot(res_minus2)
sqrt(mean(residuals(model_lm2)^2))
max(res_minus2)
min(res_minus2)
mean(abs(res_minus2))
sqrt(mean(residuals(model_lm2)^2))/mean(t0_order_duration$t0_peihuo_order_num)


t0_order_duration2 %>% 
  ggplot(aes(x = t0_peihuo_order_num, 
             y = t0_peihuo_order_num_pre)) + 
  geom_point()

t0_order_duration2 %>% 
  ggplot(aes(x = t0_peihuo_order_num, 
             y = aim_order_rate)) + 
  geom_point()

ggplot() + 
  geom_point(aes(x = 1:58, 
                 y = t0_order_duration2$t0_peihuo_order_num, 
                 color = I('red'))) + 
  geom_point((aes(x = 1:58, 
                  y = predict(model_lm2, 
                              t0_order_duration2[, -1]), 
                  color = I('blue'))))

max(abs(res_minus2[1:30]))
min(abs(res_minus2[1:30]))



## 3.2 预测付款订单中多少订单在第二天(t1)完成配货 ==================
## 目标变量：t1_peihuo_order_num
## 特征变量：1.pay_order_num, pay_goods_num, aim_order_rate,demand_goods_rate,
##           2.各时间段推送数量:tn_push_goods_num
##           3.48h内发货数量：tn_send_goods_num
##           4.48h内签收数量：tn_receipt_goods_num

### 3.1.1 筛选特征变量，构建数据集
t1_order_duration <- order_duration_daily %>% 
  select(t1_peihuo_order_num, 
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
         t24_send_goods_num, 
         t36_send_goods_num, 
         t0_receipt_goods_num, 
         t12_receipt_goods_num, 
         t24_receipt_goods_num, 
         t36_receipt_goods_num)

#### 相关系数
#### 1.图
t1_corr <- cor(t1_order_duration)
corrplot(corr = t1_corr)
#### 发现t1_peihuo_order_num与其他各个变量的相关系数都不太大
#### 2.值
cor(x = t1_order_duration[, 2:19], 
    y = t1_order_duration[, 1])

### 3.1.2 建模
model_lm <- lm(t1_peihuo_order_num~., 
               data = t1_order_duration)
##### 建模结果
summary(model_lm)

### 3.1.3 
model_lm_step <- step(model_lm)

summary(model_lm_step)


#### 残差
residuals(model_lm_step)

#### 残差图
plot(residuals(model_lm_step))

#### 残差/实际单数
res_minus <- residuals(model_lm_step)/t1_order_duration$t1_peihuo_order_num
plot(res_minus)
max(res_minus)
min(res_minus)
max(abs(res_minus))
min(abs(res_minus))

#### 均方根误差
sqrt(mean(residuals(model_lm_step)^2))

#### 均方根误差/平均订单数
sqrt(mean(residuals(model_lm2)^2))/mean(t1_order_duration$t1_peihuo_order_num)

ggplot() + 
  geom_point(aes(x = 1:58, 
                 y = t1_order_duration$t1_peihuo_order_num, 
                 color = I('red'))) + 
  geom_point((aes(x = 1:58, 
                  y = predict(model_lm, 
                              t1_order_duration[, -1]), 
                  color = I('blue'))))
actual_pred <- data.frame(actual = t1_order_duration$t1_peihuo_order_num, 
                          pred = predict(model_lm, 
                                         t1_order_duration[, -1]))
actual_pred %>% 
  mutate(rate = residuals(model_lm)/actual) %>% 
  View()


head(actual_pred)

max(abs(res_minus2[1:30]))
min(abs(res_minus2[1:30]))



## 3.3 预测付款订单中多少订单在第三天(t2)完成配货 ==================
## 目标变量：t1_peihuo_order_num
## 特征变量：1.pay_order_num, pay_goods_num, aim_order_rate,demand_goods_rate,
##           2.各时间段推送数量:tn_push_goods_num
##           3.72h内发货数量：tn_send_goods_num
##           4.72h内签收数量：tn_receipt_goods_num

### 3.3.1 筛选特征变量，构建数据集
t2_order_duration <- order_duration_daily %>% 
  select(t2_peihuo_order_num, 
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
         t24_send_goods_num, 
         t36_send_goods_num, 
         t48_send_goods_num, 
         t60_send_goods_num, 
         t0_receipt_goods_num, 
         t12_receipt_goods_num, 
         t24_receipt_goods_num, 
         t36_receipt_goods_num, 
         t48_receipt_goods_num, 
         t60_receipt_goods_num)

#### 相关系数
#### 1.图
t2_corr <- cor(t2_order_duration)
corrplot(corr = t2_corr)
#### 发现t1_peihuo_order_num与其他各个变量的相关系数都不太大
#### 2.值
cor(x = t2_order_duration[, 2:23], 
    y = t2_order_duration[, 1])

### 3.3.2 建模
model_lm <- lm(t2_peihuo_order_num~., 
               data = t2_order_duration)
##### 建模结果
summary(model_lm)

### 3.3.3 
model_lm_step <- step(model_lm)

summary(model_lm_step)


#### 残差
residuals(model_lm_step)

#### 残差图
plot(residuals(model_lm_step))

#### 残差/实际单数
res_minus <- residuals(model_lm_step)/t2_order_duration$t2_peihuo_order_num
plot(res_minus)
max(res_minus)
min(res_minus)
max(abs(res_minus))
min(abs(res_minus))

#### 均方根误差
sqrt(mean(residuals(model_lm_step)^2))

#### 均方根误差/平均订单数
sqrt(mean(residuals(model_lm2)^2))/mean(t1_order_duration$t1_peihuo_order_num)

ggplot() + 
  geom_point(aes(x = 1:58, 
                 y = t2_order_duration$t2_peihuo_order_num, 
                 color = I('red'))) + 
  geom_point((aes(x = 1:58, 
                  y = predict(model_lm, 
                              t2_order_duration[, -1]), 
                  color = I('blue'))))
actual_pred <- data.frame(actual = t2_order_duration$t2_peihuo_order_num, 
                          pred = predict(model_lm, 
                                         t2_order_duration[, -1]))
actual_pred %>% 
  mutate(rate = residuals(model_lm)/actual) %>% 
  ggplot(aes(x = 1:58, 
             y = rate)) + 
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









