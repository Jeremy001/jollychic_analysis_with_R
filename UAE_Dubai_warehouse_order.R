
# 0.load library =================================================
library(tidyverse)
library(implyr)
library(odbc)
library(reshape2)

# 1.连impala =====================================================
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = "Cloudera ODBC Driver for Impala",
  dsn = 'jolly_impala_64'
)

# 2.取数据，处理之 ===============================================

## 取数：order_id,sku_id,pay_date, goods_number
order_sku_detail <- dbGetQuery(
  impala, 
  "SELECT p1.order_id
  ,SUBSTR(CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END, 1, 10) AS pay_date
  ,p2.sku_id
  ,p2.goods_number
  FROM zydb.dw_order_sub_order_fact AS p1
  LEFT JOIN zydb.dw_order_goods_fact AS p2
  ON p1.order_id = p2.order_id
  WHERE p1.country_name = 'United Arab Emirates'
  AND (CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END) >= '2018-01-02'
  AND (CASE WHEN p1.pay_id = 41 THEN p1.pay_time ELSE p1.result_pay_time END) < '2018-01-16'
  ;"
)

## 修改字段类型
str(order_sku_detail)
order_sku_detail[, 1] <- as.character(order_sku_detail[, 1])
order_sku_detail[, 3] <- as.numeric(order_sku_detail[, 3])

## filter goods_number>=1的record, 并转成宽型表
order_sku_detail2 <- order_sku_detail %>% 
  filter(goods_number >= 1) %>% 
  arrange(sku_id, pay_date) %>% 
  select(sku_id, order_id, goods_number) %>% 
  dcast(sku_id ~ order_id) %>% 
  head(20) %>% 
  View()
  



str(order_sku_detail2)









