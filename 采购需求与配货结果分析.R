
# 0.library
library(tidyverse)

# 1.连impala
library(implyr)
library(odbc)
drv <- odbc::odbc()
impala <- src_impala(
  drv = drv,
  driver = "Cloudera ODBC Driver for Impala",
  dsn = 'jolly_impala_64'
)

# 2.查询数据
pur_outing_detail <- tbl(impala, 
                         sql("SELECT * 
                              FROM zybiro.neo_pur_demand_peihuo_detail"))
## 修改order_id和sku_id的类型
pur_outing_detail$order_id <- as.character(pur_outing_detail$order_id)
pur_outing_detail$sku_id <- as.character(pur_outing_detail$sku_id)

pur_outing_detail %>% 
  filter(order_id == 27077099) %>% 
  View()

pur_outing_detail %>% 
  summarise(a = avg(send_num))

min(pur_outing_detail$order_id, na.rm = TRUE)
