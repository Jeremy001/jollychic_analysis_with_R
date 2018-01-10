
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
## 2.1 供应商商品数、有重量的商品数、在售商品数
provider_info <- dbGetQuery(
  impala,
  "WITH
  t1 AS
  (SELECT p1.provider_code
  ,COUNT(p1.goods_id) AS total_goods_count
  ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) AS have_weight_goods_count
  ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS have_weight_rate
  ,SUM(CASE WHEN p1.is_on_sale = 1 THEN 1 ELSE 0 END) AS onsale_goods_count
  ,SUM(CASE WHEN p1.is_on_sale = 1 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS onsale_goods_rate
  FROM jolly.who_goods AS p1
  GROUP BY p1.provider_code
  )
  SELECT *
  FROM t1
  ORDER BY total_goods_count DESC
  ;"
)

provider_info$total_goods_count <- as.integer(provider_info$total_goods_count)
provider_info$have_weight_goods_count <- as.integer(provider_info$have_weight_goods_count)
provider_info$onsale_goods_count <- as.integer(provider_info$onsale_goods_count)

provider_info %>% 
  filter(total_goods_count >= 200 
         & have_weight_rate < 0.5) %>% 
  View()
  
### total_goods_count vs have_weight_rate
provider_info %>% 
  filter(total_goods_count >= 200 
         & total_goods_count < 10000) %>% 
  ggplot(aes(total_goods_count, have_weight_rate)) + 
  geom_point()

provider_info %>% 
  filter(total_goods_count <= 500) %>% 
  ggplot(aes(x = total_goods_count)) + 
  geom_histogram(binwidth = 10)

summary(provider_info)

## 2.2 蓄水池商品
supp_pool_info <- dbGetQuery(
  impala,
  "WITH
  t1 AS
  (SELECT FROM_UNIXTIME(p1.gmt_created, 'yyyy') AS add_year
  ,FROM_UNIXTIME(p1.gmt_created, 'yyyy-MM') AS add_month
  ,p1.supp_code
  ,COUNT(p1.goods_id) AS total_goods_count
  ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) AS have_weight_goods_count
  ,SUM(CASE WHEN p1.goods_weight >= 0.00001 THEN 1 ELSE 0 END) / COUNT(p1.goods_id) AS have_weight_rate
  FROM jolly.who_product_pool AS p1
  GROUP BY FROM_UNIXTIME(p1.gmt_created, 'yyyy')
  ,FROM_UNIXTIME(p1.gmt_created, 'yyyy-MM')
  ,p1.supp_code
  )
  SELECT *
  FROM t1
  ;"
)

supp_pool_info$total_goods_count <- as.integer(supp_pool_info$total_goods_count)
supp_pool_info$have_weight_goods_count <- as.integer(supp_pool_info$have_weight_goods_count)

sum(supp_pool_info$total_goods_count)
sum(supp_pool_info$have_weight_goods_count)

supp_pool_info %>% 
  group_by(add_year, add_month) %>% 
  summarise(total_goods_count = sum(total_goods_count),
            have_weight_goods_count = sum(have_weight_goods_count))









