
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