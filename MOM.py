from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
from pyspark.sql.functions import *
 
pddf = spark.read.text('gs://b-raw-data/20181201_20181207_HindiGEC_MoM_MoM_HindiGEC_20181201_20181207.txt')
pddf.write.parquet('gs://b-raw-zone/20181201_20181207_HindiGEC_MoM/')
mompar = spark.read.parquet('gs://b-raw-zone/20181201_20181207_HindiGEC_MoM/')
 
mom_tab=mompar.withColumn("col1", split(col("value"), "\\;").getItem(0)) \
.withColumn("Channel", split(col("value"), "\\;").getItem(1)) \
.withColumn("Year&Week", split(col("value"), "\\;").getItem(2)) \
.withColumn("Date", split(col("value"), "\\;").getItem(3)) \
.withColumn("Start_Time", split(col("value"), "\\;").getItem(4)) \
.withColumn("HSM_Universe", split(col("value"), "\\;").getItem(5)) \
.withColumn("HSM_15+", split(col("value"), "\\;").getItem(6)) \
.withColumn("HSM_Female_15+_AB", split(col("value"), "\\;").getItem(7)) \
.withColumn("HSM_Urban_Universe", split(col("value"), "\\;").getItem(8)) \
.withColumn("HSM_Urban_15+", split(col("value"), "\\;").getItem(9)) \
.withColumn("HSM_Urban_Female_15+_AB", split(col("value"), "\\;").getItem(10)) \
.withColumn("HSM_Rural_Universe", split(col("value"), "\\;").getItem(11)) \
.withColumn("HSM_Rural_15+", split(col("value"), "\\;").getItem(12)) \
.withColumn("HSM_Rural_Female_15+_AB", split(col("value"), "\\;").getItem(13)) \
.withColumn("Mega_Cities_Universe", split(col("value"), "\\;").getItem(14)) \
.withColumn("Mega_Cities_15+", split(col("value"), "\\;").getItem(15)) \
.withColumn("Mega_Cities_Female_15+_AB", split(col("value"), "\\;").getItem(16))
 
 
 
mom_tab_stag = mom_tab.drop('value')
mom_tab_final=mom_tab_stag.where((mom_tab_stag.col1 != '" "') & (mom_tab_stag.col1 !='"[TOTAL]"'))
 
mom_tab_final.write.parquet("gs://b-refined-zone/mom_data/")