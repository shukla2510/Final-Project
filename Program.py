from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('abc').getOrCreate()
from pyspark.sql.functions import *
 
pddf = spark.read.text('gs://b-raw-data/20181201_20181207_HindiGEC_Program_Program_HindiGEC_20181201_20181207.txt')
pddf.write.parquet('gs://b-raw-zone/20181201_20181207_HindiGEC_Program/')
progpar = spark.read.parquet('gs://b-raw-zone/20181201_20181207_HindiGEC_Program/')
 
program_tab=progpar.withColumn("col1", split(col("value"), "\\;").getItem(0)) \
.withColumn("Channel", split(col("value"), "\\;").getItem(1)) \
.withColumn("Date", split(col("value"), "\\;").getItem(2)) \
.withColumn("Description", split(col("value"), "\\;").getItem(3)) \
.withColumn("Programme_Theme", split(col("value"), "\\;").getItem(4)) \
.withColumn("Programme_Genre", split(col("value"), "\\;").getItem(5)) \
.withColumn("Level", split(col("value"), "\\;").getItem(6)) \
.withColumn("Start_Time", split(col("value"), "\\;").getItem(7)) \
.withColumn("End_Time", split(col("value"), "\\;").getItem(8)) \
.withColumn("Length", split(col("value"), "\\;").getItem(9)) \
.withColumn("HSM_Universe", split(col("value"), "\\;").getItem(10)) \
.withColumn("HSM_15+", split(col("value"), "\\;").getItem(11)) \
.withColumn("HSM_Female_15+_AB", split(col("value"), "\\;").getItem(12)) \
.withColumn("HSM_Urban_Universe", split(col("value"), "\\;").getItem(13)) \
.withColumn("HSM_Urban_15+", split(col("value"), "\\;").getItem(14)) \
.withColumn("HSM_Urban_Female_15+_AB", split(col("value"), "\\;").getItem(15)) \
.withColumn("HSM_Rural_Universe", split(col("value"), "\\;").getItem(16)) \
.withColumn("HSM_Rural_15+", split(col("value"), "\\;").getItem(17)) \
.withColumn("HSM_Rural_Female_15+_AB", split(col("value"), "\\;").getItem(18)) \
.withColumn("Mega_Cities_Universe", split(col("value"), "\\;").getItem(19)) \
.withColumn("Mega_Cities_15+", split(col("value"), "\\;").getItem(20)) \
.withColumn("Mega_Cities_Female_15+", split(col("value"), "\\;").getItem(21))
 
program_tab_stag = program_tab.drop('value')
program_tab_final=program_tab_stag.where((program_tab_stag.col1 != '" "') & (program_tab_stag.col1 !='"[TOTAL]"'))
program_tab_final.write.parquet("gs://b-refined-zone/program_data/")