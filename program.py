import sys
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('extract_program_data').getOrCreate()
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import lit

class program_data:
    def save_data(self, file_name, raw_zone_bucket, refined_zone_bucket):
        pddf = spark.read.text(file_name)
        json_name = file_name.rsplit('.', 1)[0].rsplit('/', 1)[1]
        raw_zone_path = 'gs://{}/{}/'.format(raw_zone_bucket, json_name)
        pddf.write.mode('append').parquet(raw_zone_path)
        mompar = spark.read.parquet(raw_zone_path)

        program_tab=progpar.withColumn("col1", split(col("value"), "\\;").getItem(0)) \
        .withColumn("Channel", split(col("value"), "\\;").getItem(1)) \
        .withColumn("Episode_Date", split(col("value"), "\\;").getItem(2)) \
        .withColumn("Description", split(col("value"), "\\;").getItem(3)) \
        .withColumn("Programme_Theme", split(col("value"), "\\;").getItem(4)) \
        .withColumn("Programme_Genre", split(col("value"), "\\;").getItem(5)) \
        .withColumn("Level", split(col("value"), "\\;").getItem(6)) \
        .withColumn("Start_Time", split(col("value"), "\\;").getItem(7)) \
        .withColumn("End_Time", split(col("value"), "\\;").getItem(8)) \
        .withColumn("Length", split(col("value"), "\\;").getItem(9)) \
        .withColumn("HSM_Universe", split(col("value"), "\\;").getItem(10)) \
        .withColumn("HSM_15_plus", split(col("value"), "\\;").getItem(11)) \
        .withColumn("HSM_Female_15_plus_AB", split(col("value"), "\\;").getItem(12)) \
        .withColumn("HSM_Urban_Universe", split(col("value"), "\\;").getItem(13)) \
        .withColumn("HSM_Urban_15_plus", split(col("value"), "\\;").getItem(14)) \
        .withColumn("HSM_Urban_Female_15_plus_AB", split(col("value"), "\\;").getItem(15)) \
        .withColumn("HSM_Rural_Universe", split(col("value"), "\\;").getItem(16)) \
        .withColumn("HSM_Rural_15_plus", split(col("value"), "\\;").getItem(17)) \
        .withColumn("HSM_Rural_Female_15_plus_AB", split(col("value"), "\\;").getItem(18)) \
        .withColumn("Mega_Cities_Universe", split(col("value"), "\\;").getItem(19)) \
        .withColumn("Mega_Cities_15_plus", split(col("value"), "\\;").getItem(20)) \
        .withColumn("Mega_Cities_Female_15_plus", split(col("value"), "\\;").getItem(21))
    
        def remove_end_quote(col):
            return col.rsplit('"')[0]
        remove_end_quote_udf = F.UserDefinedFunction(remove_end_quote, T.StringType())

        # mom_tab_stag = program_tab.drop('value')
        program_tab_final=program_tab.where((program_tab.col1 != '" ') & (program_tab.col1 !='"[TOTAL]'))
        program_tab_final = program_tab_final.withColumn('Mega_Cities_Female_15_plus_new', remove_end_quote_udf(program_tab_final.Mega_Cities_Female_15_plus)).drop('Mega_Cities_Female_15_plus').drop('col1')
        program_tab_final.write.mode('append').parquet('gs://{}/program_data/'.format(refined_zone_bucket))

if __name__ == '__main__':

    file_name = sys.argv[1:][0]
    raw_zone_bucket = sys.argv[1:][1]
    refined_zone_bucket = sys.argv[1:][2]
    # file_name = 'gs://b-raw-data/20181201_20181207_HindiGEC_MoM_MoM_HindiGEC_20181201_20181207.txt'
    # raw_zone_bucket = 'b-raw-zone'
    # refined_zone_bucket = 'b-refined-zone'
    print('***************************************************************')
    print(file_name)
    print(raw_zone_bucket)
    print(refined_zone_bucket)
    print('***************************************************************')
    print(raw_zone_bucket)
    mom_data().save_data(file_name, raw_zone_bucket, refined_zone_bucket)
