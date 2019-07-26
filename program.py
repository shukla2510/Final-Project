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
        progpar = spark.read.parquet(raw_zone_path)

        program_tab=progpar.withColumnRenamed('Channel','Channel')\
        .withColumnRenamed('Date','Episode_Date')\
        .withColumnRenamed('Description','Description')\
        .withColumnRenamed('Programme Theme','Programme_Theme')\
        .withColumnRenamed('Programme Genre','Programme_Genre')\
        .withColumnRenamed('Level','Level')\
        .withColumnRenamed('Start Time {Av(Tm)}','Start_Time')\
        .withColumnRenamed('End Time {Av(Tm)}','End_Time')\
        .withColumnRenamed('Length [hhmmss] {Av}','Length')\
        .withColumnRenamed('HSM','HSM_Universe')\
        .withColumnRenamed(' 11','HSM_15_plus')\
        .withColumnRenamed(' 12','HSM_Female_15_plus_AB')\
        .withColumnRenamed('HSM Urban','HSM_Urban_Universe')\
        .withColumnRenamed(' 14','HSM_Urban_15_plus')\
        .withColumnRenamed(' 15','HSM_Urban_Female_15_plus_AB')\
        .withColumnRenamed('HSM Rural','HSM_Rural_Universe')\
        .withColumnRenamed(' 17','HSM_Rural_15_plus')\
        .withColumnRenamed(' 18','HSM_Rural_Female_15_plus_AB')\
        .withColumnRenamed('Mega Cities','Mega_Cities_Universe')\
        .withColumnRenamed(' 20','Mega_Cities_15_plus')\
        .withColumnRenamed(' 21','Mega_Cities_Female_15_plus')\
        .drop(' 0')

        program_tab_final=program_tab.where((program_tab.Channel != ' ') & (program_tab.Channel !='"[TOTAL]'))  
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
