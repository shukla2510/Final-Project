from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('create hive table job').getOrCreate()
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import *

class CreateHiveTable:

    # source_bucket='b-refined-zone'

    """Reads parquet file from 'b-refined-zone' bucket"""
    def read_file(self, file_name, refined_bucket):
        return spark.read.parquet('gs://{}/{}/'.format(refined_bucket, file_name))

    def create_table_query(self, table_name, columns, refined_bucket):
            return 'create external table {} ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://{}/{}/";'.format(table_name, columns, refined_bucket, table_name)

    def trigger_process(self, refined_bucket):
        agePar = self.read_file('agegender', refined_bucket)
        charPar = self.read_file('character', refined_bucket)
        speechPar = self.read_file('speech', refined_bucket)
        locationPar = self.read_file('location', refined_bucket)
        shotanglePar = self.read_file('shotangle', refined_bucket)
        colorPar = self.read_file('color', refined_bucket)
        # programPar = self.read_file('program_data', refined_bucket)
        momPar = self.read_file('mom_data', refined_bucket)

        ageDataTypeList=[]
        charDataTypeList=[]
        speechDataTypeList=[]
        locationDataTypeList=[]
        shotangleDataTypeList=[]
        colorDataTypeList=[]
        # programParDataTypeList=[]
        momParDataTypeList=[]

        # agePar = spark.read.parquet('gs://b-refined-zone/agegender/')
        # charPar = spark.read.parquet('gs://b-refined-zone/character/')
        # speechPar = spark.read.parquet('gs://b-refined-zone/speech/')
        # locationPar = spark.read.parquet('gs://b-refined-zone/location/')
        # shotanglePar = spark.read.parquet('gs://b-refined-zone/shotangle/')
        # colorPar = spark.read.parquet('gs://b-refined-zone/color/')


        for i in agePar.dtypes:
            ageDataTypeList.append('{} {},'.format(i[0],i[1]))

        for i in shotanglePar.dtypes:
            shotangleDataTypeList.append('{} {},'.format(i[0],i[1]))

        for i in charPar.dtypes:
            charDataTypeList.append('{} {},'.format(i[0],i[1]))

        for i in speechPar.dtypes:
            speechDataTypeList.append('{} {},'.format(i[0],i[1]))

        for i in locationPar.dtypes:
            locationDataTypeList.append('{} {},'.format(i[0],i[1]))

        for i in colorPar.dtypes:
            colorDataTypeList.append('{} {},'.format(i[0],i[1]))

        # for i in programPar.dtypes:
        #     programParDataTypeList.append('{} {},'.format(i[0],i[1]))


        for i in momPar.dtypes:
            momParDataTypeList.append('{} {},'.format(i[0],i[1]))



        age_stag_str=''
        char_stag_str=''
        speech_stag_str=''
        location_stag_str=''
        shotangle_stag_str=''
        color_stag_str=''
        # programPar_stag_str=''
        momPar_stag_str=''

        for i in ageDataTypeList:
            age_stag_str=age_stag_str+i.rsplit(',')[0]+','

        for i in momParDataTypeList:
            momPar_stag_str=momPar_stag_str+i.rsplit(',')[0]+','

        # for i in programParDataTypeList:
        #     programPar_stag_str=programPar_stag_str+i.rsplit(',')[0]+','

        for i in colorDataTypeList:
            color_stag_str=color_stag_str+i.rsplit(',')[0]+','

        for i in shotangleDataTypeList:
            shotangle_stag_str=shotangle_stag_str+i.rsplit(',')[0]+','

        for i in speechDataTypeList:
            speech_stag_str=speech_stag_str+i.rsplit(',')[0]+','

        for i in charDataTypeList:
            char_stag_str=char_stag_str+i.rsplit(',')[0]+','

        for i in locationDataTypeList:
            location_stag_str=location_stag_str+i.rsplit(',')[0]+','



        final_age=age_stag_str.rsplit(',',1)[0]
        final_char=char_stag_str.rsplit(',',1)[0]
        final_speech=speech_stag_str.rsplit(',',1)[0]
        final_location=location_stag_str.rsplit(',',1)[0]
        final_shotangle=shotangle_stag_str.rsplit(',',1)[0]
        final_color=color_stag_str.rsplit(',',1)[0]
        # final_programPar=programPar_stag_str.rsplit(',',1)[0]
        final_momPar=momPar_stag_str.rsplit(',',1)[0]




        # content_to_write_char='create external table character ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/character/";'.format(final_char)
        # content_to_write_age='create external table agegender ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/agegender/";'.format(final_age)
        # content_to_write_speech='create external table speech ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/speech/";'.format(final_speech)
        # content_to_write_location='create external table location ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/location/";'.format(final_location)
        # content_to_write_shotangle='create external table shotangle ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/shotangle/";'.format(final_shotangle)
        # # content_to_write_color='create external table color ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/color/";'.format(final_color)
        # content_to_write_program_data='create external table program_data ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/program_data/";'.format(final_programPar)
        # content_to_write_mom_data='create external table mom_data ({}) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," STORED AS PARQUET LOCATION "gs://b-refined-zone/mom_data/";'.format(final_momPar)

        
        agef= open("/tmp/create_age.hive","w+")
        agef.write(self.create_table_query('agegender', final_age, refined_bucket))
        agef.close()

        speechf= open("/tmp/create_speech.hive","w+")
        speechf.write(self.create_table_query('speech', final_speech, refined_bucket))
        speechf.close()

        charf= open("/tmp/create_character.hive","w+")
        charf.write(self.create_table_query('character', final_char, refined_bucket))
        charf.close()

        locationf= open("/tmp/create_location.hive","w+")
        locationf.write(self.create_table_query('location', final_location, refined_bucket))
        locationf.close()

        shotanglef= open("/tmp/create_shotangle.hive","w+")
        shotanglef.write(self.create_table_query('shotangle', final_shotangle, refined_bucket))
        shotanglef.close()

        colorf= open("/tmp/create_color.hive","w+")
        colorf.write(self.create_table_query('color', final_color, refined_bucket))
        colorf.close()

        mom_dataf= open("/tmp/create_mom_data.hive","w+")
        mom_dataf.write(self.create_table_query('mom_data', final_momPar, refined_bucket))
        mom_dataf.close()

        # program_dataf= open("/tmp/create_program_data.hive","w+")
        # program_dataf.write(self.create_table_query('program_data', final_programPar, refined_bucket))
        # program_dataf.close()


if __name__ == '__main__':
    soruce_bucket = 'b-refined-zone'
    CreateHiveTable().trigger_process('b-refined-zone')
