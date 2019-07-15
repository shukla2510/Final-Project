import sys
import datetime
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Extarct Feature Data').getOrCreate()
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.functions import *
from pyspark.sql.functions import lit

class FeatureDataExtract:
    # raw_zone_bucket = 'b-raw-zone'
    def get_frame_df(self, file_name, raw_zone_bucket):
        json_name = file_name.rsplit('.',1)[0].rsplit('/',1)[1]
        video = spark.read.json(file_name)
        raw_zone_path = 'gs://{}/{}/'.format(raw_zone_bucket, json_name)
        video.write.parquet(raw_zone_path)
        videoPar = spark.read.parquet(raw_zone_path)
        scenedf = videoPar.select(explode(videoPar.VIDEO.SCENE).alias('SCENE'))
        cameradf = scenedf.select(explode(scenedf.SCENE.CAMERA_CUT).alias('CAMERA_CUT'))
        framedf = cameradf.select(explode(cameradf.CAMERA_CUT.FRAME).alias('FRAME')).select('FRAME.*')
        return framedf, json_name
    



    def update_time_stamp(col):
        col1 = col.rsplit('.',1)[0]
        return col1
    
    update_time_stamp_udf = F.UserDefinedFunction(update_time_stamp, T.StringType())
    
    def get_starttime(self, framedf):
        starttimedf = framedf.select('STARTTIME')
        starttimedf = starttimedf.withColumn("id", monotonically_increasing_id())
        start_time=starttimedf.withColumn('START_TIME', self.update_time_stamp_udf(starttimedf.STARTTIME))
        start_time = start_time.drop('STARTTIME')
        return start_time
    
    def get_endtime(self, framedf):
        endtimedf = framedf.select('ENDTIME')
        endtimedf = endtimedf.withColumn("id", monotonically_increasing_id())
        end_time = endtimedf.withColumn('END_TIME', self.update_time_stamp_udf(endtimedf.ENDTIME))
        end_time = end_time.drop('ENDTIME')
        return end_time

    def add_time(col,json_name):
        HHMMSS = [json_name.rsplit('.')[0].rsplit('_',1)[1][i:i+2] for i in range(6) if i%2==0]
        col_final = col.split(':')
        s = datetime.timedelta(hours=int(col_final[0]), minutes=int(col_final[1]), seconds=int(col_final[2])) +  datetime.timedelta(hours=int(HHMMSS[0]), minutes=int(HHMMSS[1]), seconds=int(HHMMSS[2]))
        return str(s)

    add_time_function = F.UserDefinedFunction(add_time, T.StringType())
    
    def character(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        chardf= framedf.select(explode(framedf.CHARACTER).alias('CHARACTER')).select('CHARACTER.*')
        characterDF= chardf.withColumn('DLIB_CONFIDENCE',chardf.DLIB_CONFIDENCE)\
        .withColumn('EMBEDDING',chardf.EMBEDDING)\
        .withColumn('CONFIDENCE',chardf.PREDICTION.CONFIDENCE)\
        .withColumn('LABEL',chardf.PREDICTION.LABEL)\
        .withColumn('WIDTH',chardf.QUALITY_PARAMETER.WIDTH)\
        .withColumn('HEIGHT',chardf.QUALITY_PARAMETER.HEIGHT)\
        .withColumn('PIXEL_COUNT',chardf.QUALITY_PARAMETER.PIXEL_COUNT)\
        .withColumn('CHANNEL_NUMBER', lit(channel_number))\
        .drop('PREDICTION','QUALITY_PARAMETER')
        
        characterDF=characterDF.withColumn("id", monotonically_increasing_id())
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        characterDF = characterDF.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return characterDF
    
    def location(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        Locationdf= framedf.select(explode(framedf.LOCATION.PREDICTION).alias('Location')).select('Location.*')
        Locationdf = Locationdf.withColumn('LOCATION_CONFIDENCE',Locationdf.CONFIDENCE)\
        .withColumn('LOCATION_LABEL',Locationdf.LABEL)\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn('CHANNEL_NUMBER', lit(channel_number))\
        .drop('CONFIDENCE','LABEL')
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        
        Locationdf = Locationdf.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return Locationdf
    
    def shotangle(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        shotangledf= framedf.select(explode(framedf.SHOT_ANGLE.PREDICTION).alias('SHOT_ANGLE')).select('SHOT_ANGLE.*')
        shotangledf = shotangledf.withColumn('SHOT_ANGLE_CONFIDENCE', shotangledf.CONFIDENCE)\
        .withColumn('SHOT_ANGLE_LABEL', shotangledf.LABEL)\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn('CHANNEL_NUMBER', lit(channel_number))\
        .drop('CONFIDENCE','LABEL')
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        shotangledf = shotangledf.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return shotangledf
    
    def speech(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        speechdf= framedf.select(explode(framedf.SPEECH.PREDICTION).alias('SPEECH')).select('SPEECH.*')
        speechdf = speechdf.withColumn('SPEECH_CONFIDENCE', speechdf.CONFIDENCE)\
        .withColumn('SPEECH_LABEL',speechdf.LABEL)\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn('CHANNEL_NUMBER', lit(channel_number))\
        .drop('CONFIDENCE','LABEL')
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        speechdf = speechdf.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return speechdf
    
    def agegender(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        agedf= framedf.select(explode(framedf.AGE_GENDER).alias('AGE_GENDER')).select('AGE_GENDER.*')
        ageGenderDF= agedf.withColumn('CONFIDENCE',agedf.PREDICTION.CONFIDENCE)\
        .withColumn('LABEL',agedf.PREDICTION.LABEL)\
        .withColumn('WIDTH',agedf.QUALITY_PARAMETER.WIDTH)\
        .withColumn('HEIGHT',agedf.QUALITY_PARAMETER.HEIGHT)\
        .withColumn('PIXEL_COUNT',agedf.QUALITY_PARAMETER.PIXEL_COUNT)\
        .withColumn("id", monotonically_increasing_id())\
        .withColumn('CHANNEL_NUMBER', lit(channel_number))\
        .drop('PREDICTION','QUALITY_PARAMETER')
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        ageGenderDF = ageGenderDF.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return ageGenderDF
    
    def color(self, framedf, json_name):
        channel_number = json_name.rsplit('_')[1]
        colordf= framedf.select(framedf.COLOR).select('COLOR.*')
        colordf = framedf.select(explode(framedf.COLOR.COLOR_LUMINANCE).alias('COLOR_LUMINANCE')).select('COLOR_LUMINANCE.*')
        colordf1 = framedf.select(explode(framedf.COLOR.COLOR_PALETTE).alias('COLOR_PALETTE')).select('COLOR_PALETTE.*')
        colordf2 = framedf.select(explode(framedf.COLOR.COLOR_TEMPERATURE).alias('COLOR_TEMPERATURE')).select('COLOR_TEMPERATURE.*')
        colordfCL = colordf.withColumn('COLOR_LUMINANCE_CONFIDENCE', colordf.CONFIDENCE)\
        .withColumn('COLOR_LUMINANCE_LABEL', colordf.LABEL)\
        .withColumn("id", monotonically_increasing_id())\
        .drop('CONFIDENCE', 'LABEL')
        colordfCP = colordf1.withColumn('COLOR_PALETTE_CONFIDENCE', colordf1.CONFIDENCE)\
        .withColumn('COLOR_PALETTE_LABEL', colordf1.LABEL)\
        .withColumn('COLOR_PALETTE_RGB', colordf1.RGB)\
        .withColumn("id", monotonically_increasing_id())\
        .drop('CONFIDENCE', 'LABEL', 'RGB')
        colordfCT = colordf2.withColumn('COLOR_TEMPERATURE_CONFIDENCE', colordf2.CONFIDENCE)\
        .withColumn('COLOR_TEMPERATURE_LABEL', colordf2.LABEL)\
        .withColumn("id", monotonically_increasing_id())\
        .drop('CONFIDENCE', 'LABEL')
        
        colordfCL = colordfCL.join(colordfCP, ['id'])
        colordfCL = colordfCL.join(colordfCT, ['id'])
        start_time = self.get_starttime(framedf)
        end_time = self.get_endtime(framedf)
        colordfCL = colordfCL.join(start_time, ['id'])
        colordfCL = colordfCL.join(end_time, ['id'])
        def my_func(col):
            col1 = '{}_{}'.format(col, json_name)
            return col1
        my_udf = F.UserDefinedFunction(my_func, T.StringType())
        unique_id=start_time.withColumn('unique_id', my_udf(start_time.START_TIME))
        start_time=start_time.withColumn('START_TIME_MOM', self.add_time_function(start_time.START_TIME, lit(json_name)))
        unique_id=unique_id.drop('STARTTIME')
        unique_id=unique_id.drop('START_TIME')
        end_time = end_time.drop('ENDTIME')
        start_time = start_time.drop('STARTTIME')
        colordfCL = colordfCL.withColumn('CHANNEL_NUMBER', lit(channel_number)).drop('START_TIME')
        colordfCL = colordfCL.drop('END_TIME')
        colordfCL = colordfCL.join(start_time,['id']).join(end_time,['id']).join(unique_id,['id']).drop('id')
        return colordfCL

        
if __name__ == '__main__':
    file_name = sys.argv[1:][0]
    raw_zone_bucket = sys.argv[1:][1]
    refined_zone_bucket = sys.argv[1:][2]
    # file_name = 'gs://b-landing-zone/20181207_5_173003.000.json'
    framedf, json_name = FeatureDataExtract().get_frame_df(file_name, raw_zone_bucket)
    chardf = FeatureDataExtract().character(framedf, json_name)
    chardf.write.mode('append').parquet('gs://{}/character/'.format(refined_zone_bucket))
    locdf = FeatureDataExtract().location(framedf, json_name)
    locdf.show()
    locdf.write.mode('append').parquet('gs://{}/location/'.format(refined_zone_bucket))
    shotdf = FeatureDataExtract().shotangle(framedf, json_name)
    shotdf.write.mode('append').parquet('gs://{}/shotangle/'.format(refined_zone_bucket))
    speechdf = FeatureDataExtract().speech(framedf, json_name)
    speechdf.write.mode('append').parquet('gs://{}/speech/'.format(refined_zone_bucket))
    agedf = FeatureDataExtract().agegender(framedf, json_name)
    agedf.write.mode('append').parquet('gs://{}/agegender/'.format(refined_zone_bucket))
    colordf = FeatureDataExtract().color(framedf, json_name)
    colordf.write.mode('append').parquet('gs://{}/color/'.format(refined_zone_bucket))
