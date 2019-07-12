import sys
from pyspark.sql.types import *
from  pyspark.sql.functions import *
import functools
from pyspark.sql import SparkSession

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col


spark = SparkSession.builder.appName('Assignment_1').getOrCreate()


class Assignment_1:

# /home/hdfs/lumiq/aadhaar_data.csv
    @staticmethod
    def checkpoint_1_ques_1():
        # Load the data into Spark

        '''
        This method is used to load the data,
        we will use system arguments to get the path of data
        Here we have created the schema and using it we are creating aadhaar dataframe
        :return: Aadhaar dataframe
        '''

        data_path = sys.argv[1]
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("registrar", StringType(), True),
            StructField("private_agency", StringType(), True),
            StructField("state", StringType(), True),
            StructField("district", StringType(), True),
            StructField("sub_district", StringType(), True),
            StructField("pincode", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("age", StringType(), True),
            StructField("aadhaar_generated", IntegerType(), True),
            StructField("rejected", IntegerType(), True),
            StructField("mobile_number", StringType(), True),
            StructField("email_id", IntegerType(), True)])

        aadhaar_df = spark.read.csv(data_path, schema=schema)
        return aadhaar_df


    @staticmethod
    def checkpoint_1_ques_2(aadhaar_df):

        '''
        this method is used to View/result of the top 25 rows from each individual agency.
        Here we are getting the list of distinct dataframe of private_agency first
        after that we are looping to get desired result
        we are appending it to dfs list, so dfs is list of dataframes which contain 25 rows for each agency
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method

        '''

        # View/result of the top 25 rows from each individual agency.

        agen = aadhaar_df.select('private_agency').distinct().collect()
        dfs = []

        #for i in range(len(agen)):
        for i in range(3):
            dfs.append(aadhaar_df.where(aadhaar_df.private_agency == agen[i].private_agency).limit(25))

        unioned_df = Assignment_1.unionAll(dfs)
        folder=sys.argv[2]
        unioned_df.write.mode('overwrite').option("header","true").csv(folder+str('/assignment_1_checkpoint_1_ques_2/'))


    @staticmethod
    def unionAll(dfs):
        return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)

    @staticmethod
    def checkpoint_2_ques_1(aadhaar_df):
        aadhaar_df.printSchema()
        #   OR
        aadhaar_df.describe()


    @staticmethod
    def checkpoint_2_ques_2(aadhaar_df):

        '''
        Here we are finding the count and names of registrars in the table.
        we have used dataframe's groupby and count functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method

        '''
        # Find the count and names of registrars in the table.

        name = aadhaar_df.groupby('registrar').count()
        folder = sys.argv[2]
        name.write.mode('overwrite').option("header","true").csv(folder+str('/assignment_1_checkpoint_2_ques_2/'))


    @staticmethod
    def checkpoint_2_ques_3(aadhaar_df):

        '''
        First we are getting the number of states using distinct
        Then we are finding number of districts in each state
        then we are find numbering of sub-districts in each districts
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''

        # number of states
        aadhaar_df.select(aadhaar_df.state).distinct()

        # find number of districts in each state
        districtdin = aadhaar_df.select('district', 'state').distinct()
        distrinct_count = districtdin.select('district', 'state').where(districtdin.state == districtdin.state).groupby(
            'state').count().withColumnRenamed('count', 'district_count')
        folder = sys.argv[2]
        distrinct_count.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_2_ques_3/'))

        # find number of sub-districts in each districts
        sub_district=aadhaar_df.select('district','sub_district').distinct()
        sub_district_count = sub_district.select('district','sub_district').where(sub_district.district==sub_district.district).groupby('district').count().withColumnRenamed('count', 'sub_district_count')
        folder = sys.argv[2]
        sub_district_count.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_2_ques_3_sub_district/'))


    @staticmethod
    def checkpoint_2_ques_4(aadhaar_df):
        '''
        First getting distinct dataframe of private agency
        then we are finding the names of private agencies for each state
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''


        # Find out the names of private agencies for each state

        private_agency = aadhaar_df.select('private_agency', 'state').distinct()
        name = private_agency.select('private_agency', 'state').where(private_agency.state == private_agency.state).orderBy(
            'state')
        folder = sys.argv[2]
        name.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_2_ques_4/'))


    @staticmethod
    def checkpoint_3_ques_1(aadhaar_df):

        '''
        here we are finding top 3 states generating most number of Aadhaar cards
        using dataframe's select, groupby ,orderby, sum, limit functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''

        # Find top 3 states generating most number of Aadhaar cards?

        most_adhar= aadhaar_df.select(aadhaar_df.state, aadhaar_df.aadhaar_generated).groupBy(aadhaar_df.state).sum('aadhaar_generated').orderBy('sum(aadhaar_generated)',ascending=False).limit(3)
        folder = sys.argv[2]
        most_adhar.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_3_ques_1/'))

    @staticmethod
    def checkpoint_3_ques_2(aadhaar_df):
        '''
        here we are finding the top 3 districts where enrolment numbers are maximum
        using dataframe's select, groupby ,orderby, count, limit functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''

        # Find top 3 districts where enrolment numbers are maximum?

        most_enroll= aadhaar_df.select(aadhaar_df.district, aadhaar_df.email_id).groupBy(aadhaar_df.district).count().orderBy('count', ascending=False).limit(3)
        folder = sys.argv[2]
        most_enroll.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_3_ques_2/'))

    @staticmethod
    def checkpoint_3_ques_3(aadhaar_df):

        '''
        here we are finding no. of Aadhaar cards generated in each state
        using dataframe's select, groupby ,orderby, sum functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''

        # Find the no. of Aadhaar cards generated in each state?

        each_state= aadhaar_df.select(aadhaar_df.state, aadhaar_df.aadhaar_generated).groupBy(aadhaar_df.state).sum('aadhaar_generated').orderBy('sum(aadhaar_generated)',
                                                                                             ascending=False)
        folder = sys.argv[2]
        each_state.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_3_ques_3/'))

    @staticmethod
    def checkpoint_4_ques_1(aadhaar_df):

        '''
        here we are finding number of unique pin codes in the data
        using dataframe's select and distinct functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''


        # Find the number of unique pin codes in the data?
        pincode = aadhaar_df.select('pincode').distinct()


        folder = sys.argv[2]
        pincode.write.mode('overwrite').option("header", "true").csv(folder + str('/assignment_1_checkpoint_4_ques_1/'))


    @staticmethod
    def checkpoint_4_ques_2(aadhaar_df):

        '''
        here we are finding number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra
        using dataframe's select, where, groupby, isin and sum functions
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''


        # Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?


        rejected= aadhaar_df.select(aadhaar_df.state, aadhaar_df.rejected).where(aadhaar_df.state.isin(['Uttar Pradesh', 'Maharashtra'])).groupBy(aadhaar_df.state).sum(
        'rejected')


        folder = sys.argv[2]
        rejected.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_4_ques_2/'))


    @staticmethod
    def checkpoint_5_ques_1(aadhaar_df):
        '''
        here we are finding top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
        first we are creating the temp view named datatb
        Then using spark-SQL we are getting the desired results
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''


        # Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
        aadhaar_df.createOrReplaceTempView('datatb')

        three_states = spark.sql(
            "select state, sum(aadhaar_generated)/count(aadhaar_generated) *100 as adh_percent from datatb where gender='M' group by state order by adh_percent desc").limit(
                3)

        folder = sys.argv[2]
        three_states.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_5_ques_1/'))


    @staticmethod
    def checkpoint_5_ques_2(aadhaar_df):
        '''
        here we are finding in each of previous 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
        first we are creating the temp view named datatb
        Then using spark-SQL we are getting the desired results using winoow function and rank function
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''


        # Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
        aadhaar_df.createOrReplaceTempView('datatb')

        fr_query = spark.sql(
            "select first(state),  district, sum(rejected)/count(rejected) *100 as adh_percent from datatb where gender='F' and state  in ('Manipur','Tamil Nadu','Arunachal Pradesh') group by district order by adh_percent desc")

        window = Window.partitionBy(fr_query['first(state, false)']).orderBy(fr_query['adh_percent'].desc())

        female_rejected = fr_query.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 3).withColumnRenamed('first(state, false)', 'state')


        folder = sys.argv[2]
        female_rejected.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_5_ques_2/'))


    @staticmethod
    def checkpoint_5_ques_3(aadhaar_df):
        '''
        here we are finding summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
        first we are writing the dataframe in parquet format and creating the bucket on age column
        Then using spark-SQL we are getting the desired results
        then we are creating the temp view named newtb
        on that table we are applying aggregate function to get desired results
        then we are saving it given location in csv format
        :param aadhaar_df: This is dataframe we have creating in previous method
        :return:
        '''

        # Find the summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.


        aadhaar_df.write.format('parquet').bucketBy(10, 'age').saveAsTable('bucketage')
        query = spark.sql("select aadhaar_generated, rejected, (aadhaar_generated + rejected)as enrolled  from bucketage")
        query.createOrReplaceTempView('newtb')
        output = spark.sql("select (sum(aadhaar_generated)/sum(enrolled)*100)as percentOfAcceptance from newtb")

        folder = sys.argv[2]
        output.write.mode('overwrite').option("header", "true").csv(
            folder + str('/assignment_1_checkpoint_5_ques_3/'))




if __name__ == "__main__":

    # Load the data into Spark
    # Always need to run this method
    aadhaar_df = Assignment_1.checkpoint_1_ques_1()

    # View/result of the top 25 rows from each individual agency.
    Assignment_1.checkpoint_1_ques_2(aadhaar_df)

    # Describe the schema.
    # this method is commented because it shows only data on spark shell
    # Assignment_1.checkpoint_2_ques_1(aadhaar_df)

    # Find the count and names of registrars in the table.
    Assignment_1.checkpoint_2_ques_2(aadhaar_df)

    # Find the number of states, districts in each state and sub-districts in each district.
    Assignment_1.checkpoint_2_ques_3(aadhaar_df)

    # Find out the names of private agencies for each state
    Assignment_1.checkpoint_2_ques_4(aadhaar_df)

    # Find top 3 states generating most number of Aadhaar cards?
    Assignment_1.checkpoint_3_ques_1(aadhaar_df)

    # Find top 3 districts where enrolment numbers are maximum?
    Assignment_1.checkpoint_3_ques_2(aadhaar_df)

    # Find the no. of Aadhaar cards generated in each state?
    Assignment_1.checkpoint_3_ques_3(aadhaar_df)

    # Find the number of unique pin codes in the data?
    Assignment_1.checkpoint_4_ques_1(aadhaar_df)

    # Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?
    Assignment_1.checkpoint_4_ques_2(aadhaar_df)

    # Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
    Assignment_1.checkpoint_5_ques_1(aadhaar_df)

    # Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
    Assignment_1.checkpoint_5_ques_2(aadhaar_df)

    # Find the summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
    Assignment_1.checkpoint_5_ques_3(aadhaar_df)