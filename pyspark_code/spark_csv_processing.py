# simple way to launch this is '/usr/bin/spark-submit ./spark_csv_processing.py'

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
import datetime

# define params
s3_target_bucket_name = 'mattsona-spark-demo'

sc = SparkContext() # create a spark context

# load data file from s3
print('Loading file from S3...')
source_file = sc.textFile('s3://mattsona-public/spark_demo_data/mock-data-01.csv')
# split each line on ',' as this is a csv
lines = source_file.map(lambda x: x.split(','))

# create filter RDDs to show 'Male' & 'Female' sets, sort them by last name, and save to S3
time_stamp = datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_') # create a datestamp for both RDDs to be saved
female_rdd = lines.filter(lambda x: x[4] == 'Female') # [4] is gender
sorted_female_rdd = female_rdd.sortBy(lambda x: x[2], True) # [2] is last name
male_rdd = lines.filter(lambda x: x[4] == 'Male') # [4] is gender
sorted_male_rdd = male_rdd.sortBy(lambda x: x[2], True) # [2] is last name

sorted_female_rdd_to_s3 = sorted_female_rdd.saveAsTextFile('s3://' + s3_target_bucket_name + '/spark_csv_processing/' + time_stamp + '/female')
sorted_male_rdd_to_s3 = sorted_male_rdd.saveAsTextFile('s3://' + s3_target_bucket_name + '/spark_csv_processing/' + time_stamp + '/male')
