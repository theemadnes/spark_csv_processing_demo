# simple way to launch this is '/usr/bin/spark-submit ./spark_csv_processing.py'

# import critical modules
from __future__ import print_function # support python 2.7 & 3
from pyspark import SparkContext
import datetime

# define params
s3_target_bucket_name = 'mattsona-public'

sc = SparkContext()

# load data file from s3
print('Loading file from S3...')
source_file = sc.textFile('s3://mattsona-public/spark_demo_data/mock-data-01.csv')
# split each line on ',' as this is a csv
lines = source_file.map(lambda x: x.split(','))

# create filter RDDs to show 'Male' & 'Female' sets and save to S3
time_stamp = datetime.datetime.isoformat(datetime.datetime.now()).replace(':','_') # create a datestamp for both RDDs to be saved
female_rdd = lines.filter(lambda x: x[4] == 'Female')
# female_rdd = source_file.map(lambda x: x.split(',')).filter(lambda x: lines[4] == 'Female')
male_rdd = lines.filter(lambda x: x[4] == 'Male')

female_rdd_to_s3 = female_rdd.saveAsTextFile('s3://' + s3_target_bucket_name + '/spark_csv_processing/' + time_stamp + '/female')
male_rdd_to_s3 = male_rdd.saveAsTextFile('s3://' + s3_target_bucket_name + '/spark_csv_processing/' + time_stamp + '/male')
