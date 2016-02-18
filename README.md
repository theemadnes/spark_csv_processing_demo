# spark_csv_processing_demo

Taking a simple data set from https://www.mockaroo.com/ to perform some basic Spark processing transforms to / from S3.
It will take a data set that has female and male entries, filter them in to separate RDDs, and then sort the results.
Once that's done, it'll store the sorted results to S3. 

Notes:
- This uses PySpark, and was testing using Spark 1.5.2 on EMR.
- The data set referenced is based in the us-west-2 region.
- Test using spark-submit
