import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("myapp")\
        .getOrCreate()
    
    #Bucket Auth
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")  

    def validFields(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[7])
            int(fields[11])
            return True
        except:
            return False

    
    t = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = t.filter(validFields)
    date_vals = clean_lines.map(lambda l: (time.strftime("%B - %Y",time.gmtime(int(l.split(',')[11]))),(float(l.split(',')[7]),1))) 
    reducing = date_vals.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) 
    transactionsAvg = reducing.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    transactionsAvgFormated = transactionsAvg.map(lambda op: ','.join(str(tr) for tr in op))

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    my_result_object = my_bucket_resource.Object(s3_bucket,'parta2/average.txt')
    my_result_object.put(Body=json.dumps(transactionsAvgFormated.take(1000)))
    
    spark.stop()