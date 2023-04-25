import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def TransactionFilter(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            
            float(fields[7])
            return True
        except:
            return False

    

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
    
    
    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    trans = transactions.filter(TransactionFilter)
    trans_map = trans.map(lambda x: ((x.split(',')[5], x.split(',')[6]),1))
    output = trans_map.reduceByKey(operator.add)
    
    output=output.filter(lambda l: l[1] > 1000)
    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    
    my_result_object = my_bucket_resource.Object(s3_bucket,'test/wash.txt')
    my_result_object.put(Body=json.dumps(output.take(999999)))

    
    spark.stop()
    