import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
if __name__ == "__main__":
    
    #Bucket Auth
    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()
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
    

    def BlockFilter(line):
        try:
            fields = line.split(',')
            if len(fields) == 19 and fields[1] != 'hash':
                str(fields[9])
                int(fields[12])
                return True
            else:
                return False
        except:
            return False
    
    blocks = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv")
    clean_lines=blocks.filter(BlockFilter)
    blocksFiltered = clean_lines.map(lambda l: (str(l.split(',')[9]),int(l.split(',')[12])))
    blockReduce = blocksFiltered.reduceByKey(operator.add)
    top10Min = blockReduce.takeOrdered(10, key=lambda l: -l[1])
    
    
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'partc/top10Mners.txt')
    my_result_object.put(Body=json.dumps(top10Min))
    
    spark.stop()
    
    