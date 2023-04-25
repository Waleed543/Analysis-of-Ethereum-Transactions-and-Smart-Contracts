import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession

if __name__ == "__main__":

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
    
    

    def ValidTransactions(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            int(fields[3])
            return True
        except:
            return False


    def ValidContracts(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            else:
                return True
        except:
            return False

    
    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    trans = transactions.filter(ValidTransactions)
    cons = contracts.filter(ValidContracts)

    tfiltered = trans.map(lambda x: (x.split(',')[6], int(x.split(',')[7])))
    cfiltered = cons.map(lambda x: (x.split(',')[0],1))
    trans_reducing = tfiltered.reduceByKey(operator.add)
    joined = trans_reducing.join(cfiltered)
    AddressValue =joined.map(lambda x: (x[0], x[1][0]))
    top10 = AddressValue.takeOrdered(10, key=lambda l: -1*l[1])

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'partb/top10SmartContracts.txt')
    my_result_object.put(Body=json.dumps(top10))
    
    spark.stop()