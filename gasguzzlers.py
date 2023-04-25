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

    def TransactionFilter(line):
        try:
            fields = line.split(',')
            if len(fields)!=15:
                return False
            float(fields[9])
            float(fields[11])
            return True
        except:
            return False

    def contractsFilter(line):
        try:
            fields = line.split(',')
            if len(fields)!=6:
                return False
            else:
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
    contracts = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/contracts.csv")

    
    trans = transactions.filter(TransactionFilter)
    cons = contracts.filter(contractsFilter)
    transf = trans.map(lambda l: ( time.strftime("%m/%Y",time.gmtime(int(l.split(',')[11]))) ,(float(l.split(',')[9]),1)))
    reducingT = transf.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    avg = reducingT.sortByKey(ascending=True)
    averageGasPrice = avg.map(lambda a: (a[0], str(a[1][0]/a[1][1]))) 
    

    trans1 = trans.map(lambda l: (l.split(',')[6] ,(time.strftime("%m/%Y",time.gmtime(int(l.split(',')[11]))), float(l.split(',')[8]))))
    contf1 = cons.map(lambda x: (x.split(',')[0],1))
    joins = trans1.join(contf1)
    mapping = joins.map(lambda x: (x[1][0][0], (x[1][0][1],x[1][1])))
    reducingT1 = mapping.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    averageGasUsed = reducingT1.map(lambda a: (a[0], str(a[1][0]/a[1][1])))
    averageGasUsed = averageGasUsed.sortByKey(ascending = True)
    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    my_result_object = my_bucket_resource.Object(s3_bucket,'gasg/avgGasprice.txt')
    my_result_object.put(Body=json.dumps(averageGasPrice.take(100)))               
    my_result_object1 = my_bucket_resource.Object(s3_bucket,'gasg/avgGasused.txt')
    my_result_object1.put(Body=json.dumps(averageGasUsed.take(100)))
    
    spark.stop()
    