###################################################################################################################
#  author : ENES ÇAVUŞ
#  subject : Python file for transforming Local Trends via PySpark
###################################################################################################################
                                                                                                                                                                                                                                             
from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.streaming.kafka import KafkaUtils 
import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
import unicodedata
import sys  
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType, IntegerType

reload(sys)  
sys.setdefaultencoding('utf-8')

# OLD STATIC - def consumer_to_spark_data_frame():
#     consumer = KafkaConsumer(bootstrap_servers='localhost:9092',value_deserializer=lambda m: json.loads(m.decode('utf-8')))
#     consumer.subscribe(['trends2'])

#     for message in consumer:
#         global ss
#         data = ["",0]
#         data[0] = unicodedata.normalize('NFKD', (message.value[0])).encode('ascii', 'ignore')
#         data[1] = int(message.value[1]) # trend rank
#         schema = StructType([StructField("topic", StringType()),StructField("rank", IntegerType())])
#         # schema=['topic', 'rank']
#         my_list = data
#         rdd = sc.parallelize([my_list])
#         df = ss.createDataFrame(rdd, schema=schema)
#         df.show()
#         df.write.option('nullValue', None).saveAsTable(name='default.trendsLocal', format='hive', mode="append")

# this function control RDD structures - create a specific rdd type with unique column names and types
# transfor RDD for parallelizing
# match schema and write these RDDs into specific hive table - one by one            
def handle_rdd(rdd):                                                                                                    
    if not rdd.isEmpty():                                                                                               
        global ss      
        schema = StructType([StructField("topic", StringType()),StructField("rank", StringType())])
        # print(type(rdd.take(1)))
        # print(rdd.take(1)[0])
        my_list = rdd.take(1)[0]
        rdd = sc.parallelize([my_list])
        df = ss.createDataFrame(rdd, schema=schema)
        df.show()                                                                                                       
        df.write.saveAsTable(name='default.trendslocal', format='hive', mode='append')    

# givin a name to app                                                                                                         
sc = SparkContext(appName="LocalTrend")
ssc = StreamingContext(sc, 1)      
# Hadoop hive connections between spark and hive - local file paths                                                                                                   
ss = SparkSession.builder.appName("LocalTrend").config("spark.sql.warehouse.dir", "/user/hive/warehouse").config("hive.metastore.uris", "thrift://localhost:9083").enableHiveSupport().getOrCreate()                                                                                                                                                                                                                         
ss.sparkContext.setLogLevel('WARN') 
# getting data from kafka stream DIRECTLY                                                                                                                        
getStream = KafkaUtils.createDirectStream(ssc, ['trends2'], {'metadata.broker.list': 'localhost:9092'})                       
readyToTransformation = getStream.map(lambda v: json.loads(v[1]))    
# Spark data transformations with RDD mapping functions                                                                                                                     
transformedData = readyToTransformation.map(lambda stream : (str(unicodedata.normalize('NFKD', (stream[0])).encode('ascii', 'ignore')),
                                                            str(stream[1])))                                                                                       
transformedData.foreachRDD(handle_rdd)  
# OLD - consumer_to_spark_data_frame()
ssc.start()                                                                                                             
ssc.awaitTermination()

# END - ENES ÇAVUŞ - Bitirme Projesi - SAU - Bahar 2021

