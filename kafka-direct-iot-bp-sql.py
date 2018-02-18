"""
 Processes direct stream from kafka, '\n' delimited text directly received 
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a 
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

from Crypto.PublicKey import RSA
from Crypto.Cipher import AES, PKCS1_OAEP
from ast import literal_eval
import base64
import json
import sys
import re

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import add

def get_private_key():
  PRIVATE_KEY_FILE_PATH = '/home/ec2-user/Key/private_key.pem'
  with open(PRIVATE_KEY_FILE_PATH, 'r') as file:
    return file.read()

def decrypt(encrypt_session_key_base64_enc, ciphertext_base64_enc):
  #Use a RSA private_key to decrypt AES encrypt_session_key
  private_key_string = get_private_key()
  private_key = RSA.importKey(private_key_string)
  cipher_rsa_private = PKCS1_OAEP.new(private_key)

  encrypt_session_key = base64.b64decode(encrypt_session_key_base64_enc)
  session_key = cipher_rsa_private.decrypt(encrypt_session_key)

  #Use base64 to decode ciphertext_base64_enc
  ciphertext_base64 = base64.b64decode(ciphertext_base64_enc)

  #Use AES to decrypt ciphertext_base64_dec[16:]
  IV = ciphertext_base64[:16]
  cipher_aes = AES.new(session_key, AES.MODE_CFB, IV)
  iotmsg = cipher_aes.decrypt(ciphertext_base64[16:])
  return iotmsg

def getSqlContextInstance(sparkContext):
  #if ( 'sqlContextSingletonInstance' not in globals()):
  globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
  return globals()['sqlContextSingletonInstance']
 
def process(time, rdd):
  #print("========== %s ==========" % str(time))
  if rdd.isEmpty():
    return
   
  # Get the singleton instance of SparkSession
  sqlContext = getSqlContextInstance(rdd.context)
  df = sqlContext.read.json(rdd)
  df.printSchema()
  df.registerTempTable('iotmsgsTable')
  sqlstring = 'select payload.data.systolic AS systolic, payload.data.diastolic AS diastolic from iotmsgsTable'

  try:
    df_bp = sqlContext.sql(sqlstring)
    #df_bp.show()
    df_bp_alias = df_bp.select(df_bp.systolic.alias('systolic'), df_bp.diastolic.alias('diastolic')).orderBy('systolic')
    print('There are %d blood pressure records as the table shows below.' % df_bp_alias.count() )
    df_bp_alias.show(n=100)

    # Filter, Hypotension 
    df_hypo = df_bp_alias.filter('systolic < 90 or diastolic < 60')
    print('There are %d hypotension records as the table shows below.' % df_hypo.count() )
    df_hypo.show(n=100)

    # Filter, Hypertension
    df_hyper = df_bp_alias.filter('systolic >= 140 or diastolic >= 90')
    print('There are %d hypertension records as the table shows below.' % df_hyper.count() )
    df_hyper.show(n=100)

    # Filter, Normal
    df_normal = df_bp_alias.filter('systolic >= 90 and systolic <= 139 and diastolic >=60 and diastolic <= 89')
    print('There are %d normal records as the table shows below.' % df_normal.count() )
    df_normal.show(n=100)

    # Clean-up
    sqlContext.dropTempTable('iotmsgsTable')
  
  except Exception as e:
    status = 'fail'
    print(e)
  else:
    status = 'success'
  print(status)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaBloodPressure")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)
    sc.setLogLevel("WARN")

    # Global variables
    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    # Read in the Kafka Direct Stream into a TransformedDStream
    encodedDStream = kvs.map(lambda (key, value): value)
    #encodedDStream.pprint()
    
    # Turn base64 encoded strings to tuples(encoded_session_key, encoded_ciphertext)
    encodedTuples = encodedDStream.map(lambda x: literal_eval(x))
    encodedTuples.pprint()
   
    # Deal with RSA, AES decrypt
    iotmsg_jsonDStream = encodedTuples.map(lambda (x,y): decrypt(x,y))
    iotmsg_jsonDStream.pprint()

    # Process each RDD of the DStream coming in from Kafka
    iotmsg_jsonDStream.foreachRDD(process)
 
    ssc.start()
    ssc.awaitTermination()

