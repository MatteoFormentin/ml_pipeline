import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import *
j = """{
   "snB":33.0,
   "snA":34.0,
   "protection":"2x1+0 Single Pipe",
   "isRxPwrOk":1,
   "acmSnA":8.0,
   "rxRef":-40,
   "freqband":19.0,
   "provider":1,
   "acmSnB":8.0,
   "@version":"1",
   "acmUpperMode":"acm-256QAM",
   "rxSnA":"None",
   "timestamp":"2021-04-09T09:55:25.545Z",
   "rxSnB":"None",
   "ses":0,
   "rxminB":-38.0,
   "area":1,
   "isG828Ok":1,
   "rxminA":-39.0,
   "ramo":1,
   "bandwidth":56.0,
   "uas":0,
   "acmEngine":1,
   "es":0,
   "eqtype":19,
   "RxNominal":-43,
   "txMaxA":19.0,
   "isTxPwrOk":1,
   "rxmaxB":-38.0,
   "acmMax":7.0,
   "@timestamp":"2021-04-09T09:55:25.545Z",
   "txminA":19.0,
   "txMaxB":19.0,
   "txminB":19.0,
   "txRef":"None",
   "rxmaxA":-39.0,
   "idlink":100047,
   "ip_a":"10.197.169.130",
   "acmLowerMode":"acm-16QAM",
   "ip_b":"10.197.169.132"
}"""


spark = SparkSession.builder.config(
    'spark.driver.extraClassPath', 'scripts/elasticsearch-hadoop-7.12.0.jar').appName("Schema-JSON").getOrCreate()

df = spark.read.json(spark.sparkContext.parallelize([j]))

with open("schema.json", "w") as f:
    json.dump(df.schema.jsonValue(), f)
