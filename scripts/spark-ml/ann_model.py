from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


ES_HOST = "http://localhost"
INDEX_NAME = "siae-pm"

spark = SparkSession.builder.config(
    'spark.driver.extraClassPath', 'scripts/elasticsearch-spark-20_2.12-7.12.0.jar').appName("ANN-Model-1.0").getOrCreate()



reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option(
    "es.nodes.wan.only", "true").option("es.port", "9200").option("es.net.ssl", "false").option("es.nodes", "http://localhost")
df = reader.load(INDEX_NAME)
df.show()
