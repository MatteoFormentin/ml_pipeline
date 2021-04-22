#
#   spark_rdd_ex.py
#   Example of running spark RDD api
#   RDD api are the low level spark api. An rdd is an immutable distributed set of contains unstructured tuples. 
#   The basic operations that can be performed on RDD are based functional programming like filter/map/reduce.
#   Since RDD are immutable operations that outputsnew tuples set output a new rdd (reduce instead is an actions that return a single value)
#

import pyspark

ES_HOST = "http://localhost"
INDEX_NAME = "siae-pm"

# Get the SparkContext -> Used to refer to spark RDD module
# NB: SparkContext is created by the library when imported
conf = pyspark.SparkConf().setAppName('Test').setMaster("local").set(
    'spark.driver.extraClassPath', 'scripts/elasticsearch-hadoop-7.12.0.jar')
sc = pyspark.SparkContext(conf=conf)

# Connect to ES and create an RDD from an index
# Elasticsearch only supports data retrive as RDD
rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
                         "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable",
                         conf={"es.resource": INDEX_NAME, "es.nodes": ES_HOST, "es.port": "9200",
                               "es.nodes.client.only": "false", "es.nodes.wan.only": "true"})

# Print one element of the RDD
print(rdd.first())


def m(a):  # Map gets only one param
    out = a[1]['txMaxA']
    if out == None:
        out = 0
    return out


def red(a, b):  # Reduce gets two param
    return (a + b)/2


# NB: Map Reduce works only on RDD
# map -> apply the same func to each element of the dataset, save the results in an intermediate dataset
res = rdd.map(m)
# reduce -> Apply the func to the map results, elaborating a pair at time till one value remain
out = res.reduce(red)


print(out)

print("Done")
