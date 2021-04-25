from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np
#from sklearn.externals import joblib

ES_HOST = "http://localhost"
INDEX_NAME = "siae-pm"
MODEL_PATH = "ann_model.joblib"
spark = SparkSession.builder.config(
    'spark.driver.extraClassPath', 'scripts/elasticsearch-spark-20_2.12-7.12.0.jar').appName("ANN-Model-1.0").getOrCreate()


reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option(
    "es.nodes.wan.only", "true").option("es.port", "9200").option("es.net.ssl", "false").option("es.nodes", "http://localhost")
df = reader.load(INDEX_NAME)


#  Windowed schema
schema = StructType([
    StructField("idlink", LongType(), True),
    StructField("ramo", LongType(), True),
    StructField("IP_A", StringType(), True),
    StructField("IP_B", StringType(), True),
    StructField("dataN-2", TimestampType(), True),
    StructField("dataN-1", TimestampType(), True),
    StructField("dataN", TimestampType(), True),
    StructField("eqtype", LongType(), True),
    StructField("acmLowerMode", StringType(), True),
    StructField("freqband", FloatType(), True),
    StructField("bandwidth", FloatType(), True),
    StructField("RxNominal", LongType(), True),
    StructField("acmEngine", LongType(), True),
    StructField("esN-2", LongType(), True),
    StructField("sesN-2", LongType(), True),
    StructField("txMaxAN-2", FloatType(), True),
    StructField("txminAN-2", FloatType(), True),
    StructField("rxmaxAN-2", FloatType(), True),
    StructField("rxminAN-2", FloatType(), True),
    StructField("txMaxBN-2", FloatType(), True),
    StructField("txminBN-2", FloatType(), True),
    StructField("rxmaxBN-2", FloatType(), True),
    StructField("rxminBN-2", FloatType(), True),
    StructField("esN-1", LongType(), True),
    StructField("sesN-1", LongType(), True),
    StructField("txMaxAN-1", FloatType(), True),
    StructField("txminAN-1", FloatType(), True),
    StructField("rxmaxAN-1", FloatType(), True),
    StructField("rxminAN-1", FloatType(), True),
    StructField("txMaxBN-1", FloatType(), True),
    StructField("txminBN-1", FloatType(), True),
    StructField("rxmaxBN-1", FloatType(), True),
    StructField("rxminBN-1", FloatType(), True),
    StructField("esN", LongType(), True),
    StructField("sesN", LongType(), True),
    StructField("txMaxAN", FloatType(), True),
    StructField("txminAN", FloatType(), True),
    StructField("rxmaxAN", FloatType(), True),
    StructField("rxminAN", FloatType(), True),
    StructField("txMaxBN", FloatType(), True),
    StructField("txminBN", FloatType(), True),
    StructField("rxmaxBN", FloatType(), True),
    StructField("rxminBN", FloatType(), True),
    StructField("_metadata", MapType(StringType(), StringType(), True), True)])

columns = np.array(['idlink', 'ramo', 'IP_A', 'IP_B', 'dataN-2', 'dataN-1', 'dataN', 'eqtype', 'acmLowerMode',
                    'freqband', 'bandwidth',
                    'RxNominal', 'acmEngine', 'esN-2', 'sesN-2', 'txMaxAN-2', 'txminAN-2', 'rxmaxAN-2', 'rxminAN-2',
                    'txMaxBN-2', 'txminBN-2', 'rxmaxBN-2', 'rxminBN-2', 'esN-1', 'sesN-1', 'txMaxAN-1', 'txminAN-1',
                    'rxmaxAN-1', 'rxminAN-1', 'txMaxBN-1', 'txminBN-1', 'rxmaxBN-1', 'rxminBN-1', 'esN', 'sesN',
                    'txMaxAN', 'txminAN', 'rxmaxAN', 'rxminAN', 'txMaxBN', 'txminBN', 'rxmaxBN', 'rxminBN'])


# Input-> a dataframe that contains a group (one link), out-> windowed df
@F.pandas_udf(schema, functionType=F.PandasUDFType.GROUPED_MAP)
def make_window(df_grouped):
    chunksize = 3  # windows dim
    # Create a Dataframe explaining the 45-minutes window
    df_window = pd.DataFrame(data=None, columns=columns)
    for i in range(0, len(df_grouped)-2):  # Loop over the rows of the file
        chunk = df_grouped.iloc[range(i, i+chunksize)]
        chunk.index = range(0, chunksize)
        # check that the row in position 2 suffers at least one second of UAS
        if (chunk.iloc[0]['idlink'] == chunk.iloc[1]['idlink']) and (chunk.iloc[1]['idlink'] == chunk.iloc[2]['idlink']) \
                and (chunk.iloc[2]['uas'] != 0) and (chunk.iloc[0]['ramo'] == chunk.iloc[1]['ramo']) \
                and (chunk.iloc[1]['ramo'] == chunk.iloc[2]['ramo']):
            # This wll also reorder the dataframe
            data = [[chunk.iloc[0]['idlink'],
                     chunk.iloc[0]['ramo'],
                     chunk.iloc[0]['ip_a'],
                     chunk.iloc[0]['ip_b'],
                     chunk.iloc[0]['timestamp'],
                     chunk.iloc[1]['timestamp'],
                     chunk.iloc[2]['timestamp'],
                     chunk.iloc[2]['eqtype'],
                     chunk.iloc[2]['acmLowerMode'],
                     chunk.iloc[2]['freqband'],
                     chunk.iloc[2]['bandwidth'],
                     chunk.iloc[0]['RxNominal'],
                     chunk.iloc[2]['acmEngine'],

                     chunk.iloc[0]['es'],
                     chunk.iloc[0]['ses'],
                     chunk.iloc[0]['txMaxA'],
                     chunk.iloc[0]['txminA'],
                     chunk.iloc[0]['rxmaxA'],
                     chunk.iloc[0]['rxminA'],
                     chunk.iloc[0]['txMaxB'],
                     chunk.iloc[0]['txminB'],
                     chunk.iloc[0]['rxmaxB'],
                     chunk.iloc[0]['rxminB'],

                     chunk.iloc[1]['es'],
                     chunk.iloc[1]['ses'],
                     chunk.iloc[1]['txMaxA'],
                     chunk.iloc[1]['txminA'],
                     chunk.iloc[1]['rxmaxA'],
                     chunk.iloc[1]['rxminA'],
                     chunk.iloc[1]['txMaxB'],
                     chunk.iloc[1]['txminB'],
                     chunk.iloc[1]['rxmaxB'],
                     chunk.iloc[1]['rxminB'],

                     chunk.iloc[2]['es'],
                     chunk.iloc[2]['ses'],
                     chunk.iloc[2]['txMaxA'],
                     chunk.iloc[2]['txminA'],
                     chunk.iloc[2]['rxmaxA'],
                     chunk.iloc[2]['rxminA'],
                     chunk.iloc[2]['txMaxB'],
                     chunk.iloc[2]['txminB'],
                     chunk.iloc[2]['rxmaxB'],
                     chunk.iloc[2]['rxminB']]]

            df_window.append(data)

    return df_window


df.orderBy(df.idlink.asc(), df.ramo, df.timestamp.desc()) \
    .groupBy(df.idlink) \
    .apply(make_window) \
    .show(truncate=False)


'''
# Load classifier and broadcast to executors.
clf = spark.broadcast(joblib.load(MODEL_PATH))

# Define Pandas UDF
@F.pandas_udf(returnType=DoubleType(), functionType=F.PandasUDFType.SCALAR)
def predict(*cols):
    # Columns are passed as a tuple of Pandas Series'.
    # Combine into a Pandas DataFrame
    X = pd.concat(cols, axis=1)

    # Make predictions and select probabilities of positive class (1).
    predictions = clf.value.predict_proba(X)[:, 1]

    # Return Pandas Series of predictions.
    return pd.Series(predictions)

# Make predictions on Spark DataFrame.
df = df.withColumn("predictions", predict(*feature_cols))
'''

'''
StructType([
    StructField("@timestamp", TimestampType()(), True),
    StructField("@version", StringType()(), True),
    StructField("RxNominal", LongType(), True),
    StructField("acmEngine", LongType(), True),
    StructField("acmLowerMode", StringType(), True),
    StructField("acmMax", FloatType(), True),
    StructField("acmSnA", FloatType(), True),
    StructField("acmSnB", FloatType(), True),
    StructField("acmUpperMode", StringType(), True),
    StructField("area", LongType(), True),
    StructField("bandwidth", FloatType(), True),
    StructField("eqtype", LongType(), True),
    StructField("es", LongType(), True),
    StructField("freqband", FloatType(), True),
    StructField("idlink", LongType(), True),
    StructField("ip_a", StringType(), True),
    StructField("ip_b", StringType(), True),
    StructField("isG828Ok", LongType(), True),
    StructField("isRxPwrOk", LongType(), True),
    StructField("isTxPwrOk", LongType(), True),
    StructField("protection", StringType(), True),
    StructField("provider", LongType(), True),
    StructField("ramo", LongType(), True),
    StructField("rxRef", LongType(), True),
    StructField("rxSnA", FloatType(), True),
    StructField("rxSnB", FloatType(), True),
    StructField("rxmaxA", FloatType(), True),
    StructField("rxmaxB", FloatType(), True),
    StructField("rxminA", FloatType(), True),
    StructField("rxminB", FloatType(), True),
    StructField("ses", LongType(), True),
    StructField("snA", FloatType(), True),
    StructField("snB", FloatType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("txMaxA", FloatType(), True),
    StructField("txMaxB", FloatType(), True),
    StructField("txRef", FloatType(), True),
    StructField("txminA", FloatType(), True),
    StructField("txminB", FloatType(), True),
    StructField("uas", LongType(), True),
    StructField("_metadata", MapType(StringType(), StringType(), True), True)])
'''
