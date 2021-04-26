from os import truncate
from numpy.core.numeric import False_
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import pandas as pd
import numpy as np
# from sklearn.externals import joblib

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
    StructField("acmLowerMode", LongType(), True),
    # TODO: Is a string or a float?
    StructField("freqband", StringType(), True),
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
    StructField("_metadata", MapType(StringType(), StringType(), True), True)
])

columns = np.array(['idlink', 'ramo', 'IP_A', 'IP_B', 'dataN-2', 'dataN-1', 'dataN', 'eqtype', 'acmLowerMode',
                    'freqband', 'bandwidth',
                    'RxNominal', 'acmEngine', 'esN-2', 'sesN-2', 'txMaxAN-2', 'txminAN-2', 'rxmaxAN-2', 'rxminAN-2',
                    'txMaxBN-2', 'txminBN-2', 'rxmaxBN-2', 'rxminBN-2', 'esN-1', 'sesN-1', 'txMaxAN-1', 'txminAN-1',
                    'rxmaxAN-1', 'rxminAN-1', 'txMaxBN-1', 'txminBN-1', 'rxmaxBN-1', 'rxminBN-1', 'esN', 'sesN',
                    'txMaxAN', 'txminAN', 'rxmaxAN', 'rxminAN', 'txMaxBN', 'txminBN', 'rxmaxBN', 'rxminBN', '_metadata'])


# Input-> a dataframe that contains a group (one link), out-> windowed df
@F.pandas_udf(schema, functionType=F.PandasUDFType.GROUPED_MAP)
def make_window(df_grouped):
    # Cast to datetime else cause problem with pyarrow
    df_grouped['data'] = pd.to_datetime(df_grouped['data'], dayfirst=True)

    chunksize = 3  # df_window dim
    # Create a Dataframe explaining the 45-minutes window
    df_window = pd.DataFrame(data=None, columns=columns)
    # Make the windows
    for i in range(0, len(df_grouped) - 2):  # Loop over the rows of the file
        # iloc gets rows of dataframeza

        chunk = df_grouped.iloc[range(i, i + chunksize)]
        chunk.index = range(0, chunksize)
        # check that the row in position 2 suffers at least one second of UAS
        if (chunk.iloc[0]['idlink'] == chunk.iloc[1]['idlink']) and (chunk.iloc[1]['idlink'] == chunk.iloc[2]['idlink']) \
                and (chunk.iloc[2]['uas'] != 0) and (chunk.iloc[0]['ramo'] == chunk.iloc[1]['ramo']) \
                and (chunk.iloc[1]['ramo'] == chunk.iloc[2]['ramo']):
            # This wll also reorder the dataframe
            data = [[
                chunk.iloc[0]['idlink'],
                chunk.iloc[0]['ramo'],
                chunk.iloc[0]['ip_a'],
                chunk.iloc[0]['ip_b'],
                chunk.iloc[0]['data'],
                chunk.iloc[1]['data'],
                chunk.iloc[2]['data'],
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
                chunk.iloc[2]['rxminB'],

                chunk.iloc[0]['_metadata']
            ]]

            # Create a one row dataframe
            wind = pd.DataFrame(data=data, columns=columns)

            # Set correct freqband (data provided by links have wrong value)
            # Cast freqband to str
            wind["freqband"] = wind.freqband.astype(int)  # round to int
            wind["freqband"] = wind.freqband.astype(str)

            if wind.iloc[0]['freqband'] == "19":
                wind.at[0, 'freqband'] = "18"
            elif wind.iloc[0]['freqband'] == "17":
                wind.at[0, 'freqband'] = "18"
            elif (wind.iloc[0]['freqband'] == "10") and (wind.iloc[i]['eqtype'] == 29 or wind.iloc[i]['eqtype'] == 27):
                wind.at[0, 'freqband'] = "11"
            elif wind.iloc[0]['freqband'] == "12":
                wind.at[0, 'freqband'] = "13"
            elif wind.iloc[0]['freqband'] == "22":
                wind.at[0, 'freqband'] = "23"
            elif wind.iloc[0]['freqband'] == "37":
                wind.at[0, 'freqband'] = "38"
            elif wind.iloc[0]['freqband'] == "41":
                wind.at[0, 'freqband'] = "42"
            elif wind.iloc[0]['freqband'] == "6":
                wind.at[0, 'freqband'] = "6U"

            df_window = df_window.append(wind)

        # Encode acmLowerMode in the windowed dataframe
        index = df_window.index[df_window['acmLowerMode'] == "BPSK-Normal"]
        df_window.loc[index, 'acmLowerMode'] = 0

        index = df_window.index[(df_window['acmLowerMode'] == "4QAM-Strong")
                                | (df_window['acmLowerMode'] == "acm-4QAMs")]
        df_window.loc[index, 'acmLowerMode'] = 1

        index = df_window.index[(df_window['acmLowerMode'] == "4QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-4QAM")]
        df_window.loc[index, 'acmLowerMode'] = 2

        index = df_window.index[(df_window['acmLowerMode'] == "8PSK-Normal")
                                | (df_window['acmLowerMode'] == "acm-8PSK")]
        df_window.loc[index, 'acmLowerMode'] = 3

        index = df_window.index[(df_window['acmLowerMode'] == "16QAM-Strong")
                                | (df_window['acmLowerMode'] == "acm-16QAMs")]
        df_window.loc[index, 'acmLowerMode'] = 4

        index = df_window.index[(df_window['acmLowerMode'] == "16QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-16QAM")]
        df_window.loc[index, 'acmLowerMode'] = 5

        index = df_window.index[(df_window['acmLowerMode'] == "32QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-32QAM")]
        df_window.loc[index, 'acmLowerMode'] = 6

        index = df_window.index[(df_window['acmLowerMode'] == "64QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-64QAM")]
        df_window.loc[index, 'acmLowerMode'] = 7

        index = df_window.index[(df_window['acmLowerMode'] == "128QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-128QAM")]
        df_window.loc[index, 'acmLowerMode'] = 8

        index = df_window.index[(df_window['acmLowerMode'] == "256QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-256QAM")]
        df_window.loc[index, 'acmLowerMode'] = 9

        index = df_window.index[(df_window['acmLowerMode'] == "512QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-512QAM")]
        df_window.loc[index, 'acmLowerMode'] = 10

        index = df_window.index[(df_window['acmLowerMode'] == "1024QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-1024QAM")]
        df_window.loc[index, 'acmLowerMode'] = 11

        index = df_window.index[(df_window['acmLowerMode'] == "2048QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-2048QAM")]
        df_window.loc[index, 'acmLowerMode'] = 12

        index = df_window.index[(df_window['acmLowerMode'] == "4096QAM-Normal")
                                | (df_window['acmLowerMode'] == "acm-4096QAM")]
        df_window.loc[index, 'acmLowerMode'] = 13

    return df_window


df.orderBy(df.idlink.asc(), df.ramo.asc(), df.data.desc()) \
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
