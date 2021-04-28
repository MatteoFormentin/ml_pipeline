from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
import joblib

ES_HOST = "http://localhost"
INDEX_NAME = "siae-pm"
MODEL_PATH = "scripts/spark-ml/ann_model.joblib"

# Init Spark session - add elasticsearch-spark-20_2.12-7.12.0.jar to provide Elasticsearch itegration
spark = SparkSession.builder.config(
    'spark.driver.extraClassPath', 'scripts/elasticsearch-spark-20_2.12-7.12.0.jar').appName("ANN-Model-1.0").getOrCreate()

# Read the full dataset from Elasticsearch
reader = spark.read.format("org.elasticsearch.spark.sql").option("es.read.metadata", "true").option(
    "es.nodes.wan.only", "true").option("es.port", "9200").option("es.net.ssl", "false").option("es.nodes", "http://localhost")
df = reader.load(INDEX_NAME)

# Broadcast the dataframe that contains 3 new feautures
thr = spark.sparkContext.broadcast(pd.read_csv(
    "scripts/spark-ml/Project_good_modulation.csv", low_memory=False))

# Broadcast the ML model
ann_model = spark.sparkContext.broadcast(joblib.load(MODEL_PATH))

# Value for scaling input values
mean = spark.sparkContext.broadcast(np.array([0.70447761,  38.56467662,   7.73880597,  25.00945274,  24.39900498,
                                              -66.55522388, -71.47363184,  20.40149254,  20.21641791, -60.65572139,
                                              -64.79004975,  43.63383085,   9.00995025,  25.37661692,  24.69402985,
                                              -67.51094527, -73.96119403,  20.81094527,  20.72835821, -61.87363184,
                                              - 67.11293532,  55.81393035,  11.18656716,  25.54129353,  24.34278607,
                                              - 67.58557214, -78.19154229,  20.6681592,   20.34577114, -61.85522388,
                                              -71.10298507, -75.57711443,  20.86467662, -42.36666667, -84.43283582]))

std = spark.sparkContext.broadcast(np.array([0.45627723, 123.57359298,  35.18979014,  24.79695476,  26.46595483,
                                             30.5354078,   30.02765808,  13.74593555,  14.91493055,  24.25004241,
                                             24.26291933, 126.16662855,  38.79644417,  25.28163651,  27.27465636,
                                             30.51920739,  29.67238169,  14.35554497,  16.86584,     24.65059214,
                                             24.57174857, 135.94802272,  41.07536477,  24.95222373,  28.68671768,
                                             30.16953573,  28.84897419,  13.40055806,  19.03718133,  23.87716149,
                                             24.45482872,   7.01377633,   2.73917517,   9.16365215,   2.10866877]))


#  Windowed dataframe schema
schema = StructType([
    StructField("idlink", LongType(), True),
    StructField("dataN-2", TimestampType(), True),
    StructField("dataN-1", TimestampType(), True),
    StructField("dataN", TimestampType(), True),
    StructField("eqtype", LongType(), True),
    StructField("acmLowerMode", LongType(), True),
    StructField("freqband", StringType(), True),
    StructField("bandwidth", FloatType(), True),
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
    StructField("lowthr", FloatType(), True),
    StructField("ptx", LongType(), True),
    StructField("RxNominal", LongType(), True),
    StructField("Thr_min", LongType(), True)
    # StructField("_metadata", MapType(StringType(), StringType(), True), True)
])

# This must correspond with the above schema for success conversion from pd to spark


# Input-> a dataframe that contains a group (one link), out-> windowed df
@F.pandas_udf(schema, functionType=F.PandasUDFType.GROUPED_MAP)
def make_window(df_grouped):
    columns = np.array(['idlink', 'dataN-2', 'dataN-1', 'dataN', 'eqtype', 'acmLowerMode',
                        'freqband', 'bandwidth', 'acmEngine', 'esN-2', 'sesN-2', 'txMaxAN-2', 'txminAN-2', 'rxmaxAN-2', 'rxminAN-2',
                        'txMaxBN-2', 'txminBN-2', 'rxmaxBN-2', 'rxminBN-2', 'esN-1', 'sesN-1', 'txMaxAN-1', 'txminAN-1',
                        'rxmaxAN-1', 'rxminAN-1', 'txMaxBN-1', 'txminBN-1', 'rxmaxBN-1', 'rxminBN-1', 'esN', 'sesN',
                        'txMaxAN', 'txminAN', 'rxmaxAN', 'rxminAN', 'txMaxBN', 'txminBN', 'rxmaxBN', 'rxminBN',
                        'lowthr', 'ptx', 'RxNominal', 'Thr_min'
                        ])

    # Cast data column to datetime else cause problem with pyarrow
    df_grouped['data'] = pd.to_datetime(df_grouped['data'], dayfirst=True)
    chunksize = 3  # window dim
    # Create a Dataframe for 45-minutes window
    df_window = pd.DataFrame(data=None, columns=columns)

    # ------------------------------------------------
    # 1 - MAKE WINDOWS - Based on Windows_dataset.py
    # ------------------------------------------------

    for i in range(0, len(df_grouped) - 2):
        # get 3 rows of the original df
        chunk = df_grouped.iloc[range(i, i + chunksize)]
        chunk.index = range(0, chunksize)
        # check that the row in position 2 suffers at least one second of UAS and that all three records belong to same link anmd ramo
        if (chunk.iloc[0]['idlink'] == chunk.iloc[1]['idlink']) and (chunk.iloc[1]['idlink'] == chunk.iloc[2]['idlink']) \
                and (chunk.iloc[2]['uas'] != 0) and (chunk.iloc[0]['ramo'] == chunk.iloc[1]['ramo']) \
                and (chunk.iloc[1]['ramo'] == chunk.iloc[2]['ramo']):
            # This wll also reorder the dataframe
            data = [[
                chunk.iloc[0]['idlink'],
                chunk.iloc[0]['data'],
                chunk.iloc[1]['data'],
                chunk.iloc[2]['data'],
                chunk.iloc[2]['eqtype'],
                chunk.iloc[2]['acmLowerMode'],
                chunk.iloc[2]['freqband'],
                chunk.iloc[2]['bandwidth'],
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
                None,  # lowthr placeholder
                None,  # ptx placeholder
                chunk.iloc[0]['RxNominal'],
                None  # Thr_min placeholder
                # chunk.iloc[0]['_metadata']
            ]]

            # Create a one row dataframe
            wind = pd.DataFrame(data=data, columns=columns)

            # --------------------------------------------------------------------------------------------------
            # 2 - SET CORRECT freqband (data provided by links have wrong value) - Based on Windows_dataset.py
            # --------------------------------------------------------------------------------------------------

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

            # ------------------------------------------------------
            # 3 - ENCODE acmLowerMode - Based on Windows_dataset.py
            # NB: Performed directly on current created window
            # ------------------------------------------------------

            if wind.iloc[0]['acmLowerMode'] == "BPSK-Normal":
                wind.at[0, 'acmLowerMode'] = 0

            elif wind.iloc[0]['acmLowerMode'] == "4QAM-Strong" or wind.iloc[0]['acmLowerMode'] == "acm-4QAMs":
                wind.at[0, 'acmLowerMode'] = 1

            elif wind.iloc[0]['acmLowerMode'] == "4QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-4QAM":
                wind.at[0, 'acmLowerMode'] = 2

            elif wind.iloc[0]['acmLowerMode'] == "8PSK-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-8PSK":
                wind.at[0, 'acmLowerMode'] = 3

            elif wind.iloc[0]['acmLowerMode'] == "16QAM-Strong" or wind.iloc[0]['acmLowerMode'] == "acm-16QAMs":
                wind.at[0, 'acmLowerMode'] = 4

            elif wind.iloc[0]['acmLowerMode'] == "16QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-16QAM":
                wind.at[0, 'acmLowerMode'] = 5

            elif wind.iloc[0]['acmLowerMode'] == "32QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-32QAM":
                wind.at[0, 'acmLowerMode'] = 6

            elif wind.iloc[0]['acmLowerMode'] == "64QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-64QAM":
                wind.at[0, 'acmLowerMode'] = 7

            elif wind.iloc[0]['acmLowerMode'] == "128QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-128QAM":
                wind.at[0, 'acmLowerMode'] = 8

            elif wind.iloc[0]['acmLowerMode'] == "256QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-256QAM":
                wind.at[0, 'acmLowerMode'] = 9

            elif wind.iloc[0]['acmLowerMode'] == "512QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-512QAM":
                wind.at[0, 'acmLowerMode'] = 10

            elif wind.iloc[0]['acmLowerMode'] == "1024QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-1024QAM":
                wind.at[0, 'acmLowerMode'] = 11

            elif wind.iloc[0]['acmLowerMode'] == "2048QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-2048QAM":
                wind.at[0, 'acmLowerMode'] = 12

            elif wind.iloc[0]['acmLowerMode'] == "4096QAM-Normal" or wind.iloc[0]['acmLowerMode'] == "acm-4096QAM":
                wind.at[0, 'acmLowerMode'] = 13

            # ------------------------------------------------------------
            # 4 - ADD 3 NEW FEAUTURE - Based onadd_the_3_new_features.py
            # NB: Performed directly on current created window
            # ------------------------------------------------------------

            # Locate the rows in file that correspond to current equipment (x will contain indexes == position)
            x = thr.value.index[(thr.value['EQTYPE'] == wind.iloc[0]['eqtype'])
                                & (thr.value['FREQBAND'] == wind.iloc[0]['freqband'])
                                & (thr.value['LOWERMOD'] == wind.iloc[0]['acmLowerMode'])
                                & (thr.value['BANDWITDH'] == wind.iloc[0]['bandwidth'])].tolist()

            if len(x) != 0:  # Check if there is a corrispondence in the project file (ONLY one correspondence) -> if not discard record (not append)
                # Take the maximum Tx power
                # Assign the minimum Rx power threshold
                wind.at[0, 'lowthr'] = float(thr.value['PTH'][x])
                # Assign the maximum Tx power
                wind.at[0, 'ptx'] = thr.value['PTX'][x].astype("int64")

                # Take the index of the project element with the same characteristics of the considered window
                # EQTYPE-FREQBAND-BANDWIDTH
                x = thr.value.index[(thr.value['EQTYPE'] == wind.iloc[0]['eqtype'])
                                    & (thr.value['FREQBAND'] == wind.iloc[0]['freqband'])
                                    & (thr.value['BANDWITDH'] == wind.iloc[0]['bandwidth'])]

                # Take all the project data indicated by the indexes
                w = thr.value.iloc[x]
                # Select and assign the min Rx power Threshold correlated to the lowest configurable modulation
                wind.at[0, 'Thr_min'] = int(
                    w.loc[w['LOWERMOD'] == min(w['LOWERMOD'])]['PTH'])
                # Append the window to the df
                df_window = df_window.append(wind)

    return df_window


# Define Pandas UDF
@F.pandas_udf(returnType=DoubleType(), functionType=F.PandasUDFType.SCALAR)
def predict(*cols):
    # Columns are passed as a tuple of Pandas Series'.
    # Combine into a Pandas DataFrame
    windows = pd.concat(cols, axis=1)
    windows.columns = np.array(['acmEngine', 'esN-2', 'sesN-2', 'txMaxAN-2', 'txminAN-2', 'rxmaxAN-2', 'rxminAN-2',
                                'txMaxBN-2', 'txminBN-2', 'rxmaxBN-2', 'rxminBN-2', 'esN-1', 'sesN-1', 'txMaxAN-1', 'txminAN-1',
                                'rxmaxAN-1', 'rxminAN-1', 'txMaxBN-1', 'txminBN-1', 'rxmaxBN-1', 'rxminBN-1', 'esN', 'sesN',
                                'txMaxAN', 'txminAN', 'rxmaxAN', 'rxminAN', 'txMaxBN', 'txminBN', 'rxmaxBN', 'rxminBN',
                                'lowthr', 'ptx', 'RxNominal', 'Thr_min'
                                ])
    '''for col in windows.columns:
        print(col)'''

    tx = ['txMaxAN-2', 'txminAN-2', 'txMaxBN-2', 'txminBN-2', 'txMaxAN-1', 'txminAN-1',
          'txMaxBN-1', 'txminBN-1', 'txMaxAN', 'txminAN', 'txMaxBN', 'txminBN']
    # Fill the Nan value in the Tx power features with 100
    windows[tx] = windows[tx].fillna(100)
    # Fill the Nan value in the Rx power features with -150.
    # After we filled tx values, we know from the database that the only NAN values will be the rx ones
    windows = windows.fillna(-150)
    windows = (windows.to_numpy() - mean.value) / std.value

    pred = ann_model.value.predict(windows)

    # Take the index where is present the value 1, that identify the predicted label
    pred = np.argmax(pred, axis=1)

    # Return Pandas Series of predictions.
    return pd.Series(pred)


print("------------------------------------------------------------")
print("--------------------- PREPROCESSING ------------------------")
print("------------------------------------------------------------")

# Order by id asc, ramo asc, data asc
preprocessed_df = df.orderBy(df.idlink.asc(), df.ramo.asc(), df.data.asc()) \
    .groupBy(df.idlink) \
    .apply(make_window)

preprocessed_df.show(truncate=False)


print("------------------------------------------------------------")
print("----------------------- PREDICTION -------------------------")
print("------------------------------------------------------------")

predicted_df = preprocessed_df.withColumn("predictions", predict(col('acmEngine'), col(
    'esN-2'), col('sesN-2'), col('txMaxAN-2'), col('txminAN-2'), col('rxmaxAN-2'), col('rxminAN-2'),
    col('txMaxBN-2'), col('txminBN-2'), col('rxmaxBN-2'), col('rxminBN-2'), col(
    'esN-1'), col('sesN-1'), col('txMaxAN-1'), col('txminAN-1'),
    col('rxmaxAN-1'), col('rxminAN-1'), col('txMaxBN-1'), col('txminBN-1'), col(
        'rxmaxBN-1'), col('rxminBN-1'), col('esN'), col('sesN'),
    col('txMaxAN'), col('txminAN'), col('rxmaxAN'), col('rxminAN'), col(
    'txMaxBN'), col('txminBN'), col('rxmaxBN'), col('rxminBN'),
    col('lowthr'), col('ptx'), col('RxNominal'), col('Thr_min')))


predicted_df.write.format('csv').option('header', True).mode(
    'overwrite').option('sep', ',').save('predicted_dataset.csv')
predicted_df.show()
'''preprocessed_df.write.format('csv').option('header', True).mode(
    'overwrite').option('sep', ',').save('preprocessed_dataset.csv')'''


# TODO: Check predictions with idlink 287
