# Imports 
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

from pyspark.sql import SQLContext
from pyspark.sql import types 
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import *
from pyspark.sql.functions import *

from pyspark.sql import Row
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import isnan, when, col, round 
import databricks.koalas as ks

#Create the Spark Session

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)




#Create a custom schema for be applied to the new file
customSchema = StructType([
        StructField("VendorID", ShortType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", ShortType(), True),
        StructField("trip_distance", DecimalType(), True),
        StructField("RatecodeID", ShortType(), True),
        StructField("store_and_fwd_flag", BooleanType(), True),
        StructField("PULocationID", ShortType(), True),
        StructField("DOLocationID", ShortType(), True),
        StructField("payment_type", ShortType(), True),
        StructField("fare_amount", DecimalType(), True),   
        StructField("extra", DecimalType(), True),
        StructField("mta_tax", DecimalType(), True),
        StructField("tip_amount", DecimalType(), True),
        StructField("tolls_amount", DecimalType(), True),
        StructField("improvement_surcharge", DecimalType(), True),
        StructField("total_amount", DecimalType(), True),
        StructField("congestion_surcharge", DecimalType(), True),
])

spark.read.schema(customSchema).parquet("./tmp/ny_yc_trip_data_cleansed.parquet")
df_parquet = spark.read.parquet("./tmp/ny_yc_trip_data_cleansed.parquet")

#Applying that schema to the parquet file and casting to the correct data types
def cast_data_types(y = df_parquet):
    df_parquet = y.select(
        col('VendorID').cast(ShortType()),
        col('tpep_pickup_datetime').cast(TimestampType()),
        col('tpep_dropoff_datetime').cast(TimestampType()),
        col('passenger_count').cast(ShortType()),
        col('trip_distance').cast(DecimalType()),
        col('RatecodeID').cast(ShortType()),
        col('store_and_fwd_flag').cast(DecimalType()),
        col('PULocationID').cast(ShortType()),
        col('DOLocationID').cast(ShortType()),
        col('payment_type').cast(ShortType()),
        col('fare_amount').cast(DecimalType()),
        col('extra').cast(DecimalType()),
        col('mta_tax').cast(DecimalType()),
        col('tip_amount').cast(DecimalType()),
        col('tolls_amount').cast(DecimalType()),
        col('improvement_surcharge').cast(DecimalType()),
        col('total_amount').cast(DecimalType()),
        col('congestion_surcharge').cast(DecimalType()),
        
    )

    print(df_parquet.printSchema())
    return(df_parquet)

table_data = cast_data_types()

table_data.repartition(1).write.parquet('./tmp/ny_yc_trip_data_casted2.parquet')




