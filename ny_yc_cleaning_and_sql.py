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

#Create the Spark Session

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Place Parquet file in a dataframe
trips_df = spark.read\
                .option("header", "true") \
                .option("inferSchema", "true") \
                .parquet("./tmp/ny_yc_trip_data.parquet")
print(trips_df.printSchema())


#Create a temp table and view table counts
trips_df.registerTempTable("trips")
print("Original count: "+str(trips_df.count()))

#Check for Duplicate rows and drop them
def check_duplicates(data):

    distinctDF = data.distinct()
    print("Distinct count: "+str(distinctDF.count()))
    



    df2 = data.dropDuplicates()
    print("Distinct count: "+str(df2.count()))
    return(df2)


#place filtered data into new dataframe
distinct_df= check_duplicates(trips_df)



#Dropping sketchy data

def drop_unreal(y = distinct_df):

    dropDisDF = distinct_df.dropDuplicates(["VendorID","tpep_pickup_datetime","tpep_dropoff_datetime"])
    print("Distinct count: "+str(dropDisDF.count()))

    df_filtered=dropDisDF.filter(dropDisDF.trip_distance>0)
    print("df_filtered count: "+str(df_filtered.count()))

    df_filtered = df_filtered.dropna()
    print("df_filtered count: "+str(df_filtered.count()))


    df_filtered_Fare=df_filtered.filter(df_filtered.fare_amount>0)
    print("df_filtered_Fare count: "+str(df_filtered_Fare.count()))

    return(df_filtered)

final_filtered_df = drop_unreal()   

#saving the clean data in a new parquet file
final_filtered_df.write.parquet('./tmp/ny_yc_trip_data_cleansed.parquet')

