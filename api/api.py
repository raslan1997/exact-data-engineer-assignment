import flask
from flask import request, jsonify , render_template, session, redirect
import simplejson
from decimal import Decimal
# Imports 
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import time

from pyspark.sql import SQLContext
from pyspark.sql import types 
from pyspark.sql.functions import isnan, when, count, col, round
from pyspark import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import json
from json import dumps
from flask_jsonpify import jsonpify

from pyspark.sql.types import DoubleType






app = flask.Flask(__name__)
app.config["DEBUG"] = True
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


df_parquet = spark.read.parquet("./tmp/ny_yc_trip_data_cleansed.parquet")


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
df_parquet.createOrReplaceTempView("parquetTable")








@app.route('/', methods=['GET'])
def home():
    return "<h1>Distant Reading Archive</h1><p>This site is a prototype API for Viewing Data.</p>"






@app.route('/api/tip/2020/01/max', methods=['GET'])
def api_Call():
    #Getting the Difference in Time in SECs and Maximum Distance in Meters and Sorting them Descendingly
    
    data = spark.sql("SELECT b as Quarter , MAX(d) as DROPOff_Location_ID, CAST(MAX(a)*100 as Numeric(4,2) ) as AVERAGE_TIP_Percentage\n"
    "  from (SELECT DOLocationID as d, QUARTER(tpep_dropoff_datetime) as b ,AVG(tip_amount/total_amount) as a  FROM parquetTable GROUP BY  d, b ORDER BY a DESC) \n"
    "GROUP BY b ORDER BY b ASC ;" )
 
    p_data = data.toPandas()
    print(p_data.head(10))
    
    JSONP_data = p_data.to_json()
    return JSONP_data

 






@app.route('/api/speed/2020/01/01/max', methods=['GET'])
def api_Call2():
    data = spark.sql("SELECT h , AVG(speed)  as AVERAGE_SPEED from(SELECT EXTRACT(HOUR FROM tpep_dropoff_datetime ) as h, trip_distance *1.6 /(( (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime))/3600 ))  as speed FROM parquetTable )GROUP BY h ORDER by AVERAGE_SPEED DESC ")
    
    p_data = data.toPandas()
    print(p_data.head(10))
    
    JSONP_data = p_data.to_json()
    return JSONP_data
app.run()