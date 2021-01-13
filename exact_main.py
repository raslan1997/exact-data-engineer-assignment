import csv
import pandas as pd 
import pyarrow
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from endpoints import Controller
import psycopg2
from download_ny import months

#initialize an empty dataframe 
df = pd.DataFrame()

#for each month in the months array in download_ny.py
#append each month csv to the dataframe then save it as a parquet file

for month in months:
    df = df.append(pd.read_csv("yellow_tripdata_2020-"+month+".csv"), ignore_index=True)
    print(df.tail(10))

df.to_parquet('./tmp/ny_yc_trip_data.parquet')
