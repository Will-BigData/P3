from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from schemaGenerator import generateGeoSegmentSchema, generateSegment1Schema, generateSegment2Schema, generateSegment3Schema
from dotenv import load_dotenv
import os
load_dotenv()

main_path = os.getenv('FOLDER_PATH', '')

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

geo_df = spark.read.csv(f'{main_path}/GeoHeader', sep='|', schema=generateGeoSegmentSchema())
seg1_df = spark.read.csv(f'{main_path}/Segment1', sep='|', schema=generateSegment1Schema())
seg2_df = spark.read.csv(f'{main_path}/Segment2', sep='|', schema=generateSegment2Schema())
seg3_df = spark.read.csv(f'{main_path}/Segment3', sep='|', schema=generateSegment3Schema())
print(seg2_df.filter(col(geo_df.columns[1])=='US').head())

