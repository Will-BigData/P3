from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from schemaGenerator import generate2020GeoSegmentSchema, generateSegment1Schema, generate2020And2010Segment2Schema, generateSegment3Schema, generate2000Segment2Schema
from pygen_census import gen_schema
from dotenv import load_dotenv
import os
load_dotenv()

main_path = os.getenv('FOLDER_PATH', '')

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

geo_df = spark.read.csv(f'{main_path}/p3_data_2020/GeoHeader', sep='|', schema=generate2020GeoSegmentSchema()).drop("CIFSN").drop("CHARITER")
seg1_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment1', sep='|', schema=generateSegment1Schema()).drop("CIFSN").drop("CHARITER")
seg2_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment2', sep='|', schema=generate2020And2010Segment2Schema()).drop("CIFSN").drop("CHARITER")
seg3_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment3', sep='|', schema=generateSegment3Schema()).drop("CIFSN").drop("CHARITER")

link_cols = ["FILEID", "STUSAB", "LOGRECNO"]
combined_df_2020 = geo_df.join(seg1_df, link_cols).join(seg2_df, link_cols).join(seg3_df, link_cols)

geo_df_2010 = gen_schema(spark.read.text(f"{main_path}/p3_data_2010/GeoHeader"), spark, 2010)
seg1_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment1', sep=',', schema=generateSegment1Schema())
seg2_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment2', sep=',', schema=generate2020And2010Segment2Schema())

geo_df_2000 = gen_schema(spark.read.text(f"{main_path}/p3_data_2000/GeoHeader"), spark, 2000)
seg1_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment1', sep=',', schema=generateSegment1Schema())
seg2_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment2', sep=',', schema=generate2000Segment2Schema())

geo_df_2000.show()
# print(seg2_df.filter(col(geo_df.columns[1])=='US').head())
# seg1_df_2000.show()

