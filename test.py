from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from schemaGenerator import generate2020GeoSegmentSchema, generateSegment1Schema, generate2020And2010Segment2Schema, generate2000Segment2Schema
from pygen_census import gen_schema
from dotenv import load_dotenv
import os
import sys
from specified_columns.select_specified_columns import select_specified_columns
load_dotenv()

output_path = os.getenv("OUTPUT_PATH", "")

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

data = spark.read.parquet(f"{output_path}/output")

data.show()