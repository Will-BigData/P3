from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os
load_dotenv()

main_path = os.getenv('FOLDER_PATH', '')
print(main_path)

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

geo_df = spark.read.csv(f'{main_path}/GeoHeader', sep='|')
geo_df.filter(col(geo_df.columns[1])=='US').show()

