
# 1. What is the total population by race across the United States, and how does it change over time from 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusRaceAnalysis").getOrCreate()

# path for data
base_path = "hdfs:///user/dejtes/census_data_parquet"


df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")
