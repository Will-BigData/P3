# 3. How does the racial distribution vary between metropolitan and micropolitan areas from 2000-2020?
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusMetroMicroAnalysis").getOrCreate()

base_path = "hdfs:///user/dirname/census_data_parquet"

df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")


columns_of_interest = [
    "SUMLEV",
    "P0010003",
    "P0010004",
    "P0010005",
    "P0010006",
    "P0010007",
    "P0010008",
    "P0010009",
]