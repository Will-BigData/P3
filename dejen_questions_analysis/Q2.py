# 2. WHAT ARE THE TOP 10 COUNTIES WITH THE HIGHEST DIVERSITY INDEX, AND HOW DOES IT CHANGE FROM 2000-2020?

from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("CensusDiversityIndex").getOrCreate()

# Base path for datasets
base_path = "hdfs:///user/dirname/census_data_parquet"

# Load datasets for 2000, 2010, and 2020
df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")


# Columns of interest for diversity analysis
columns_of_interest = [
    "STUSAB",
    "COUNTY",
    "SUMLEV",
    "P0010001",
    "P0010003",   
    "P0010004",   
    "P0010005",   
    "P0010006",  
    "P0010007",   
    "P0010008",   
    "P0010009",
]