from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensusParquetAnalysis").getOrCreate()

# Paths to HDFS directories
base_path = "hdfs:///user/dirname/census_data_parquet"

df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")

# Print schema
df_2000.printSchema()
df_2010.printSchema()
df_2020.printSchema()

df_2000.show(5)
df_2010.show(5)
df_2020.show(5)
