from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensusParquetAnalysis").getOrCreate()

# Paths to HDFS directories
base_path = "hdfs:///user/dirname/census_data_parquet"

df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")

# Print schema
df_2000.printSchema()

df_2000.show(5)

