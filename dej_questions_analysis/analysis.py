from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CensusParquetAnalysis").getOrCreate()

base_path = "hdfs:///user/dejtes/census_data_parquet"