import os
from pyspark.sql import Row, SparkSession

namenode_host = "localhost"
port = "9000"
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.hadoop.fs.defaultFS", f"hdfs://{namenode_host}:{port}") \
    .config("spark.sql.hive.metastore.version", "3.1.3") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://localhost:9083") \
 \
    .config("spark.sql.hive.metastore.jars", "maven") \
    .config("spark.hadoop.hive.exec.scratchdir", "/tmp/hive") \
    .config("spark.hadoop.hive.metastore.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hadoop.hive.site", os.path.join(os.environ['HIVE_HOME'], "conf/hive-site.xml")) \
 \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS project3_db")

# file_path = os.path.join(os.getcwd(), "extracted_data/census_data/YEAR=2010")
# df = spark.read.csv(f"file://{file_path}", header=True, inferSchema=True)
# hdfs_path = "user/hive/warehouse/census_data/YEAR=2010/*/*.snappy.parquet"
# hdfs_path = "hdfs://localhost:9000/user/hive/warehouse/census_data/YEAR=2010/*/*.snappy.parquet"
hdfs_path = "hdfs://localhost:9000/user/hive/warehouse/census_data/YEAR=2010/STUSAB=FL/*.snappy.parquet"
df = spark.read.parquet(hdfs_path)
df.show(5)
print(df.count())
# recursive_loaded_df = spark.read.format("parquet") \
#     .option("recursiveFileLookup", "true") \
#     .load("hdfs://localhost:9000/data")

# todo figure out how to read and combine several files together
