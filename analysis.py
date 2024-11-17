import os
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

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
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS project3_db")


hdfs_path_2000 = "hdfs://localhost:9000/user/hive/warehouse/census_data/YEAR=2000/*/*.snappy.parquet"
hdfs_path_2010 = "hdfs://localhost:9000/user/hive/warehouse/census_data/YEAR=2010/*/*.snappy.parquet"
hdfs_path_2020 = "hdfs://localhost:9000/user/hive/warehouse/census_data/YEAR=2020/*/*.snappy.parquet"
hdfs_path_state_codes = "hdfs://localhost:9000/user/hive/warehouse/state_codes.csv"

df_2000 = spark.read.parquet(hdfs_path_2000)
df_2010 = spark.read.parquet(hdfs_path_2010)
df_2020 = spark.read.parquet(hdfs_path_2020)
state_codes_df = spark.read.csv(hdfs_path_state_codes, header=True)

spark.sql("USE project3_db")
# df_2000.write.mode("overwrite").saveAsTable("project3_db.census_data_2000")
# df_2010.write.mode("overwrite").saveAsTable("project3_db.census_data_2010")
# df_2020.write.mode("overwrite").saveAsTable("project3_db.census_data_2020")
# state_codes_df.write.mode("overwrite").saveAsTable("project3_db.state_codes")

'''
##########################################################
# Question 1

# state codes: https://www.census.gov/library/reference/code-lists/ansi/ansi-codes-for-states.html
df_2000_select = df_2000.select("pop100", "sumlev", "state" )
df_2000_filtered = df_2000_select.filter((col('sumlev') == '050') & (col('state') != '72'))
population_2000 = df_2000_filtered.agg(F.sum(df_2000_filtered.pop100)).collect()[0][0]
print(f"Total Population 2000: {population_2000}")


df_2010_select = df_2010.select("pop100", "sumlev", "state" )
df_2010_filtered = df_2010_select.filter((col('sumlev') == '050') & (col('state') != '72'))
population_2010 = int(df_2010_filtered.agg(F.sum(df_2010_filtered.pop100)).collect()[0][0])
print(f"Total Population 2010: {population_2010}")

df_2020_select = df_2020.select("pop100", "sumlev", "state" )
df_2020_filtered = df_2020_select.filter((col('sumlev') == '050') & (col('state') != '72'))
population_2020 = int(df_2020_filtered.agg(F.sum(df_2020_filtered.pop100)).collect()[0][0])
print(f"Total Population 2020: {population_2000}")

growth2010 = ((population_2010 - population_2000) / population_2000) * 100
print(f"Population Growth from 2000 to 2010: {round(growth2010, 2)}%")


growth2020 = ((population_2020 - population_2010) / population_2010) * 100
print(f"Population Growth from 2010 to 2020: {round(growth2020, 2)}%")

##########################################################
'''
# Question 2
state_codes_broadcast = F.broadcast(state_codes_df)

df_2000_select = df_2000.select('pop100', 'sumlev', 'state')
df_2000_filtered = df_2000_select.filter((F.col('sumlev') == '050') & (F.col('state') != '72'))
df_2000_groupby = df_2000_filtered.groupBy('state').agg(F.sum('pop100').alias('population'))
df_2000_sorted = df_2000_groupby.sort(F.desc('population'))
df_2000_with_names = df_2000_sorted.join(state_codes_broadcast, on='state', how='left')
df_2000_with_names = df_2000_with_names.select('state_name', 'population')
df_2000_with_names.show()


df_2010_select = df_2010.select('pop100', 'sumlev', 'state')
df_2010_filtered = df_2010_select.filter((F.col('sumlev') == '050') & (F.col('state') != '72'))
df_2010_groupby = df_2010_filtered.groupBy('state').agg(F.sum('pop100').alias('population'))
df_2010_sorted = df_2010_groupby.sort(F.desc('population'))
df_2010_with_names = df_2010_sorted.join(state_codes_broadcast, on='state', how='left')
df_2010_with_names = df_2010_with_names.select('state_name', 'population')
df_2010_with_names.show()

df_2020_select = df_2020.select('pop100', 'sumlev', 'state')
df_2020_filtered = df_2020_select.filter((F.col('sumlev') == '050') & (F.col('state') != '72'))
df_2020_groupby = df_2020_filtered.groupBy('state').agg(F.sum('pop100').alias('population'))
df_2020_sorted = df_2020_groupby.sort(F.desc('population'))
df_2020_with_names = df_2020_sorted.join(state_codes_broadcast, on='state', how='left')
df_2020_with_names = df_2020_with_names.select('state_name', 'population')
df_2020_with_names.show()

df_growth = df_2010_with_names.alias("df_2010") \
    .join(df_2000_with_names.alias("df_2000"), on="state_name", how="inner") \
    .join(df_2020_with_names.alias("df_2020"), on="state_name", how="inner") \
    .select(
        F.col("state_name"),
        F.col("df_2000.population").alias("population_2000"),
        F.col("df_2010.population").alias("population_2010"),
        F.col("df_2020.population").alias("population_2020")
    )

df_growth = df_growth.withColumn(
    "growth_2000_2010",
    F.round(((F.col("population_2010") - F.col("population_2000")) / F.col("population_2000")) * 100, 2)
).withColumn(
    "growth_2010_2020",
    F.round(((F.col("population_2020") - F.col("population_2010")) / F.col("population_2010")) * 100, 2))


df_growth_sorted = df_growth.sort(F.desc("growth_2000_2010"))

df_growth_sorted.show(truncate=False)
