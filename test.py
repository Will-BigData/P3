from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, max, sum, format_number, col, count
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

data = spark.read.parquet(output_path)



#data = data.select("YEAR", "STUSAB", "SUMLEV", col("POP100").cast("int"), "REGION", "COUNTY")
data = data.select("SUMLEV", col("POP100").cast("int"))
total = data.count()
print(total)
data = data.groupBy("SUMLEV").count().orderBy("count", ascending=False).withColumn("Percent", format_number((col("count")*100/total),4))

#data = data.groupBy("YEAR", "STUSAB").agg(max("POP100").alias("Largest"), (sum("POP100")).alias("Total")).withColumn("Total", format_number("Total", 0))

data.show(n=data.count())
