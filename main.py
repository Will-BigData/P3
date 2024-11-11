from pathlib import Path
from pyspark.sql import SparkSession

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

geoheader = spark.read.csv("p3/Alabama/algeo2020.pl", sep='|')
file1 = spark.read.csv("p3/Alabama/al000012020.pl", sep='|')

with open("NotesToSelf/output.txt", "w") as file:
     file.write(file1._show_string())
file1.printSchema()

spark.stop()