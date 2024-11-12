from schemaGenerator import generateGeoSegmentSchema, generateSegment1Schema, generateSegment2Schema, generateSegment3Schema
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("census-dataset")\
.config("spark.master", "local[*]")\
.getOrCreate()

geoSchema = generateGeoSegmentSchema()

geoData = spark.read.option("delimiter", "|").schema(geoSchema).csv("P3/geo")
segment1Data = spark.read.option("delimiter", "|").schema(geoSchema).csv("P3/segment1")
segment2Data = spark.read.option("delimiter", "|").schema(geoSchema).csv("P3/segment2")
segment3Data = spark.read.option("delimiter", "|").schema(geoSchema).csv("P3/segment3")

print(geoData.count())
print(segment1Data.count())
print(segment2Data.count())
print(segment3Data.count())
