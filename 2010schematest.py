from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("census-dataset")\
.config("spark.master", "local[*]")\
.getOrCreate()

geo_column_dict = {}

with open("data/2010+2000geoheaders.txt") as file:
    for line in file:
        key_value = line.split(',')
        geo_column_dict[key_value[0]] = int(key_value[1].strip())


current_byte = 0

