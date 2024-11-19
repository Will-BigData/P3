from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col,lit


# initialize a Spark Session
spark = SparkSession.builder.appName("census analysis").getOrCreate()

#2000 year census path
path_2000 = "/myp3/input/original_p3_data/YEAR=2000" 

#2010 year census path
path_2010 = "/myp3/input/original_p3_data/YEAR=2010" 

#2020 year census path
path_2020 = "/myp3/input/original_p3_data/YEAR=2020" 

#read all parquet files in the 2000 folder
df0 = spark.read.parquet(path_2000)

#read all parquet files in the 2010 folder
df1 = spark.read.parquet(path_2010)

#read all parquet files in the 2020 folder
df2 = spark.read.parquet(path_2020)


#adding year column
df0 = df0.withColumn("year", lit(2000))
df1 = df1.withColumn("year", lit(2010))
df2 = df2.withColumn("year", lit(2020))


#unioning all of them
df = df0.union(df1)
df = df.union(df2)


#print data frame schema
df.printSchema()



df.coalesce(100).write.parquet("/myp3/output/all_census_years", mode="overwrite")

