from pyspark.sql import SparkSession

# initialize a Spark Session
spark = SparkSession.builder.appName("census analysis").getOrCreate()


#field_names.csv path
fn_path = "/myp3/input/field_names.csv"

#read the csv as an RDD
rdd = spark.sparkContext.textFile(fn_path)

#turn rdd to list
names = rdd.collect()

#remove first element
names.pop(0)

#2000 year census path
path_2000 = "/myp3/input/p3_output/YEAR=2000" 

#read all parquet files in the folder
df = spark.read.parquet(path_2000)

