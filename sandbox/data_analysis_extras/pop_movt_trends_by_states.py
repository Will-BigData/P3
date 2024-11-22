import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, sum, when, format_number, format_string
from pyspark.sql.window import Window

load_dotenv()

spark = SparkSession.builder.appName("Population Movements By State").getOrCreate()

directory_path = os.getenv("TRIMMED_OUTPUT_DIRECTORY_PATH", "")

p3data = spark.read.parquet(directory_path)

p3data = p3data.withColumn("POP100", col("POP100").cast("int"))

p3data = p3data.withColumn("SUMLEV", col("SUMLEV").cast("int"))

p3data = p3data.select("STATE", "YEAR", "POP100") \
    .filter(col("POP100").isNotNull()) \
        .filter(p3data["SUMLEV"] == 50)

total_population = p3data.groupBy("YEAR").agg(sum("POP100").alias("Total_Population")).orderBy("YEAR")

print()
print("=============================================")
print("Total Population between 2000, 2010, and 2020")
print("=============================================")

total_population.show()

state_population = (
    p3data.groupBy("STATE", "YEAR")
    .agg(sum("POP100").alias("Total Population"))
    .orderBy("STATE", "YEAR")
)

window_spec = Window.partitionBy("STATE").orderBy("YEAR")

state_population = state_population.withColumn(
    "Prior_POP",
    lag("Total Population").over(window_spec)
)

print()
print("======================================================")
print("Total Population by State between 2000, 2010, and 2020")
print("======================================================")

state_population.show()

population_moved = state_population.withColumn(
    "Population_Moved",
    when(
        (col("Prior_POP").isNotNull()) &(col("YEAR") != 2000),
        col("Total Population") - col("Prior_POP")
    ).otherwise(0)
)

print()
print("===========================================================")
print("Population Moved across States between 2000, 2010, and 2020")
print("===========================================================")

population_moved.show()

percentage_moved = population_moved.withColumn(
    "Percentage_Moved",
    when(
        col("Prior_POP").isNotNull() & (col("Prior_POP") > 0),
        (col("Population_Moved") / col("Prior_POP")) * 100
    ).otherwise(None)
)

percentage_moved = percentage_moved.withColumn(
    "Total Population",
    format_number(col("Total Population"), 0)
).withColumn(
    "Prior_POP",
    format_number(col("Prior_POP"), 0)
).withColumn(
    "Population_Moved",
    format_number(col("Population_Moved"), 0) 
).withColumn(
    "Percentage_Moved",
    when(col("Percentage_Moved").isNotNull(), format_string("%,.2f%%", col("Percentage_Moved")))
    .otherwise("NA")
)

print()
print("===========================================================")
print("Percentage Moved across States between 2000, 2010, and 2020")
print("===========================================================")

percentage_moved.select(
    "STATE", "YEAR", "Prior_POP", "Total Population", "Population_Moved", "Percentage_Moved"
).orderBy("STATE", "YEAR").show(truncate=False)