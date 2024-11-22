import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum, format_string, lag
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

load_dotenv()


spark = SparkSession.builder.appName("PopGrowthTrendsByRegion").getOrCreate()

directory_path = os.getenv("TRIMMED_OUTPUT_DIRECTORY_PATH", "")

p3data = spark.read.parquet(directory_path)

p3data = p3data.withColumn("POP100", col("POP100").cast("int"))

p3data = p3data.withColumn("SUMLEV", col("SUMLEV").cast("int"))

p3data = p3data.select("REGION", "YEAR", "POP100", "SUMLEV")

p3data = p3data.filter(p3data["REGION"].isNotNull())

p3data = p3data.filter(p3data["SUMLEV"] == 50)

pop100_per_year_per_region = (
    p3data.groupBy("YEAR", "REGION")
    .agg(sum("POP100").alias("Total Population"))
    .orderBy("YEAR", "REGION")
)

pop100_per_year_per_region = pop100_per_year_per_region.withColumn(
    "REGION_NAME",
    when(col("REGION") == 1, "Northeast")
    .when(col("REGION") == 2, "Midwest")
    .when(col("REGION") == 3, "South")
    .when(col("REGION") == 4, "West")
    .when(col("REGION") == 9, "Not in a region (Puerto Rico)")
    .otherwise(None)
)

total_population_per_year_per_region = pop100_per_year_per_region.withColumn(
    "Total Population",
    format_string("%,d", col("Total Population"))
)

print()
print("============================================================")
print("Total Population Across Regions between 2000, 2010, and 2020")
print("============================================================")

total_population_per_year_per_region.select(
    "YEAR", "REGION_NAME", "Total Population"
    ).show(truncate=False)

window_spec = Window.partitionBy("REGION").orderBy("YEAR")

regional_growth = pop100_per_year_per_region.withColumn(
    "Prior_POP",
    lag("Total Population").over(window_spec).cast(DoubleType())
)

regional_growth = regional_growth.withColumn(
    "Total Population",
    col("Total Population").cast(DoubleType())
)

regional_growth = regional_growth.withColumn(
    "Percentage_Growth",
    when(
        col("Prior_POP").isNotNull(),
        ((col("Total Population") - col("Prior_POP")) / col("Prior_POP")) * 100
    ).otherwise(None)
)

regional_growth.printSchema()


regional_growth = regional_growth.withColumn(
     "Prior_POP",
    when(col("Prior_POP").isNotNull(), format_string("%,.0f", col("Prior_POP")))
    .otherwise("na")
).withColumn(
    "Total_Population",
    format_string("%,.0f", col("Total Population"))
).withColumn(
    "Percentage_Growth",
    when(col("Percentage_Growth (%)").isNotNull(), format_string("%,.2f%%", col("Percentage_Growth")))
    .otherwise("na")
)

print()
print("=================================================================")
print("Population Growth (%) Across Regions between 2000, 2010, and 2020")
print("=================================================================")

regional_growth.select(
    "YEAR", "REGION_NAME", "Prior_POP", "Total Population", "Percentage_Growth"
).show(truncate=False)
