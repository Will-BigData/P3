
# 1. WHAT IS THE TOTAL POPULATION BY RACE ACROSS THE UNITED STATES, AND HOW DOES IT CHANGE OVER TIME FROM 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusRaceAnalysis").getOrCreate()

# path for data
base_path = "hdfs:///user/dirname/census_data_parquet"


df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")

columns_of_interest = [
    "STUSAB",     
    "SUMLEV",     
    "P0010003",   
    "P0010004",   
    "P0010005",   
    "P0010006",  
    "P0010007",   
    "P0010008",   
    "P0010009",
]

# Function to calculate total population by race at the state level
def calculate_state_race_population(df, year):
    state_level_df = df.filter(df["SUMLEV"] == "040").select(*columns_of_interest)

    total_population_by_state = state_level_df.groupBy("STUSAB").agg(
        sum("P0010003").alias("White_Alone"),
        sum("P0010004").alias("Black_Alone"),
        sum("P0010005").alias("American_Indian_Alone"),
        sum("P0010006").alias("Asian_Alone"),
        sum("P0010007").alias("Native_Hawaiian_Alone"),
        sum("P0010008").alias("Some_Other_Race"),
        sum("P0010009").alias("Two_or_More_Races")
    )

    return total_population_by_state.withColumn("Year", lit(year))

# Perforem the analysis for all the years

result_2000 = calculate_state_race_population(df_2000, 2000)
result_2010 = calculate_state_race_population(df_2010, 2010)
result_2020 = calculate_state_race_population(df_2020, 2020)


# Combine the results
final_result = result_2000.union(result_2010).union(result_2020)

# Windows specification fro cumulative sums by race across all states
window_spec = Window.partitionBy("STUSAB").orderBy("Year")

# Add cululative population columns for each race
final_result = final_result.withColumn(
    "Cumulative_White_Alone", sum("White_Alone").over(window_spec)
).withColumn(
    "Cumulative_Black_Alone", sum("Black_Alone").over(window_spec)
).withColumn(
    "Cumulative_American_Indian_Alone", sum("American_Indian_Alone").over(window_spec)
).withColumn(
    "Cumulative_Asian_Alone", sum("Asian_Alone").over(window_spec)
).withColumn(
    "Cumulative_Native_Hawaiian_Alone", sum("Native_Hawaiian_Alone").over(window_spec)
).withColumn(
    "Cumulative_Some_Other_Race", sum("Some_Other_Race").over(window_spec)
).withColumn(
    "Cumulative_Two_or_More_Races", sum("Two_or_More_Races").over(window_spec)
)

final_result.show()

#save the combined result to HDFS to a singel file
final_result.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs:///user/dirname/census_data/Q1_with_cumulative")

spark.stop()