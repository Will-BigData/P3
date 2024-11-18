
# 1. WHAT IS THE TOTAL POPULATION BY RACE ACROSS THE UNITED STATES, AND HOW DOES IT CHANGE OVER TIME FROM 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusRaceAnalysis").getOrCreate()

# path for data
base_path = "hdfs:///user/dejtes/census_data_parquet"


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
    
