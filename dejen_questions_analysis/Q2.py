# 2. WHAT ARE THE TOP 10 COUNTIES WITH THE HIGHEST DIVERSITY INDEX, AND HOW DOES IT CHANGE FROM 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, pow, when

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusDiversityIndexWithCountyNames").getOrCreate()

# Base path for datasets
base_path = "hdfs:///user/dejtes/census_data_parquet"

# Load datasets for 2000, 2010, and 2020
df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")

# Columns of interest for diversity analysis
columns_of_interest = [
    "STUSAB",
    "COUNTY",
    "SUMLEV",
    "P0010001",
    "P0010003",   
    "P0010004",   
    "P0010005",   
    "P0010006",  
    "P0010007",   
    "P0010008",   
    "P0010009",
]

# Create a mapping DataFrame for County Names and State Names
county_mapping_data = [
    ("HI", 1, "Hawaii County", "Hawaii"),
    ("AK", 16, "Aleutians West Census Area", "Alaska"),
    ("HI", 9, "Maui County", "Hawaii"),
    ("AK", 13, "Aleutians East Borough", "Alaska"),
    ("NY", 5, "Bronx County", "New York"),
    ("HI", 7, "Kauai County", "Hawaii"),
    ("NY", 81, "Queens County", "New York"),
    ("HI", 3, "Honolulu County", "Hawaii"),
    ("CA", 1, "Alameda County", "California"),
    ("NC", 155, "Robeson County", "North Carolina"),
    ("NJ", 17, "Hudson County", "New Jersey"),
    ("CA", 77, "San Joaquin County", "California"),
    ("CA", 37, "Los Angeles County", "California"),
    ("TX", 157, "Montgomery County", "Texas"),
    ("TX", 113, "Hamilton County", "Texas"),
    ("CA", 95, "Solano County", "California"),
    ("HI", 5, "Kalawao County", "Hawaii"),
]

county_mapping_schema = ["STUSAB", "COUNTY", "County_Name", "State_Name"]
county_mapping = spark.createDataFrame(county_mapping_data, schema=county_mapping_schema)

# Function to filter for county-level data
def filter_county_level_data(df):
    return df.filter(col("SUMLEV") == "050").select(*columns_of_interest)

# Function to calculate diversity index
def calculate_diversity_index(df, year):
    county_df = filter_county_level_data(df)

    diversity_df = county_df.withColumn(
        "Diversity_Index",
        lit(1) - (
            pow(col("P0010003") / col("P0010001"), 2) +
            pow(col("P0010004") / col("P0010001"), 2) +
            pow(col("P0010005") / col("P0010001"), 2) +
            pow(col("P0010006") / col("P0010001"), 2) +
            pow(col("P0010007") / col("P0010001"), 2) +
            pow(col("P0010008") / col("P0010001"), 2) +
            pow(col("P0010009") / col("P0010001"), 2)
        )
    )

    diversity_df = diversity_df.withColumn("Year", lit(year))
    return diversity_df.select("STUSAB", "COUNTY", "Diversity_Index", "Year")

def top_10_counties_by_diversity_index(df):
    return df.orderBy(col("Diversity_Index").desc()).limit(10)

# Calculate Diversity Index for each year
diversity_2000 = calculate_diversity_index(df_2000, 2000)
diversity_2010 = calculate_diversity_index(df_2010, 2010)
diversity_2020 = calculate_diversity_index(df_2020, 2020)

# Find top 10 counties for each year
top_10_2000 = top_10_counties_by_diversity_index(diversity_2000)
top_10_2010 = top_10_counties_by_diversity_index(diversity_2010)
top_10_2020 = top_10_counties_by_diversity_index(diversity_2020)

# Combine
final_top_10 = top_10_2000.union(top_10_2010).union(top_10_2020)

# Join with the county mapping to add county names and state names
final_result_with_names = final_top_10.join(
    county_mapping,
    on=["STUSAB", "COUNTY"],
    how="left"
).select("STUSAB", "State_Name", "County_Name", "COUNTY", "Diversity_Index", "Year")

final_result_with_names.show()

final_result_with_names.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs:///user/dejtes/census_data/Q2_with_county_names")

spark.stop()
