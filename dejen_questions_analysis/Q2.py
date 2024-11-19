# 2. WHAT ARE THE TOP 10 COUNTIES WITH THE HIGHEST DIVERSITY INDEX, AND HOW DOES IT CHANGE FROM 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, pow, when, sum


# Initialize Spark Session
spark = SparkSession.builder.appName("CensusDiversityIndex").getOrCreate()

# Base path for datasets
base_path = "hdfs:///user/dirname/census_data_parquet"

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

# function to filter for county level data

def filter_county_level_data(df):
    filtered_df = df.filter(col("SUMLEV") == "050").select(*columns_of_interest)
    return filtered_df


# function to calculate diversity index

def calculate_diversity_index(df, year):
    county_df = filter_county_level_data(df)

    county_df = county_df.withColumn(
        "Validate_Population",
        when(col("P0010001") > 0, True).otherwise(False)
    )
        # Debug purposes
    valid_count = county_df.filter(col("Valid_Population") == True).count()
    print(f"Valid counties for diversity calculation in {year}: {valid_count}")
    
    
    # Add Diversity Index calculation
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

    diversity_df =  diversity_df.filter(col("Diversity_Index").isNotNull())

    return diversity_df.select("STUSAB", "COUNTY", "Diversity_Index", "Year")

# Function to find top 10 counties by diversity index
def top_10_counties_by_diversity_index(df):
    print(f"Total counties processed: {df.count()}")
    return df.orderBy(col("Diversity_Index").desc()).limit(10)
   

diversity_2000 = calculate_diversity_index(df_2000, 2000)
diversity_2010 = calculate_diversity_index(df_2010, 2010)
diversity_2020 = calculate_diversity_index(df_2020, 2020)

top_10_2000 = top_10_counties_by_diversity_index(diversity_2000)
top_10_2010 = top_10_counties_by_diversity_index(diversity_2010)
top_10_2020 = top_10_counties_by_diversity_index(diversity_2020)

#combine the results
final_top_10 = top_10_2000.union(top_10_2010).union(top_10_2020)

final_top_10.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs:///user/dirname/census_data/Q2")

spark.stop()