# 3. HOW DOES THE RACIAL DISTRIBUTION VARY BETWEEN METROPOLITAN AND MICROPOLITAN AREAS FROM 2000-2020?

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, lit, col

# Initialize Spark Session
spark = SparkSession.builder.appName("CensusMetroMicroAnalysis").getOrCreate()

base_path = "hdfs:///user/dirname/census_data_parquet"

df_2000 = spark.read.parquet(f"{base_path}/YEAR=2000")
df_2010 = spark.read.parquet(f"{base_path}/YEAR=2010")
df_2020 = spark.read.parquet(f"{base_path}/YEAR=2020")


columns_of_interest = [
    "SUMLEV",
    "P0010003",
    "P0010004",
    "P0010005",
    "P0010006",
    "P0010007",
    "P0010008",
    "P0010009",
]

# Function to filter for metro and micro areas
def filter_metro_micro(df):
    filtered_df = df.filter((col("SUMLEV") == "310") | (col("SUMLEV") == "320")).select(*columns_of_interest)
    return filtered_df


# calculate racial proportions
def calculate_racial_distribution(df, year):
    metro_micro_df = filter_metro_micro(df)
    
    # Calculate racial proportions
    racial_distribution_df = metro_micro_df.withColumn("White_Proportion", col("P0010003") / col("P0010001")) \
                                           .withColumn("Black_Proportion", col("P0010004") / col("P0010001")) \
                                           .withColumn("American_Indian_Proportion", col("P0010005") / col("P0010001")) \
                                           .withColumn("Asian_Proportion", col("P0010006") / col("P0010001")) \
                                           .withColumn("Native_Hawaiian_Proportion", col("P0010007") / col("P0010001")) \
                                           .withColumn("Other_Race_Proportion", col("P0010008") / col("P0010001")) \
                                           .withColumn("Two_or_More_Races_Proportion", col("P0010009") / col("P0010001")) \
                                           .withColumn("Year", lit(year))
    
    return racial_distribution_df.select("SUMLEV", "White_Proportion", "Black_Proportion", 
                                         "American_Indian_Proportion", "Asian_Proportion", 
                                         "Native_Hawaiian_Proportion", "Other_Race_Proportion", 
                                         "Two_or_More_Races_Proportion", "Year")


# Calculate racial distribution for all years
racial_distribution_2000 = calculate_racial_distribution(df_2000, 2000)
racial_distribution_2010 = calculate_racial_distribution(df_2010, 2010)
racial_distribution_2020 = calculate_racial_distribution(df_2020, 2020)

# Combine results into one DataFrame
final_racial_distribution = racial_distribution_2000.union(racial_distribution_2010).union(racial_distribution_2020)

# Aggregate by area type and year
aggregated_distribution = final_racial_distribution.groupBy("SUMLEV", "Year").agg(
    sum("White_Proportion").alias("Avg_White_Proportion"),
    sum("Black_Proportion").alias("Avg_Black_Proportion"),
    sum("American_Indian_Proportion").alias("Avg_American_Indian_Proportion"),
    sum("Asian_Proportion").alias("Avg_Asian_Proportion"),
    sum("Native_Hawaiian_Proportion").alias("Avg_Native_Hawaiian_Proportion"),
    sum("Other_Race_Proportion").alias("Avg_Other_Race_Proportion"),
    sum("Two_or_More_Races_Proportion").alias("Avg_Two_or_More_Races_Proportion")
)

# Show aggregated results
#aggregated_distribution.show()


aggregated_distribution.coalesce(1).write.mode("overwrite").option("header", "true").csv("hdfs:///user/dirname/census_data/Q3")