import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Read Nested Parquet Files") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

directory_path = "file:///mnt/d/Downloads/output-20241115T180202Z-001/output"
df = spark.read.parquet(directory_path)

# Filter out rows where STATE is null
df = df.filter(df["STATE"].isNotNull())

# Step 1: Calculate Total Housing Units
df = df.withColumn("total_housing", F.col("H0010002") + F.col("H0010003"))

# Step 2: Calculate Occupancy and Vacancy Percentages
df = df.withColumn("occupied_pct", F.col("H0010002") / F.col("total_housing") * 100)
df = df.withColumn("vacant_pct", F.col("H0010003") / F.col("total_housing") * 100)

# Step 3: Compare Housing Unit Count (HU100) to Total Housing
df = df.withColumn("housing_diff", F.col("HU100") - F.col("total_housing"))

# Step 4: Calculate Decade
df = df.withColumn("decade", F.floor(F.col("YEAR") / 10) * 10)

# Determine Occupied/Vacant Ratio Trend
# Calculate occupied/vacant ratio for each state and decade
df = df.withColumn("occupied_vacant_ratio", F.col("H0010002") / F.col("H0010003"))

# Step 5: Calculate Population and Population Change by Decade
state_population = df.groupBy("STATE", "decade").agg(F.sum("POP100").alias("population"))

# WRITE AND RELOAD DATAFRAME df HERE
df.write.parquet("file:///home/jakehunter/data_test/df_intermediate.parquet")
df.unpersist()  # Free memory after writing
df_intermediate = spark.read.parquet("file:///home/jakehunter/data_test/df_intermediate.parquet")

# Calculate population change between decades
window_spec = Window.partitionBy("STATE").orderBy("decade")
state_population = state_population.withColumn(
    "population_change", F.col("population") - F.lag("population").over(window_spec)
)

# Format for readability
state_population = state_population.withColumn(
    "population", F.format_number("population", 0)  # Format to remove scientific notation
)

state_population = state_population.withColumn(
    "population_change", F.format_number("population_change", 0)  # Format to remove scientific notation
)

# Handle NULL values in population_change and ratio_change by filling with 0
state_population = state_population.withColumn(
    "population_change", F.coalesce("population_change", F.lit(0))
)

# WRITE AND RELOAD DATAFRAME state_population HERE
state_population.write.parquet("file:///home/jakehunter/data_test/state_population_intermediate.parquet")
state_population.unpersist()  # Free memory after writing
state_population_intermediate = spark.read.parquet("file:///home/jakehunter/data_test/state_population_intermediate.parquet")

# Calculate the average ratio for each state and decade
housing_ratio = df_intermediate.groupBy("STATE", "decade").agg(F.avg("occupied_vacant_ratio").alias("avg_ratio"))

# Calculate the change in the ratio between decades
housing_ratio = housing_ratio.withColumn(
    "ratio_change", F.col("avg_ratio") - F.lag("avg_ratio").over(window_spec)
)

# Round ratio to 2 decimal places for readability
housing_ratio = housing_ratio.withColumn(
    "avg_ratio", F.round("avg_ratio", 2)  # Round to two decimal places
)

housing_ratio = housing_ratio.withColumn(
    "ratio_change", F.coalesce("ratio_change", F.lit(0))
)


# Step 7: Combine Results
# Join population change and ratio change with the main dataset
result = state_population_intermediate \
    .join(housing_ratio, ["STATE", "decade"], "inner") \
    .join(df_intermediate, ["STATE", "decade"], "inner")

# Select all original columns (assuming you know the column names you want from the original `df`)
original_columns = df.columns  # All original columns in `df`

result = result.select(
    *original_columns,  # All original columns from `df`
    "POP100",           # Include `POP100`
    "total_housing",  
    "occupied_pct",   
    "vacant_pct",     
    "housing_diff",   
    "population",    
    "population_change", 
    "avg_ratio", 
    "ratio_change"
)

# Group by `STATE` and `decade` to make sure we have unique entries
result = result.groupBy("STATE", "decade").agg(
    F.first("POP100").alias("POP100"),  
    F.first("total_housing").alias("TotalHousingUnits"),
    F.first("occupied_pct").alias("HousingOccupied%"),
    F.first("vacant_pct").alias("HousingVacant%"),
    F.first("housing_diff").alias("TotalVSCalcHousingDiff"),
    F.first("population").alias("Population"),
    F.first("population_change").alias("PopulationDiff"),
    F.first("avg_ratio").alias("AvgHousingRatio"),
    F.first("ratio_change").alias("HousingRatioChange")
)

# Show the final results
#result.show(truncate=False)

# Optionally, write the result to a CSV file
result.coalesce(1).write.parquet("file:///home/jakehunter/data_test/final_output.parquet")

# Stop Spark session after processing
spark.stop()
