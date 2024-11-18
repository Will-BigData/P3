from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col,lit

def analyze_land_water_areas(df: DataFrame, geographic_division: str) -> DataFrame:
    """
    Analyze and compare land (AREALAND) and water (AREAWATR) areas across geographic divisions.

    Args:
        df (DataFrame): The input PySpark DataFrame containing columns AREALAND, AREAWATR, and the geographic division.
        geographic_division (str): The column name representing the geographic division (e.g., 'STATE', 'COUNTY').

    Returns:
        DataFrame: A PySpark DataFrame with total land and water areas, land-to-water ratio, and land-water difference.
    """
    # Group by the specified geographic division and calculate total land and water areas
    result_df = df.groupBy(geographic_division).agg(
        sum(col("AREALAND").cast("float")).alias("Total_Land_Area"),
        sum(col("AREAWATR").cast("float")).alias("Total_Water_Area")
    )

    # Add comparison columns: Land-to-Water Ratio and Land-Water Difference
    result_df = result_df.withColumn(
        "Land_to_Water_Ratio", col("Total_Land_Area") / col("Total_Water_Area")
    ).withColumn(
        "Land_Water_Difference", col("Total_Land_Area") - col("Total_Water_Area")
    )

    return result_df

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

# Convert AREALAND from square meters to square miles by multiplying by 0.0000003861
df0 = df0.withColumn("AREALAND", col("AREALAND") * 0.0000003861)
# Convert AREAWATR from square meters to square miles by multiplying by 0.0000003861
df0 = df0.withColumn("AREAWATR", col("AREAWATR") * 0.0000003861)

# Convert AREALAND from square meters to square miles by multiplying by 0.0000003861
df1 = df1.withColumn("AREALAND", col("AREALAND") * 0.0000003861)
# Convert AREAWATR from square meters to square miles by multiplying by 0.0000003861
df1 = df1.withColumn("AREAWATR", col("AREAWATR") * 0.0000003861)


# Convert AREALAND from square meters to square miles by multiplying by 0.0000003861
df2 = df2.withColumn("AREALAND", col("AREALAND") * 0.0000003861)
# Convert AREAWATR from square meters to square miles by multiplying by 0.0000003861
df2 = df2.withColumn("AREAWATR", col("AREAWATR") * 0.0000003861)


#land_water analyze for 2000, 2100, and 2200 based on state
df0_r = analyze_land_water_areas(df0, "STUSAB")
df1_r = analyze_land_water_areas(df1, "STUSAB")
df2_r = analyze_land_water_areas(df2, "STUSAB")

#adding year column
df0_r = df0_r.withColumn("year", lit(2000))
df1_r = df1_r.withColumn("year", lit(2010))
df2_r = df2_r.withColumn("year", lit(2020))


#unioning all of them
df = df0_r.union(df1_r)
df = df.union(df2_r)


#print data frame schema
df.printSchema()



df.coalesce(1).write.csv("/myp3/output/state_land_water_analysis", header=True, mode="overwrite")


from pyspark.sql.functions import abs, col, lit, sum

# Pivot the data to create year columns for Land_to_Water_Ratio
pivoted_ratio_df = df.groupBy("STUSAB").pivot("year").agg(
    sum("Land_to_Water_Ratio").alias("Land_to_Water_Ratio")
)

# Calculate absolute changes in Land_to_Water_Ratio between years
pivoted_ratio_df = pivoted_ratio_df.withColumn(
    "Change_2000_2010", abs(col("2010") - col("2000"))
).withColumn(
    "Change_2010_2020", abs(col("2020") - col("2010"))
).withColumn(
    "Change_2000_2020", abs(col("2020") - col("2000"))
)

# Calculate the total change in Land_to_Water_Ratio across all years
pivoted_ratio_df = pivoted_ratio_df.withColumn(
    "Total_Change",
    col("Change_2000_2010") + col("Change_2010_2020") + col("Change_2000_2020")
)

# Order by the total change and select the top 10 states
top_states_ratio = pivoted_ratio_df.orderBy(col("Total_Change").desc()).limit(10)

# Show the top 10 states with the most Land_to_Water_Ratio change
top_states_ratio.show()

# Write the results to a CSV file
top_states_ratio.coalesce(1).write.csv("/myp3/output/top_states_ratio", header=True, mode="overwrite")