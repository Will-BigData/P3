from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col, lit, abs, round, corr

def analyze_land_water_areas(df: DataFrame, geographic_division: str) -> DataFrame:
    """
    Analyze and compare land (AREALAND) and water (AREAWATR) areas across geographic divisions,
    including the year in the output.

    Args:
        df (DataFrame): The input PySpark DataFrame containing columns AREALAND, AREAWATR, and the geographic division.
        geographic_division (str): The column name representing the geographic division (e.g., 'STATE', 'COUNTY').

    Returns:
        DataFrame: A PySpark DataFrame with total land and water areas, land-to-water ratio, land-water difference,
        and year.
    """
    # Filter rows where SUMLEV = 40 
    filtered_df = df.filter(col("SUMLEV") == 40)

    # Group by the specified geographic division and year, and calculate total land and water areas
    result_df = filtered_df.groupBy(geographic_division, "year").agg(
        sum(col("POP100")).alias("Total_Population"),
        sum(col("AREALAND")).alias("Total_Land_Area"),
        sum(col("AREAWATR")).alias("Total_Water_Area")
    )

    # Add comparison columns: Land-to-Water Ratio and Land-Water Difference
    result_df = result_df.withColumn(
        "Land_to_Water_Ratio",  round(col("Total_Land_Area") / col("Total_Water_Area"),2)
    ).withColumn(
        "Land_Water_Difference", round(col("Total_Land_Area") - col("Total_Water_Area"),2)
    )

    return result_df

# Initialize a Spark Session
spark = SparkSession.builder.appName("census analysis").getOrCreate()

# Paths for different census years
path_2000 = "/myp3/input/original_p3_data/YEAR=2000"
path_2010 = "/myp3/input/original_p3_data/YEAR=2010"
path_2020 = "/myp3/input/original_p3_data/YEAR=2020"

# Read all parquet files for each year
df0 = spark.read.parquet(path_2000).withColumn("year", lit(2000))
df1 = spark.read.parquet(path_2010).withColumn("year", lit(2010))
#df2 = spark.read.parquet(path_2020).withColumn("year", lit(2020))

# Union all data
df = df0.union(df1) #.union(df2)

# Convert AREALAND and AREAWATR to float
df = df.withColumn("AREALAND", col("AREALAND").cast("float"))
df = df.withColumn("AREAWATR", col("AREAWATR").cast("float"))

#convert POP1000 to int
df = df.withColumn("POP100", col("POP100").cast("int"))

# square meters to square miles conversion
conversion_factor = 3.861e-7

# Analyze land and water areas based on state and year
df_result = analyze_land_water_areas(df, "STUSAB")

#column that has total Land Area in miles and round the values in a column to two decimal places
df_result = df_result.withColumn("TLA_miles", round(col("Total_Land_Area") * conversion_factor,2))
#column that has total Water Area in miles  and round the values in a column to two decimal places
df_result = df_result.withColumn("TWA_miles", round(col("Total_Water_Area") * conversion_factor,2))

#make a column for TOTAL_AREA_MILES
df_result = df_result.withColumn("TOTAL_AREA_MILES", round(col("TLA_miles")+col("TWA_miles"),2) )

#make a column for Percent area, water
df_result = df_result.withColumn("Percent area, water (%)", round(col("TWA_miles")/col("TOTAL_AREA_MILES")*100,2)  )


df_result.show()

#making a table for the result
df_result.createOrReplaceTempView("df_result")

#select states New Mexico, Texas, and Oklahoma from the year 2010
test_df = spark.sql("SELECT  STUSAB, TLA_miles, TWA_miles, year FROM df_result WHERE (STUSAB = 'NM' OR STUSAB = 'TX' OR STUSAB = 'OK')  AND year = 2010 ")

#display New Mexico, Texas, and Oklahoma AREALAND and AREAWATR in sqaure miles for testing my results
test_df.show()



# Print DataFrame schema
df_result.printSchema()

# Save the results to the output path
df_result.coalesce(1).write.csv("/myp3/output/state_land_water_analysis", header=True, mode="overwrite")


# Pivot the data to create year columns for Land_to_Water_Ratio
pivoted_ratio_df = df_result.groupBy("STUSAB").pivot("year").agg(
    sum("Land_to_Water_Ratio")
)

pivoted_ratio_df.filter(pivoted_ratio_df["STUSAB"] == "NM").show()

# Calculate absolute changes in Land_to_Water_Ratio between years
pivoted_ratio_df = pivoted_ratio_df.withColumn(
    "Change_2000_2010", round(abs(col("2010") - col("2000")),2)
)

# Order by the total change and select the top 10 states
top_states_ratio = pivoted_ratio_df.orderBy(col("Change_2000_2010").desc()).limit(10)

# Show the top 10 states with the most Land_to_Water_Ratio change
top_states_ratio.show()

# Write the results to a CSV file
top_states_ratio.coalesce(1).write.csv("/myp3/output/top_states_ratio", header=True, mode="overwrite")
