from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, col, lit, max, abs

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
        sum(col("AREALAND").cast("double")).alias("Total_Land_Area"),
        sum(col("AREAWATR").cast("double")).alias("Total_Water_Area")
    )

    # Add comparison columns: Land-to-Water Ratio and Land-Water Difference
    result_df = result_df.withColumn(
        "Land_to_Water_Ratio", col("Total_Land_Area") / col("Total_Water_Area")
    ).withColumn(
        "Land_Water_Difference", col("Total_Land_Area") - col("Total_Water_Area")
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

# Convert AREALAND and AREAWATR from square meters to square miles
df = df.withColumn("AREALAND", col("AREALAND").cast("double") * 0.0000003861)
df = df.withColumn("AREAWATR", col("AREAWATR").cast("double") * 0.0000003861)

# Analyze land and water areas based on state and year
df_result = analyze_land_water_areas(df, "STUSAB")


# Print DataFrame schema
df_result.printSchema()

# Save the results to the output path
df_result.coalesce(1).write.csv("/myp3/output/state_land_water_analysis", header=True, mode="overwrite")

