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


#field_names.csv path
fn_path = "/myp3/input/field_names.csv"

#read the csv as an RDD
rdd = spark.sparkContext.textFile(fn_path)

#turn rdd to list
names = rdd.collect()

#remove first element
names.pop(0)

#2000 year census path from the 
path_2000 = "/myp3/input/original_p3_data/YEAR=2000" 

#2010 year census path from the 
path_2010 = "/myp3/input/original_p3_data/YEAR=2010" 

#2020 year census path from the 
path_2020 = "/myp3/input/original_p3_data/YEAR=2020" 

#read all parquet files in the 2000 folder
df0 = spark.read.parquet(path_2000)

#read all parquet files in the 2010 folder
df1 = spark.read.parquet(path_2010)

#read all parquet files in the 2020 folder
df2 = spark.read.parquet(path_2020)

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

df.coalesce(1).write.csv("/myp3/output/", header=True, mode="overwrite")