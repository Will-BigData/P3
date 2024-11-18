from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from schemaGenerator import generate2020GeoSegmentSchema, generateSegment1Schema, generate2020And2010Segment2Schema, generate2000Segment2Schema
from pygen_census import gen_schema
from dotenv import load_dotenv
import os
import sys
from specified_columns.select_specified_columns import select_specified_columns
load_dotenv()

main_path = os.getenv('FOLDER_PATH', '')
output_path = os.getenv("OUTPUT_PATH", "")

spark_builder: SparkSession.Builder = SparkSession.builder
spark_builder.appName("p3-census").config("spark.master", "local[*]")

spark: SparkSession = spark_builder.getOrCreate()
sc = spark.sparkContext

geo_df = spark.read.csv(f'{main_path}/p3_data_2020/GeoHeader', sep='|', schema=generate2020GeoSegmentSchema())
seg1_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment1', sep='|', schema=generateSegment1Schema())
seg2_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment2', sep='|', schema=generate2020And2010Segment2Schema())

geo_df_2010 = gen_schema(spark.read.text(f"{main_path}/p3_data_2010/GeoHeader"), spark, 2010)
seg1_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment1', sep=',', schema=generateSegment1Schema())
seg2_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment2', sep=',', schema=generate2020And2010Segment2Schema())

geo_df_2000 = gen_schema(spark.read.text(f"{main_path}/p3_data_2000/GeoHeader"), spark, 2000)
seg1_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment1', sep=',', schema=generateSegment1Schema())
seg2_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment2', sep=',', schema=generate2000Segment2Schema())

filename = './specified_columns/columns_file.txt'
geo_df = select_specified_columns(geo_df, filename)
seg1_df = select_specified_columns(seg1_df, filename)
seg2_df = select_specified_columns(seg2_df, filename)

geo_df_2010 = select_specified_columns(geo_df_2010, filename)
seg1_df_2010 = select_specified_columns(seg1_df_2010, filename)
seg2_df_2010 = select_specified_columns(seg2_df_2010, filename)

geo_df_2000 = select_specified_columns(geo_df_2000, filename)
seg1_df_2000 = select_specified_columns(seg1_df_2000, filename)
seg2_df_2000 = select_specified_columns(seg2_df_2000, filename)

link_cols = ["FILEID", "STUSAB", "LOGRECNO"]
combined_df_2020 = geo_df.join(seg1_df, link_cols).join(seg2_df, link_cols).withColumn("YEAR", lit(2020))
combined_df_2010 = geo_df_2010.join(seg1_df_2010, link_cols).join(seg2_df_2010, link_cols).withColumn("YEAR", lit(2010))
combined_df_2000 = geo_df_2000.join(seg1_df_2000, link_cols).join(seg2_df_2000, link_cols).withColumn("YEAR", lit(2000))

final_data = combined_df_2020.unionByName(combined_df_2010, allowMissingColumns=True).unionByName(combined_df_2000, allowMissingColumns=True)

final_data.write.partitionBy("YEAR", "STUSAB").parquet(output_path)

if __name__ == "__main__":

    print(f"Script Name: {sys.argv[0]}")

    # Other arguments
    """ columns = []
    if len(sys.argv) > 1:
        columns = sys.argv[1:] """

    file_path = sys.argv[1]

    try:
        with open(file_path, 'r') as file:
            specified_columns = [line.strip() for line in file.readlines()]

        
        geo_df.select(specified_columns).show()

    except FileNotFoundError as e:
        print(e.message) 
