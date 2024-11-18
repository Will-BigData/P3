import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from dotenv import load_dotenv
from schemaGenerator import generate2020GeoSegmentSchema, generateSegment1Schema, generate2020And2010Segment2Schema, generate2000Segment2Schema
from pygen_census import gen_schema
from specified_columns.select_specified_columns import select_specified_columns

from sandbox.tkinter_gui import create_gui
from tkinter import messagebox

load_dotenv()


main_path = os.getenv('FOLDER_PATH', '')
output_path = os.getenv("OUTPUT_PATH", '')
selected_columns_file = './specified_columns/selected_columns_file.txt'

spark = SparkSession.builder.appName("p3-census").config("spark.master", "local[*]").getOrCreate()



def read_data_from_all_available_years():
    geo_df = spark.read.csv(f'{main_path}/p3_data_2020/GeoHeader', sep='|', schema=generate2020GeoSegmentSchema())
    seg1_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment1', sep='|', schema=generateSegment1Schema())
    seg2_df = spark.read.csv(f'{main_path}/p3_data_2020/Segment2', sep='|', schema=generate2020And2010Segment2Schema())
    
    geo_df_2010 = gen_schema(spark.read.text(f"{main_path}/p3_data_2010/GeoHeader"), spark, 2010)
    seg1_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment1', sep=',', schema=generateSegment1Schema())
    seg2_df_2010 = spark.read.csv(f'{main_path}/p3_data_2010/Segment2', sep=',', schema=generate2020And2010Segment2Schema())
    
    geo_df_2000 = gen_schema(spark.read.text(f"{main_path}/p3_data_2000/GeoHeader"), spark, 2000)
    seg1_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment1', sep=',', schema=generateSegment1Schema())
    seg2_df_2000 = spark.read.csv(f'{main_path}/p3_data_2000/Segment2', sep=',', schema=generate2000Segment2Schema())

    return geo_df, seg1_df, seg2_df, geo_df_2010, seg1_df_2010, seg2_df_2010, geo_df_2000, seg1_df_2000, seg2_df_2000

def select_columns(df, selected_columns_file):
    return select_specified_columns(df, selected_columns_file)

def join_data_from_all_available_years(read_data_func, selected_columns_file):
    geo_df, seg1_df, seg2_df, geo_df_2010, seg1_df_2010, seg2_df_2010, geo_df_2000, seg1_df_2000, seg2_df_2000 = read_data_func()

    geo_df = select_columns(geo_df, selected_columns_file)
    seg1_df = select_columns(seg1_df, selected_columns_file)
    seg2_df = select_columns(seg2_df, selected_columns_file)
    geo_df_2010 = select_columns(geo_df_2010, selected_columns_file)
    seg1_df_2010 = select_columns(seg1_df_2010, selected_columns_file)
    seg2_df_2010 = select_columns(seg2_df_2010, selected_columns_file)
    geo_df_2000 = select_columns(geo_df_2000, selected_columns_file)
    seg1_df_2000 = select_columns(seg1_df_2000, selected_columns_file)
    seg2_df_2000 = select_columns(seg2_df_2000, selected_columns_file)

    foreign_keys = ["FILEID", "STUSAB", "LOGRECNO"]
    combined_df_2020 = geo_df.join(seg1_df, on = foreign_keys).join(seg2_df, on = foreign_keys).withColumn("YEAR", lit(2020))
    combined_df_2010 = geo_df_2010.join(seg1_df_2010, on = foreign_keys).join(seg2_df_2010, on = foreign_keys).withColumn("YEAR", lit(2010))
    combined_df_2000 = geo_df_2000.join(seg1_df_2000, foreign_keys).join(seg2_df_2000, foreign_keys).withColumn("YEAR", lit(2000))

    return combined_df_2020, combined_df_2010, combined_df_2000

def combine_data_from_all_three_years(combined_df_2020, combined_df_2010, combined_df_2000):
    return combined_df_2020.unionByName(combined_df_2010, allowMissingColumns=True).unionByName(combined_df_2000, allowMissingColumns=True)

def process_data_from_all_available_years(read_data_func, selected_columns_file):
    combined_df_2020, combined_df_2010, combined_df_2000 = join_data_from_all_available_years(read_data_func, selected_columns_file)
    final_data = combine_data_from_all_three_years(combined_df_2020, combined_df_2010, combined_df_2000)
    return final_data

def save_final_data_to_parquet(final_data):
    final_data.write.partitionBy("YEAR", "STUSAB").parquet(f"{output_path}/p3_data_combined_parquet")

def save_selected_columns_to_file(selected_columns):
    selected_columns_file = './specified_columns/selected_columns_file.txt'
    Path(os.path.dirname(selected_columns_file)).mkdir(parents=True, exist_ok=True)
    with open(selected_columns_file, "w") as f:
        for column in selected_columns:
            f.write(column + "\n")
    return selected_columns_file

def generate_parquet_file(column_checkboxes, read_data_func):
    selected_columns = [column for column, checkbox in column_checkboxes.items() if checkbox.get()]
    if not selected_columns:
        messagebox.showerror("Error", "Please select at least one field!")
        return
    
    selected_columns_file = save_selected_columns_to_file(selected_columns)

    final_data = process_data_from_all_available_years(read_data_func, selected_columns_file)
    save_final_data_to_parquet(final_data)
    messagebox.showinfo("Success", "Parquet file generated successfully!")

columns = []
try:
    with open(selected_columns_file, 'r') as f:
        columns = [line.strip() for line in f.readlines()]
except FileNotFoundError:
    messagebox.showerror("Error", "Columns file not found!")

create_gui(columns, read_data_from_all_available_years, generate_parquet_file)
