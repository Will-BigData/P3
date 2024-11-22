import os
from pathlib import Path

from pyspark.sql import SparkSession
from dotenv import load_dotenv

from tkinker_gui import create_gui
from tkinter import messagebox

load_dotenv()


data_path = os.getenv("DATA_PATH", '')
trimmed_path = os.getenv("TRIMMED_OUTPUT_PATH", '')
columns_file = './specified_columns/columns_file.txt'

spark = SparkSession.builder.appName("p3-census").config("spark.master", "local[*]").getOrCreate()

def save_selected_columns_to_file(selected_columns):
    selected_columns_file = './specified_columns/selected_columns_file.txt'
    Path(os.path.dirname(selected_columns_file)).mkdir(parents=True, exist_ok=True)
    with open(selected_columns_file, "w") as f:
        for column in selected_columns:
            f.write(column + "\n")
    return selected_columns_file

def save_final_data_to_parquet(final_data):
    final_data.write.mode("overwrite").parquet(f"{trimmed_path}/p3_data_combined_parquet")
    command = f"hdfs dfs -rm {trimmed_path}/p3_data_combined_parquet/_SUCCESS"
    result = os.system(command)

    if result == 0:
        print("SUCCESS")
    else:
        print("oof")

def generate_parquet_file(column_checkboxes, read_data_func):
    selected_columns = [column for column, checkbox in column_checkboxes.items() if checkbox.get()]
    if not selected_columns:
        messagebox.showerror("Error", "Please select at least one field!")
        return
    
    selected_columns_file = save_selected_columns_to_file(selected_columns)

    data = read_data_func()
    try:
        with open(selected_columns_file, 'r') as f:
            columns = [line.strip() for line in f.readlines()]
    except FileNotFoundError:
        messagebox.showerror("Error", "Columns file not found!")

    final_data = data.select(columns)
    
    final_data = final_data.filter(final_data["SUMLEV"] != 750)

    save_final_data_to_parquet(final_data)
    messagebox.showinfo("Success", "Parquet file generated successfully!")

def read_data():
    return spark.read.parquet(f'{data_path}')

columns = []
try:
    with open(columns_file, 'r') as f:
        columns = [line.strip() for line in f.readlines()]
except FileNotFoundError:
    messagebox.showerror("Error", "Columns file not found!")

create_gui(columns, read_data, generate_parquet_file)
