import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def select_columns(df, selected_columns_file):
    with open(selected_columns_file, "r") as f:
        selected_columns = [line.strip() for line in f.readlines()]

    df_selected = df.select(*list(set(selected_columns) & set(df.columns)))

    return df_selected

def generate_parquet_file(column_checkboxes, df):
    selected_columns = [column for column, checkbox in column_checkboxes.items() if checkbox.get()]
    if not selected_columns:
        messagebox.showerror("Error", "Please select at least one field!")
        return

    selected_columns_file = 'selected_columns_file.txt'
    with open(selected_columns_file, "w") as f:
        for column in selected_columns:
            f.write(column + "\n")

    final_data = select_columns(df, selected_columns_file)
    final_data.write.mode("overwrite").parquet("p3_data_parquet")

def create_gui(columns, df, generate_parquet_file_func):
    root = tk.Tk()
    root.title("Select Fields for Parquet")

    column_checkboxes = {}
    for column in columns:
        column_checkboxes[column] = tk.BooleanVar()

    checkbox_frame = ttk.Frame(root)
    checkbox_frame.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    for column in columns:
        checkbox = ttk.Checkbutton(checkbox_frame, text=column, variable=column_checkboxes[column])
        checkbox.pack(anchor='w')

    generate_button = ttk.Button(root, text="Generate Parquet File", command=lambda: generate_parquet_file_func(column_checkboxes, df))
    generate_button.pack(pady=20)

    root.mainloop()


columns = ["FILEID", "STUSAB", "SUMLEV", "GEOCOMP", "CHARITER", "LOGRECNO", "REGION", "DIVISION",
                     "STATE", "COUNTY", "POP100", "HU100", "INTPTLAT", "INTPTLON"]

data = [
    ("US", "NY", "010", "01", "001", "10001", "3", "1", "36", "001", 80000, 30000, "40.7128", "-74.0060"),
    ("US", "CA", "020", "02", "002", "10002", "4", "2", "06", "075", 150000, 50000, "34.0522", "-118.2437"),
    ("US", "IL", "010", "01", "003", "10003", "3", "1", "17", "031", 90000, 35000, "41.8781", "-87.6298")
]

df = spark.createDataFrame(data, columns)

# Call the GUI function
create_gui(columns, df, generate_parquet_file)

# Run the main loop to display the GUI
spark.stop()

                                   