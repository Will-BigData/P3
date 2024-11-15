from pyspark.sql import SparkSession


def select_specified_columns(df, columns_file):
    # Step 1: Read the list of specified columns
    with open(columns_file, "r") as f:
        specified_columns = [line.strip() for line in f.readlines()]
    
    # Step 2: Select only the specified columns from the input DataFrame
    df_selected = df.select(*list(set(specified_columns) & set(df.columns)))

    return df_selected


# # Initialize Spark session
# spark = SparkSession.builder.appName("SelectColumnsTest").getOrCreate()

# # Sample DataFrame for demonstration
# data = [
#     ("US", "NY", "010", "01", "001", "10001", "3", "1", "36", "001", 80000, 30000, "40.7128", "-74.0060"),
#     ("US", "CA", "020", "02", "002", "10002", "4", "2", "06", "075", 150000, 50000, "34.0522", "-118.2437"),
#     ("US", "IL", "010", "01", "003", "10003", "3", "1", "17", "031", 90000, 35000, "41.8781", "-87.6298")
# ]
# columns = ["FILEID", "STUSAB", "SUMLEV", "GEOCOMP", "CHARITER", "LOGRECNO", "REGION", "DIVISION",
#            "STATE", "COUNTY", "POP100", "HU100", "INTPTLAT", "INTPTLON"]

# # Creating the DataFrame
# df = spark.createDataFrame(data, columns)

# # Specify the columns file path
# columns_file = "columns_file.txt"

# # Call the function to get the selected columns
# df_selected = select_specified_columns(df, columns_file)

# # Show the resulting DataFrame with selected columns
# df_selected.show()

# # Stop the Spark session
# spark.stop()