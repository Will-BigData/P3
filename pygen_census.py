from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

# Start Spark session
spark = SparkSession.builder.appName("FixedWidthRead").getOrCreate()

geo_column_dict = {}

with open("data/2010+2000geoheaders.txt") as file:
    for line in file:
        key_value = line.split(',')
        geo_column_dict[key_value[0]] = int(key_value[1].strip())

# Define the schema based on the column widths
fields = [StructField(key, StringType(), True) for key in geo_column_dict.keys()]
schema = StructType(fields)

# Define a function to split each line by byte width into columns
def split_fixed_width(line):
    row_data = {}
    start = 0
    for column, width in geo_column_dict.items():
        row_data[column] = line[start:start+width].strip()
        start += width
    return Row(**row_data)

def gen_schema(data):
    # Apply the parsing function to the input dataframe
    parsed_data = data.rdd.map(lambda row: split_fixed_width(row['value']))
    df = spark.createDataFrame(parsed_data, schema)

    return df

# Example usage
# Assuming 'data' is a DataFrame with a single column 'value'
data = spark.read.text("/user/nispri/algeo2010.pl")
df = gen_schema(data)
df.show()

