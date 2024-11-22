from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType

def generate_dict(filename):
    c_dict = {}
    with open(filename) as file:
        for line in file:
            key_value = line.split(',')
            c_dict[key_value[0]] = int(key_value[1].strip())
    schema = StructType([StructField(key, StringType(), True) for key in c_dict.keys()])
    return c_dict, schema

geo_column_dict_2010, schema_2010 = generate_dict("data/2010-geoheader.txt")
geo_column_dict_2000, schema_2000 = generate_dict("data/2000-geoheader.txt")

# Define a function to split each line by byte width into columns
def split_fixed_width(line: str, pairs):
    row_data = {}
    start = 0
    for column, width in pairs.items():
        value = line[start:start+width].strip()
        row_data[column] = None if len(value) == 0 else value
        start += width
    return Row(**row_data)

def gen_schema(data, spark, year):
    # Apply the parsing function to the input dataframe
    schema, pairs = (schema_2000, geo_column_dict_2000) if year == 2000 else (schema_2010, geo_column_dict_2010)
    parsed_data = data.rdd.map(lambda row: split_fixed_width(row['value'], pairs))
    df = spark.createDataFrame(parsed_data, schema)
    return df

# Example usage
# Assuming 'data' is a DataFrame with a single column 'value'
# data = spark.read.text("/user/nispri/algeo2010.pl")
# df = gen_schema(data)
# df.show()

