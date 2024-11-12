from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType

# Start Spark session
spark = SparkSession.builder.appName("FixedWidthRead").getOrCreate()

geo_column_dict = {}

with open("/home/nispri/2010+2000geoheaders.txt") as file:
    for line in file:
        key_value = line.split(',')
        geo_column_dict[key_value[0]] = int(key_value[1].strip())

fields = []

# Define schema with expected field widths for a subset of fields
for key in geo_column_dict.keys():
    fields.append(StructField(key, StringType(), True))

schema = StructType(fields)

# Read the file as a single column of text
data = spark.read.text("./algeo2010.pl")  # Update with the path to argeo2010.pl

# Define function to split the line by byte widths and create Row objects
def split_fixed_width(line):
    row_data = {}
    start = 0
    for column, width in geo_column_dict.items():
        row_data[column] = line[start:start+width].strip()
        start += width
    return Row(**row_data)
    
# Apply parsing logic and create DataFrame
parsed_data = data.rdd.map(lambda row: split_fixed_width(row['value']))
df = spark.createDataFrame(parsed_data, schema)

# Show results
df.show()

