from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def generateGeoSegmentSchema():
    geoSegment =  open("data/2020_FieldNames_GeoSegment.csv")
    geoLine = geoSegment.readline().strip()
    geo_columns = geoLine.split(',')

    geo_int_columns = ["LOGRECNO", "AREALAND", "AREAWATR", "POP100", "HU100"]
    geo_fields_array = []

    for column in geo_columns:
        if column in geo_int_columns:
            geo_fields_array.append(StructField(column, IntegerType(), True))
        else:
            geo_fields_array.append(StructField(column, StringType(), True))

    schema = StructType(geo_fields_array)

    return schema

def generateSegment1Schema():
    segment = open("data/2020_FieldNames_Segment1.csv")
    line = segment.readline().strip()
    columns = line.split(',')

    segment_string_columns = ["FILEID", "CHARITER", "CIFSN"]
    fields_array = []

    for column in columns:
        if column in segment_string_columns:
            fields_array.append(StructField(column, StringType(), True))
        else:
            fields_array.append(StructField(column, IntegerType(), True))

    schema = StructType(fields_array)

    return schema

def generateSegment2Schema():
    segment = open("data/2020_FieldNames_Segment2.csv")
    line = segment.readline().strip()
    columns = line.split(',')

    segment_string_columns = ["FILEID", "CHARITER", "CIFSN"]
    fields_array = []

    for column in columns:
        if column in segment_string_columns:
            fields_array.append(StructField(column, StringType(), True))
        else:
            fields_array.append(StructField(column, IntegerType(), True))

    schema = StructType(fields_array)

    return schema

def generateSegment3Schema():
    segment = open("data/2020_FieldNames_Segment3.csv")
    line = segment.readline().strip()
    columns = line.split(',')

    segment_string_columns = ["FILEID", "CHARITER", "CIFSN"]
    fields_array = []

    for column in columns:
        if column in segment_string_columns:
            fields_array.append(StructField(column, StringType(), True))
        else:
            fields_array.append(StructField(column, IntegerType(), True))

    schema = StructType(fields_array)

    return schema
