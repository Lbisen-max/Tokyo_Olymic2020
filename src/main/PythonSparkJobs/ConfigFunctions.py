# all necessary functions
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType,DoubleType
from pyspark.sql.functions import *

# defining function so that it can read schema
def schema_function(schema_arg):
    dict_val = {
        "StringType()" : StringType(),
        "IntegerType()" : IntegerType(),
        "TimestampType()" : TimestampType(),
        "DoubleType()" : DoubleType()
    }
    split_values = schema_arg.split(",")
    sch = StructType()
    for i in split_values:
        x = i.split(" ")
        sch.add(x[0],dict_val[x[1]],True)
    return sch











