
from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import (DateType, IntegerType, StringType, StructField, StructType)

spark = SparkSession.builder.appName("Auto Report–Spark").getOrCreate()
sc = spark.sparkContext

schema = StructType([
    StructField('incident_id', IntegerType(), True),
    StructField('incident_type', StringType(), True),
    StructField('vin_num', StringType(), True),
    StructField('make', StringType(), True),
    StructField('model', StringType(), True),
    StructField('year', StringType(), True),
    StructField('incident_date', DateType(), True),
    StructField('desc', StringType(), True)
])

df = spark.read.format("csv") \
    .option("header", False) \
    .schema(schema) \
    .load("/Users/mallory/Desktop/DataEngineering/Springboard/DistComp/hadoop_auto/data.csv")
df.show()


def extract_key_vin_value(x):
    """:param x: data source loaded into SparkSession,
        :output: dictionary tuple with mapping values to be transformed into MapType"""

    vin_number = x.vin_num
    make = x.make
    year = x.year
    model = x.model
    incident_id = x.incident_id
    incident_type = x.incident_type
    incident_date = x.incident_date
    desc = x.desc

    return (vin_number, {"make": make, "year": year, "model": model, "incident_id": incident_id, "incident_type": incident_type, "incident_date": incident_date, "desc": desc})


vin_kv = df.rdd.map(lambda x: extract_key_vin_value(x))




def populate_make(data_rdd):

    output = []

    for member in data_rdd:

        incident_id = member["incident_id"]
        incident_type = member["incident_type"]
        incident_date = member["incident_date"]
        desc = member["desc"]

        if member["make"] != None:
            make = member["make"]
        if member["year"] != None:
            year = member["year"]
        if member["model"] != None:
            model = member["model"]
        
        output.append({key: {"make": make, "year": year, "model": model, "incident_id": incident_id,
                      "incident_type": incident_type, "incident_date": incident_date, "desc": desc}})
    return output


enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))
#print(type(enhance_make))  # RDD
#print(enhance_make.collect())  # list of dictionaries


def extract_key_make_value(data):

    # print(data)
    for key, value in data.items():
        # print(key, value)
        if value["incident_type"] == 'A':
            count = ((value["make"]+value["year"]), 1)
            # print(count)
        else:
            count = ((value["make"]+value["year"]), 0)
        # returns rdd
        return count


# QA
# extract_key_make_value(enhance_make)
# enhance_make.map(lambda x: extract_key_make_value(x)).collect()

make_kv = enhance_make.map(lambda x: extract_key_make_value(x))

# QA
#type(make_kv)
#print(make_kv.collect())

make_year_rdd = make_kv.reduceByKey(lambda key, count: key+count)

# QA
print(type(make_year_rdd))
print(make_year_rdd.collect())

final_df = make_year_rdd.toDF(schema=["make_year", "accident_count"])
# final_df.show()

final_df.write.mode("append").format("csv").save("./report_csv")
