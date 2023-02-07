
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql.functions import *


import argparse

parser = argparse.ArgumentParser()

parser.add_argument('--input_covid_data', required=True)
parser.add_argument('--input_vaccination_data', required=True)
parser.add_argument('--output_covid_data', required=True)
parser.add_argument('--output_vaccination_data', required=True)

args = parser.parse_args()

input_covid_data = args.input_covid_data
input_vaccination_data = args.input_vaccination_data
output_covid_data = args.output_covid_data
output_vaccination_data = args.output_vaccination_data


spark = SparkSession.builder \
    .appName('process') \
    .getOrCreate()



covid_schema = types.StructType([
    types.StructField("Date_reported", types.TimestampType(), True),
    types.StructField("Country_code", types.StringType(), True),
    types.StructField("Country", types.StringType(), True),
    types.StructField("WHO_region", types.StringType(), True),
    types.StructField("New_cases", types.IntegerType(), True),
    types.StructField("Cumulative_cases", types.IntegerType(), True),
    types.StructField("New_deaths", types.IntegerType(), True),
    types.StructField("Cumulative_deaths", types.IntegerType(), True)
])


covid_df = spark.read \
        .option("header", "true") \
        .schema(covid_schema) \
        .csv(input_covid_data)


covid_df = covid_df.withColumnRenamed('WHO_region', 'WHO_REGION')

covid_df.write.parquet(output_covid_data, mode='overwrite')



vaccination_schema = types.StructType([
    types.StructField("COUNTRY", types.StringType(), True),
    types.StructField("ISO3", types.StringType(), True),
    types.StructField("WHO_REGION", types.StringType(), True),
    types.StructField("DATA_SOURCE", types.StringType(), True),
    types.StructField("DATE_UPDATED", types.TimestampType(), True),
    types.StructField("TOTAL_VACCINATIONS", types.IntegerType(), True),
    types.StructField("PERSONS_VACCINATED_1PLUS_DOSE", types.IntegerType(), True),
    types.StructField("TOTAL_VACCINATIONS_PER100", types.IntegerType(), True),
    types.StructField("PERSONS_VACCINATED_1PLUS_DOSE_PER100", types.IntegerType(), True),
    types.StructField("PERSONS_FULLY_VACCINATED", types.IntegerType(), True),
    types.StructField("PERSONS_FULLY_VACCINATED_PER100", types.IntegerType(), True),
    types.StructField("VACCINES_USED", types.StringType(), True),
    types.StructField("FIRST_VACCINE_DATE", types.TimestampType(), True),
    types.StructField("NUMBER_VACCINES_TYPES_USED", types.IntegerType(), True),
    types.StructField("PERSONS_BOOSTER_ADD_DOSE", types.IntegerType(), True),
    types.StructField("PERSONS_BOOSTER_ADD_DOSE_PER100", types.IntegerType(), True),
])


vaccination_df = spark.read \
        .option("header", "true") \
        .schema(vaccination_schema) \
        .csv(input_vaccination_data)


vaccination_df = vaccination_df.na.fill(0,["NUMBER_VACCINES_TYPES_USED"]) \
    .na.fill("unknown",["VACCINES_USED"]) \
    .na.fill(-1, ["TOTAL_VACCINATIONS"]) \
    .na.fill(-1, ["PERSONS_VACCINATED_1PLUS_DOSE"]) \
    .na.fill(-1, ["TOTAL_VACCINATIONS_PER100"]) \
    .na.fill(-1, ["PERSONS_VACCINATED_1PLUS_DOSE_PER100"]) \
    .na.fill(-1, ["PERSONS_FULLY_VACCINATED"]) \
    .na.fill(-1, ["PERSONS_FULLY_VACCINATED_PER100"]) \
    .na.fill(-1, ["PERSONS_BOOSTER_ADD_DOSE"]) \
    .na.fill(-1, ["PERSONS_BOOSTER_ADD_DOSE_PER100"])


vaccination_df.write.parquet(output_vaccination_data, mode='overwrite')