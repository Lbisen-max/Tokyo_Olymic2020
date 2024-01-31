from pyspark.sql import SparkSession
import configparser
from src.main.PythonSparkJobs.ConfigFunctions import schema_function
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Initialing the SparkSession
spark = SparkSession.builder.appName("Datainestion").master("local").getOrCreate()

# Reading configs
config = configparser.ConfigParser()
config.read(r'../ProjectConfig/Config.ini') # set the relative path

# reading the path from config file
athletsDataIngest = config.get('path','athletsDataIngest')
coachesDataIngest = config.get('path','coachesDataIngest')
medalsDataIngest = config.get('path','medalsDataIngest')
medalsTotalDataIngest = config.get('path','medalsTotalDataIngest')
technicalOfficialsDataIngest = config.get('path','technicalOfficialsDataIngest')
outputlocation = config.get("path","transFormDataPath")

# reading the schema from config.ini file
athletsDataSchema = config.get('schema','athletsDataSchema')
coachesDataSchema = config.get('schema','coachesDataSchema')
medalsDataSchema = config.get('schema','medalsDataSchema')
medalsTotalDataSchema = config.get('schema','medalsTotalDataSchema')
technicalOfficialsDataSchema = config.get('schema','technicalOfficialsDataSchema')

# converting schema using config schema function
athletsSchema = schema_function(athletsDataSchema)
coachesSchema = schema_function(coachesDataSchema)
medalsSchema = schema_function(medalsDataSchema)
medalsTotalSchema = schema_function(medalsTotalDataSchema)
technicalOfficialsSchema = schema_function(technicalOfficialsDataSchema)


# reading the data using pyspark
athletLandingFile = spark.read.format("csv").schema(athletsSchema).option("header", True).load(athletsDataIngest)
coachesLandingFile = spark.read.format("csv").schema(coachesSchema).option("header", True).load(coachesDataIngest)
medalLandingFile = spark.read.format("csv").schema(medalsSchema).option("header", True).load(medalsDataIngest)
medalTotalLandingFile = spark.read.format("csv").schema(medalsTotalSchema).option("header", True).load(medalsTotalDataIngest)
technicalOfficialsLandingFile = spark.read.format("csv").schema(technicalOfficialsSchema).option("header", True).load(technicalOfficialsDataIngest)


# Data Transformation

# There are many null values hence have to drop such columns as if we fill null values
# then this will not be a generalize value

athletLandingFile  = athletLandingFile.drop('birth_place','birth_country','residence_place','residence_country','url','height_m/ft')
athletLandingFile  = athletLandingFile.na.drop()
coachesLandingFile  = coachesLandingFile.drop('url')
coachesLandingFile  = coachesLandingFile.na.drop()
medalLandingFile = medalLandingFile.drop('athlete_link')
technicalOfficialsLandingFile = technicalOfficialsLandingFile.drop('url')

# Birthdate is with time stamp hence removing timestamp and keeping only dob
athletLandingFile = athletLandingFile.withColumn('birth_date', to_date("birth_date", 'dd/MM/yyyy'))
coachesLandingFile = coachesLandingFile.withColumn('birth_date',to_date("birth_date",'dd/MM/yyyy'))
medalLandingFile = medalLandingFile.withColumn("medal_date",to_date("medal_date","dd/MM/yyyy"))\
                    .withColumn("athlete_sex",regexp_replace("athlete_sex","X","F"))
technicalOfficialsLandingFile = technicalOfficialsLandingFile.withColumn('birth_date',to_date("birth_date"))

print("**********************athlet data***************")
athletLandingFile.show(5)
print("**********************coaches data***************")
coachesLandingFile.show(5)
print("**********************medal data***************")
medalLandingFile.show(5)
print("**********************medalTotal data***************")
medalTotalLandingFile.show(5)
print("**********************technical official data***************")
technicalOfficialsLandingFile.show(5)

# Prining schema
athletLandingFile.printSchema()
coachesLandingFile.printSchema()
medalLandingFile.printSchema()
medalTotalLandingFile.printSchema()
technicalOfficialsLandingFile.printSchema()

# Writing the data into local

athletLandingFile.write\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputlocation+"athletLandingFile")

coachesLandingFile.write\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputlocation+"coachesLandingFile")

medalLandingFile.write\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputlocation+"medalLandingFile")


medalTotalLandingFile.write\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputlocation+"medalTotalLandingFile")

technicalOfficialsLandingFile.write\
    .option("header",True)\
    .mode("overwrite")\
    .csv(outputlocation+"technicalOfficialsLandingFile")

#### Writing the data into mysql server


athletLandingFile.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/tokyoolympic2020',
      driver='com.mysql.jdbc.Driver',
      dbtable='athlets',
      user='root',
      password='root').mode('overwrite').save()


coachesLandingFile.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/tokyoolympic2020',
      driver='com.mysql.jdbc.Driver',
      dbtable='coachs',
      user='root',
      password='root').mode('overwrite').save()


medalLandingFile.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/tokyoolympic2020',
      driver='com.mysql.jdbc.Driver',
      dbtable='medals',
      user='root',
      password='root').mode('overwrite').save()

medalTotalLandingFile.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/tokyoolympic2020',
      driver='com.mysql.jdbc.Driver',
      dbtable='totalmedals',
      user='root',
      password='root').mode('overwrite').save()


technicalOfficialsLandingFile.write.format('jdbc').options(
      url='jdbc:mysql://localhost:3306/tokyoolympic2020',
      driver='com.mysql.jdbc.Driver',
      dbtable='technicalpeopel',
      user='root',
      password='root').mode('overwrite').save()

print("transform data write into new location : MYSQL server")

