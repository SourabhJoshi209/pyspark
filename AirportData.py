# Databricks notebook source
File uploaded to /FileStore/tables/flights_small.csv
File uploaded to /FileStore/tables/airports.csv
File uploaded to /FileStore/tables/planes.csv

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, when, desc, max, asc, avg, round

# COMMAND ----------

spark = SparkSession.builder.appName("Airport_Data").getOrCreate()

# COMMAND ----------

airports_df = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/airports.csv')

display(airports_df)

# COMMAND ----------

airports_df.printSchema()

# COMMAND ----------

flights_df = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/flights_small.csv')

display(flights_df)

# COMMAND ----------

flights_df.printSchema()

# COMMAND ----------

flights_df.columns

# COMMAND ----------

flights_df = flights_df.withColumn("air_time", col("air_time").cast("Integer"))


# COMMAND ----------

flights_df = flights_df.withColumn("duration_hrs", round(col("air_time")/60, 1))
flights_df.show()

# COMMAND ----------

## Filter long flights

long_flights = flights_df.filter(col("distance") > 1000)
long_flights.show()

# COMMAND ----------

# Find the average speed of the flights
flights_df = flights_df.withColumn("avg_speed", round(col("distance")/col("duration_hrs"),2))
flights_df.show()

# COMMAND ----------

#Find the length of the shortest (in terms of distance) flight that left PDX 
flights_df.filter(col("origin")=='PDX').groupBy().min('distance').show()

# COMMAND ----------

#Find the length of the longest (in terms of distance) flight that left PDX
flights_df.filter(col("origin")=='PDX').groupBy().max('distance').show()



# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# find the lenght of the longest and shortest flight along with its origin dest avg_speed
# flights_df.groupBy('origin', 'dest', 'avg_speed').agg(max('distance').alias('max_distance')).orderBy('max_distance',ascending=0).show()

windowSpec = Window.partitionBy("origin", "dest")

flights_df.withColumn("max_distance", max(col('distance')).over(windowSpec)).select("origin","dest","max_distance").show()



# COMMAND ----------

planes_df = spark.read.format('csv').option('header','true').option('inferSchema','true').load('/FileStore/tables/planes.csv')

display(planes_df)

# COMMAND ----------

planes_df.printSchema()
