# Databricks notebook source
# MAGIC %md
# MAGIC ## Librerías

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración del Storage Account

# COMMAND ----------

spark = SparkSession.builder.appName('DataFrame').getOrCreate()

# COMMAND ----------

# MAGIC %run "./conection"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar los DataSets del Storage Account

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Crear los Path

# COMMAND ----------

raw = "abfss://edgar-steik-integrador@formacionanalitica.dfs.core.windows.net/raw"

# COMMAND ----------

trusted = "abfss://edgar-steik-integrador@formacionanalitica.dfs.core.windows.net/trusted"

# COMMAND ----------

PATH_circuits = raw + "/dbo.circuits.csv"
PATH_constructor_results = raw + "/dbo.constructor_results.csv"
PATH_constructor_standings = raw + "/dbo.constructor_standings.csv"
PATH_constructors = raw + "/dbo.constructors.csv"
PATH_driver_standings = raw + "/dbo.driver_standings.csv"
PATH_drivers = raw + "/dbo.drivers.csv"
PATH_lap_times = raw + "/dbo.lap_times.csv"
PATH_pits_stops = raw + "/dbo.pit_stops.csv"
PATH_qualifying = raw + "/dbo.qualifying.csv"
PATH_races = raw + "/dbo.races.csv"
PATH_results = raw + "/dbo.results.csv"
PATH_seasons = raw + "/dbo.seasons.csv"
PATH_sprint_results = raw + "/dbo.sprint_results.csv"
PATH_status = raw + "/dbo.status.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cargar los datasets

# COMMAND ----------

circuits_df = spark.read.load(PATH_circuits, format='csv', header='true')
constructor_results_df = spark.read.load(PATH_constructor_results, format='csv', header='true')
constructor_standings_df = spark.read.load(PATH_constructor_standings, format='csv', header='true')
constructors_df = spark.read.load(PATH_constructors, format='csv', header='true')
driver_standings_df = spark.read.load(PATH_driver_standings, format='csv', header='true')
drivers_df = spark.read.load(PATH_drivers, format='csv', header='true')
lap_times_df = spark.read.load(PATH_lap_times, format='csv', header='true')
pit_stops_df = spark.read.load(PATH_pits_stops, format='csv', header='true')
qualifying_df = spark.read.load(PATH_qualifying, format='csv', header='true')
races_df = spark.read.load(PATH_races, format='csv', header='true')
results_df = spark.read.load(PATH_results, format='csv', header='true')
seasons_df = spark.read.load(PATH_seasons, format='csv', header='true')
sprint_results_df = spark.read.load(PATH_sprint_results, format='csv', header='true')
status_df = spark.read.load(PATH_status, format='csv', header='true')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaciones:

# COMMAND ----------

circuits_df = circuits_df.drop("url","lat","lng","alt", "circuitRef")
circuits_df = circuits_df.withColumn("circuitId", f.col("circuitId").cast("int"))
circuits_df = circuits_df.toDF(*('cir_circuitId', 'cir_name', 'cir_location', 'cir_country'))
circuits_df = circuits_df.distinct()

# COMMAND ----------

constructor_results_df = constructor_results_df.withColumn("constructorResultsId", f.col("constructorResultsId").cast("int"))
constructor_results_df = constructor_results_df.withColumn("raceId", f.col("raceId").cast("int"))
constructor_results_df = constructor_results_df.withColumn("constructorId", f.col("constructorId").cast("int"))

constructor_results_df = constructor_results_df.withColumn("points", f.col("points").cast("float"))

constructor_results_df = constructor_results_df.toDF(*("constRes_constructorResultsId", "constRes_raceId", "constRes_constructorId", "constRes_points","constRes_status"))
constructor_results_df = constructor_results_df.distinct()

# COMMAND ----------

constructor_standings_df = constructor_standings_df.drop("positionText")
constructor_standings_df = constructor_standings_df.withColumn("constructorStandingsId", f.col("constructorStandingsId").cast("int"))
constructor_standings_df = constructor_standings_df.withColumn("raceId", f.col("raceId").cast("int"))
constructor_standings_df = constructor_standings_df.withColumn("constructorId", f.col("constructorId").cast("int"))

constructor_standings_df = constructor_standings_df.withColumn("points", f.col("points").cast("float"))

constructor_standings_df = constructor_standings_df.withColumn("position", f.col("position").cast("int"))
constructor_standings_df = constructor_standings_df.withColumn("wins", f.col("wins").cast("int"))

constructor_standings_df = constructor_standings_df.toDF(*("consSt_constructorStandingsId", "consSt_raceId", "consSt_constructorId", "consSt_points","consSt_position","constSt_wins"))
constructor_standings_df = constructor_standings_df.distinct()

# COMMAND ----------

constructors_df = constructors_df.drop("url","constructorRef")
constructors_df = constructors_df.withColumn("constructorId", f.col("constructorId").cast("int"))

constructors_df = constructors_df.toDF(*("const_constructorId", "const_name", "const_nationality"))
constructors_df = constructors_df.distinct()


# COMMAND ----------

driver_standings_df = driver_standings_df.drop("positionText")
driver_standings_df = driver_standings_df.withColumn("driverStandingsId", f.col("driverStandingsId").cast("int"))
driver_standings_df = driver_standings_df.withColumn("raceId", f.col("raceId").cast("int"))
driver_standings_df = driver_standings_df.withColumn("driverId", f.col("driverId").cast("int"))
driver_standings_df = driver_standings_df.withColumn("position", f.col("position").cast("int"))
driver_standings_df = driver_standings_df.withColumn("wins", f.col("wins").cast("int"))
driver_standings_df = driver_standings_df.withColumn("points", f.col("points").cast("float"))

driver_standings_df = driver_standings_df.toDF(*("drivSt_driverStandingsId","drivSt_raceId","drivSt_driverId","drivSt_points","drivSt_position","drivSt_wins"))
driver_standings_df = driver_standings_df.distinct()

# COMMAND ----------

drivers_df = drivers_df.drop("url","number","code", "driverRef","dob")
drivers_df = drivers_df.withColumn("driverId", f.col("driverId").cast("int"))

drivers_df = drivers_df.toDF(*("driv_driverId","driv_forename","driv_surname","driv_nationality"))
driver_standings_df = driver_standings_df.distinct()

# COMMAND ----------

lap_times_df = lap_times_df.drop("milliseconds")

lap_times_df = lap_times_df.withColumn("raceId", f.col("raceId").cast("int"))
lap_times_df = lap_times_df.withColumn("driverId", f.col("driverId").cast("int"))
lap_times_df = lap_times_df.withColumn("lap", f.col("lap").cast("int"))
lap_times_df = lap_times_df.withColumn("position", f.col("position").cast("int"))

lap_times_df = lap_times_df.toDF(*("lapt_raceId","lapt_driverId","lapt_lap","lapt_position","lapt_time"))
lap_times_df = lap_times_df.distinct()


# COMMAND ----------

pit_stops_df = pit_stops_df.drop("milliseconds") #los segundos los tengo ya en duration.

pit_stops_df = pit_stops_df.withColumn("raceId", f.col("raceId").cast("int"))
pit_stops_df = pit_stops_df.withColumn("driverId", f.col("driverId").cast("int"))
pit_stops_df = pit_stops_df.withColumn("stop", f.col("stop").cast("int"))
pit_stops_df = pit_stops_df.withColumn("lap", f.col("lap").cast("int"))

pit_stops_df = pit_stops_df.toDF(*("pits_raceId","pits_driverId","pits_stop","pits_lap","pits_time","pits_duration"))
pit_stops_df = pit_stops_df.distinct()

# COMMAND ----------

qualifying_df = qualifying_df.drop("number")

qualifying_df = qualifying_df.withColumn("qualifyId", f.col("qualifyId").cast("int"))
qualifying_df = qualifying_df.withColumn("raceId", f.col("raceId").cast("int"))
qualifying_df = qualifying_df.withColumn("driverId", f.col("driverId").cast("int"))
qualifying_df = qualifying_df.withColumn("constructorId", f.col("constructorId").cast("int"))
qualifying_df = qualifying_df.withColumn("position", f.col("position").cast("int"))

qualifying_df = qualifying_df.toDF(*("quali_qualify","quali_raceId","quali_driverId","queali_constructorId","quali_position","quali_q1","quali_q2","quali_q3"))
qualifying_df = qualifying_df.distinct()

# COMMAND ----------

status_df = status_df.withColumn("statusId", f.col("statusId").cast("int"))

# COMMAND ----------

races_df = races_df.drop("url","fp1_date","fp1_time","fp2_date","fp2_time","fp3_date","fp3_time","sprint_date","sprint_time","quali_time","quali_date")

races_df = races_df.withColumn("raceId", f.col("raceId").cast("int"))
races_df = races_df.withColumn("year", f.col("year").cast("int"))
races_df = races_df.withColumn("round", f.col("round").cast("int"))
races_df = races_df.withColumn("circuitId", f.col("circuitId").cast("int"))

races_df = races_df.toDF(*("races_raceId","races_year","races_round","races_circuitId","races_name","races_date","races_time"))
races_df = races_df.distinct()

# COMMAND ----------

results_df = results_df.drop("positionText","position","number")
results_df = results_df.withColumn("resultId", f.col("resultId").cast("int"))
results_df = results_df.withColumn("raceId", f.col("raceId").cast("int"))
results_df = results_df.withColumn("constructorId", f.col("ConstructorId").cast("int"))
results_df = results_df.withColumn("driverId", f.col("driverId").cast("int"))
results_df = results_df.withColumn("positionOrder", f.col("positionOrder").cast("int"))
results_df = results_df.withColumn("laps", f.col("laps").cast("int"))
results_df = results_df.withColumn("grid", f.col("grid").cast("int"))
results_df = results_df.withColumn("fastestLap", f.col("fastestLap").cast("int"))
results_df = results_df.withColumn("rank", f.col("rank").cast("int"))
results_df = results_df.withColumn("statusId", f.col("statusId").cast("int"))
results_df = results_df.withColumn("milliseconds", f.col("milliseconds").cast("int"))
results_df = results_df.withColumn("points", f.col("points").cast("float"))
results_df = results_df.withColumn("time", when(col("time") == "\\N", -1).otherwise(col("time")))
results_df = results_df.withColumn("fastestLapTime", when(col("fastestLapTime") == "\\N", -1).otherwise(col("fastestLapTime")))

results_df = results_df.toDF(("res_resultId","res_raceId","res_driverId","res_constructorId","res_grid","res_positionOrder","res_points","res_laps","res_time","res_milliseconds","res_fastestLap","res_rank","res_fastestLapTime","res_fastestLapSpeed","res_statusId"))

results_df = results_df.distinct()

# COMMAND ----------

sprint_results_df = sprint_results_df.drop("positionText","positionOrder","number")
sprint_results_df = sprint_results_df.withColumn("resultsId", f.col("raceId").cast("int"))
sprint_results_df = sprint_results_df.withColumn("constructorId", f.col("ConstructorId").cast("int"))
sprint_results_df = sprint_results_df.withColumn("driverId", f.col("driverId").cast("int"))
sprint_results_df = sprint_results_df.withColumn("position", f.col("position").cast("int"))
sprint_results_df = sprint_results_df.withColumn("laps", f.col("laps").cast("int"))
sprint_results_df = sprint_results_df.withColumn("grid", f.col("grid").cast("int"))
sprint_results_df = sprint_results_df.withColumn("fastestLap", f.col("fastestLap").cast("int"))
sprint_results_df = sprint_results_df.withColumn("statusId", f.col("statusId").cast("int"))
sprint_results_df = sprint_results_df.withColumn("milliseconds", f.col("milliseconds").cast("int"))
sprint_results_df = sprint_results_df.withColumn("points", f.col("points").cast("float"))

sprint_results_df = sprint_results_df.distinct()

# COMMAND ----------

seasons_df = seasons_df.drop("url")
seasons_df = seasons_df.withColumn("year", f.col("year").cast("int"))

seasons_df = seasons_df.withColumnRenamed('year', 'seasons_year')


seasons_df = seasons_df.distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Guardado del DataFrame en formato Parquet

# COMMAND ----------

circuits_df.write.save(
    path= f"{trusted}/circuits_df",
    format = "parquet",
    mode = "overwrite",
)
constructor_results_df.write.save(
    path = f"{trusted}/constructor_results_df",
    format = "parquet",
    mode = "overwrite",
)
constructor_standings_df.write.save(
    path = f"{trusted}/constructor_standings_df",
    format = "parquet",
    mode = "overwrite",
)
constructors_df.write.save(
    path = f"{trusted}/constructors_df",
    format = "parquet",
    mode = "overwrite",
)
driver_standings_df.write.save(
    path = f"{trusted}/driver_standings_df",
    format = "parquet",
    mode = "overwrite",
)
drivers_df.write.save(
    path = f"{trusted}/drivers_df",
    format = "parquet",
    mode = "overwrite",
)
lap_times_df.write.save(
    path = f"{trusted}/lap_times_df",
    format = "parquet",
    mode = "overwrite",
)
pit_stops_df.write.save(
    path = f"{trusted}/pit_stops_df",
    format = "parquet",
    mode = "overwrite",
)
qualifying_df.write.save(
    path = f"{trusted}/qualifying_df",
    format = "parquet",
    mode = "overwrite",
)
races_df.write.save(
    path = f"{trusted}/races_df",
    format = "parquet",
    mode = "overwrite",
)
results_df.write.save(
    path = f"{trusted}/results_df",
    format = "parquet",
    mode = "overwrite",
)
seasons_df.write.save(
    path = f"{trusted}/seasons_df",
    format = "parquet",
    mode = "overwrite",
)
sprint_results_df.write.save(
    path = f"{trusted}/sprint_results_df",
    format = "parquet",
    mode = "overwrite",
)
status_df.write.save(
    path = f"{trusted}/status_df",
    format = "parquet",
    mode = "overwrite",
)

# COMMAND ----------

