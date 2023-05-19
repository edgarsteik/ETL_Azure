# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

####Conexión al Data Lake


clave = 'Ldr/3SzOCA6Pss7IPcHViadAP62GNwa7rO34egvAR/oWgIvbttUeH8/hp80EjPIrg/5KISh10PIr+AStMPRzGw=='
spark.conf.set("fs.azure.account.key.formacionanalitica.dfs.core.windows.net", clave)
container = 'edgar-steik-integrador'
datalake = 'formacionanalitica'

####PATHS a raw y trusted

raw_data = f"abfss://{container}@{datalake}.dfs.core.windows.net/Integrador/raw_data"


trusted = f"abfss://{container}@{datalake}.dfs.core.windows.net/trusted"

#### PATHS a las tablas de interés

racesPATH = trusted + "/races_df/"
constructorsPATH = trusted + "/constructors_df/"
driversPATH = trusted + "/drivers_df/"
circuitsPATH = trusted + "/circuits_df/"
resultsPATH = trusted + "/results_df/"
statusPATH = trusted + "/status_df/"

races = spark.read.parquet(racesPATH)
constructors = spark.read.parquet(constructorsPATH)
drivers = spark.read.parquet(driversPATH)
circuits = spark.read.parquet(circuitsPATH)
results = spark.read.parquet(resultsPATH)
status = spark.read.parquet(statusPATH)

races.createOrReplaceTempView("races")
constructors.createOrReplaceTempView("constructors")
drivers.createOrReplaceTempView("drivers")
circuits.createOrReplaceTempView("circuits")
results.createOrReplaceTempView("results")
status.createOrReplaceTempView("status")

#### JOIN de tablas de interés


tablon_formula_1 = spark.sql("""
SELECT * 
FROM results
LEFT JOIN races on races.races_raceId = results.res_raceId
LEFT JOIN drivers on drivers.driv_driverId = results.res_driverId
LEFT JOIN status on  status.statusId = results.res_statusId 
LEFT JOIN circuits on circuits.cir_circuitId = races.races_circuitId
WHERE races.races_year > 2017
""")


tablon_formula_1.display()

#### Conexión JDBC


jdbcHostname = "integrador.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "DWIntegrador"
jdbcUsername = "administrador"
jdbcPassword = "Formacion2"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
 
connectionProperties = {
  "user" : jdbcUsername,
  "password" : jdbcPassword,
  "driver" : jdbcDriver
}
jdbcUrl = "jdbc:sqlserver://{0}:{1};database={2};user={3}@integrador;password={4};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;".format(jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword)

#### Guardado del tablón


tablon_formula_1.write.mode("overwrite")\
.format("jdbc")\
.option("driver", jdbcDriver)\
.option("url", jdbcUrl)\
.option("dbtable", "dbo.Formula1_Edgar")\
.save()


