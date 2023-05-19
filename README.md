#### Proceso de ELT en Azure

**Presentación del caso:**

‘Analítica Noviembre’ S.A es una empresa que se basa en el trabajo de promover la seguridad, integridad, cuidado y bien estar de las personas, dicha empresa solicitó 3 modelos analíticos para poder tomar una decisión y llevar a cabo una acción de negocio. El primer modelo analítico analiza los pilotos más propensos a riesgos debido a choques o colisiones, el segundo modelo analítico se basa en los problemas del auto que tuvo un determinado piloto y el último modelo analiza los circuitos en donde se ocasionan más choques o colisiones, es decir, los circuitos con más probabilidad de un accidente. 

**Descripción del proceso realizado:**

El origen de los datos es una base de datos de SQL Server (Azure SQL) en formato .csv, los cuáles son extraídos mediante un pipeline creado en la herramienta Azure Data Factory. Dicho pipeline está conformado por un LockUp, el cual se utiliza para buscar y leer los datos, posteriormente se utiliza un ForEach el cual nos permite iterar sobre la lista de elementos y ejecutar un conjunto de acciones o transformaciones para cada uno de ellos, en este caso es una actividad de Copy Data. 
La primera notebook de Azure Databricks llamada “Raw to Trusted” se cargan los datos alojados en el Data Lake para su posterior manipulación y transformación utilizando **PySpark**. Luego de realizar dichas transformaciones se guardan los data frames en formato parquet en un Data Lake (Azure Data Lake Gen2).
En la segunda noteebok llamada “Trusted to Refined” se utiliza **SparkSQL** para realizar los JOINS correspondientes para unir las tablas que son de interés para mi análisis, el cual se hará mediante la herramienta de Microsoft Power BI.
