// Databricks notebook source
// MAGIC %sql
// MAGIC SELECT current_metastore()

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE CATALOG IF NOT EXISTS dev;
// MAGIC CREATE SCHEMA IF NOT EXISTS dev.hospital_treatment;

// COMMAND ----------

// MAGIC %run "./ExtVolume-ExtTable-functions"

// COMMAND ----------

val listOfLocations = List(
  List(
    "`PATIENTS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/patients/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PATIENTS IN LANDING CONTAINER"
  ),
  List(
    "`PROCEDURES_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/procedures/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PROCEDURES IN LANDING CONTAINER"
  ),
  List(
    "`PAYERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/payers/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PAYERS IN LANDING CONTAINER"
  ),
  List(
    "`ORGANIZATION_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/organization/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Organization IN LANDING CONTAINER"
  ),
   List(
    "`ENCOUNTERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/encounters/",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Encounters IN LANDING CONTAINER"
  )
)

call_create_external_location(listOfLocations,spark)

// COMMAND ----------

val listOfLocations = List(
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PATIENTS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/patients/"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PROCEDURES_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/procedures/"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PAYERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/payers/"
  )
  ,
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`ORGANIZATION_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/organization/"
  ),
   List(
    "`DEV`",
    "`hospital_treatment`",
    "`ENCOUNTERS_LANDING`",
    "abfss://hos-project-landing@hospprojectstorage.dfs.core.windows.net/encounters/"
  )
)
call_create_external_volume(listOfLocations,spark)

// COMMAND ----------


