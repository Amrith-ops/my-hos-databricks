// Databricks notebook source
// i dont want to write the same functions again and again in different notebooks and duplicate the code
// instead i will reuse the functions defined in the another notebook and just call those functions



// COMMAND ----------

// MAGIC %run "./ExtVolume-ExtTable-functions"

// COMMAND ----------

val listOfLocations = List(
  List(
    "`PATIENTS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/patients/VOLUMES",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PATIENTS IN DELTALAKE CONTAINER"
  ),
  List(
    "`PROCEDURES_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/procedures/VOLUMES",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PROCEDURES IN DELTALAKE CONTAINER"
  ),
  List(
    "`PAYERS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/payers/VOLUMES",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR PAYERS IN DELTALAKE CONTAINER"
  ),
  List(
    "`ORGANIZATION_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/organization/VOLUMES",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Organization IN DELTALAKE CONTAINER"
  ),
   List(
    "`ENCOUNTERS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/encounters/VOLUMES",
    "`hos-project-credential`",
    "EXTERNAL LOCATION FOR Encounters IN DELTA LAKE CONTAINER"
  )
)

call_create_external_location(listOfLocations,spark)

// COMMAND ----------

val listOfLocations = List(
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PATIENTS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/patients/VOLUMES"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PROCEDURES_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/procedures/VOLUMES"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`PAYERS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/payers/VOLUMES"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`ORGANIZATION_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/organization/VOLUMES"
  ),
  List(
    "`DEV`",
    "`hospital_treatment`",
    "`ENCOUNTERS_DELTALAKE`",
    "abfss://hos-project-deltalake-extract@hospprojectstorage.dfs.core.windows.net/encounters/VOLUMES"
  )
)
call_create_external_volume(listOfLocations, spark)
