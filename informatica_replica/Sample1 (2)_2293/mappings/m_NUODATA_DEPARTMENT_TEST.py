# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NUODATA_DEPARTMENT_TEST")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NUODATA_DEPARTMENT_TEST", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TEST_DEPARTMENT_0 [Source Definition] 


query_0 = f"""SELECT
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM
FROM
  TEST_DEPARTMENT"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_TEST_DEPARTMENT_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TEST_DEPARTMENT_1 [Source Qualifier] 


query_1 = f"""SELECT
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_TEST_DEPARTMENT_0
WHERE
  {Initial_Load} (
    date_trunc('DAY', CREATE_DATE_TIME) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', MOD_DATE_TIME) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', CREATED_DTTM) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', LAST_UPDATED_DTTM) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  AND 1 = 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_TEST_DEPARTMENT_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2_2 [Expression] 


query_2_1 = f"""SELECT
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_TEST_DEPARTMENT_1"""

df_2_1 = spark.sql(query_2_1)

df_2_1.createOrReplaceTempView("EXPTRANS_2_1")

query_2_2 = f"""SELECT
  *,
  {LOCATION_ID} AS DC_NBR_EXP,
  current_timestamp() AS LOAD_TSTMP_EXP
FROM
  EXPTRANS_2_1"""

df_2_2 = spark.sql(query_2_2)

df_2_2.createOrReplaceTempView("EXPTRANS_2_2")

# COMMAND ----------
# DBTITLE 1, NUODATA_TEST_DEPARTMENT [Target Definition] 


spark.sql("""INSERT INTO
  NUODATA_TEST_DEPARTMENT
SELECT
  DC_NBR_EXP AS DC_NBR,
  DEPT_ID AS DEPT_ID,
  DEPT_CODE AS DEPT_CODE,
  DESCRIPTION AS DESCRIPTION,
  CREATE_DATE_TIME AS CREATE_DATE_TIME,
  MOD_DATE_TIME AS MOD_DATE_TIME,
  USER_ID AS USER_ID,
  WHSE AS WHSE,
  MISC_TXT_1 AS MISC_TXT_1,
  MISC_TXT_2 AS MISC_TXT_2,
  MISC_NUM_1 AS MISC_NUM_1,
  MISC_NUM_2 AS MISC_NUM_2,
  PERF_GOAL AS PERF_GOAL,
  VERSION_ID AS VERSION_ID,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  LOAD_TSTMP_EXP AS LOAD_TSTMP
FROM
  EXPTRANS_2_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NUODATA_DEPARTMENT_TEST")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NUODATA_DEPARTMENT_TEST", mainWorkflowId, parentName)
