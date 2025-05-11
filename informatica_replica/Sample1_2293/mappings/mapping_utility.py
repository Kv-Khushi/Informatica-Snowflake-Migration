import streamlit as st
import pandas as pd
from snowflake.snowpark.functions import udf
from snowflake.snowpark.context import get_active_session
import snowflake.snowpark
import requests
import json
from time import sleep
from snowflake.snowpark.functions import *
import os
from IPython.display import display, Markdown
from dateutil import parser
import pandas as pd
from snowflake.snowpark.types import *



def showVariables(session, tableName):
    session.sql(f"""SELECT * FROM {tableName}""").show()
	
def fetchAndCreateVariables(session, parentElementName, elementName, variablesTableName, job_id):
    query = f"""SELECT name, value, data_type FROM {variablesTableName} where ((element_name = '{elementName}' and parent_element_name = '{parentElementName}') or (element_name = '{parentElementName}')) and job_id = {job_id} and is_user_defined = true"""
    print(query)
    result = session.sql(query);
    print(result)
    variables = result.collect()
    print(variables)
    for variable in variables:
        name = variable['NAME']
        value = variable['VALUE']
        dataType = variable['DATA_TYPE']
        # Assign the value to the variable
        if(dataType=='integer'):
            if variable['value'] == '':
                value = 0
            else:
                value=int(variable['value'])
        elif (dataType == "date/time"):
            value = parser.parse(value)
        print(name)
        globals()[name] = value
    return globals()

def updateVariable(session, variables, tableName, job_id, parentName, elementName):
    variables = json.loads(variables)
    for leftOperand in variables:
        rightOperand = variables[leftOperand]
        query = f"Update {tableName} set value = (Select value from {tableName} where name = '{rightOperand}' and job_id = {job_id} and element_name = '{parentName}'), is_updated = true where name = '{leftOperand}' and element_name = '{elementName}' and (parent_element_name = '{parentName}' or parent_element_name = '')"
        print(query)
        session.sql(query)
    showVariables(session, tableName)
	
def updateMappingVariable(session, parametersTableName, elementName, mainWorkflowId, parentElementName):
    query = f"""SELECT name from {parametersTableName} where job_id = {mainWorkflowId} and element_name = '{elementName}' and parent_element_name = '{parentElementName}'"""
    variableNames = session.sql(query).collect()
    for row in variableNames:
        name = globals()[row['name']]
        update_query = f"""UPDATE {parametersTableName} set value = '{name}', old_value = '{name}' where job_id = {mainWorkflowId} and element_name = '{elementName}' and parent_element_name = '{parentElementName}'"""
        print(update_query)
        session.sql(update_query)
    showVariables(session, parametersTableName)
	
def truncateTargetTables(session, targetTables):
    targetTables = json.loads(targetTables)
    for targetTable in targetTables:
        if(targetTables[targetTable] == "YES"):
            session.sql(f"""TRUNCATE TABLE {targetTable}""")

def updateOldValueForPersistentVariables(session, parametersTableName, elementName, job_id, parentElementName):
    query = f"""UPDATE {parametersTableName} SET old_value = value where element_name = '{elementName}' and parent_element_name = '{parentElementName}' and is_updated = true and is_persistent = true and job_id = {job_id} """
    session.sql(query)
	
def updateValueForNonPersistentVariables(session, parametersTableName, elementName, job_id, parentElementName):
    query = f"""UPDATE {parametersTableName} SET value = old_value where element_name = '{elementName}' and parent_element_name = '{parentElementName}' and is_updated = true and is_persistent = false and job_id = {job_id}"""
    session.sql(query)

def persistVariables(session, parametersTableName, elementName,job_id, parentElement):
    updateOldValueForPersistentVariables(session, parametersTableName, elementName,job_id, parentElement)
    updateValueForNonPersistentVariables(session, parametersTableName, elementName,job_id, parentElement)
    query = f"""UPDATE {parametersTableName} SET is_updated = false WHERE element_name = '{elementName}' and parent_element_name = '{parentElement}' and job_id = {job_id} """
    session.sql(query)
    showVariables(session, parametersTableName)

# session.sql("CREATE OR ALTER STAGE my_stage COMMENT='my_comment'");


@udf(name="SETVARIABLE", is_permanent=True, stage_location="@my_internal_stage", replace=True)
def set_variable(variableName: str, updatedValue: str) -> str:
    return str(updatedValue)
