import snowflake.snowpark.functions as F
import os
import yaml
import json
from typing import Any
from snowflake.snowpark import Session
import modin.pandas as pd
import pandas
import snowflake.snowpark.modin.plugin
from snowflake.connector.pandas_tools import write_pandas
import logging
import glob

# Global DataFrame to hold log information
log_df = pandas.DataFrame(columns=['timestamp', 'function', 'message'])


def log_message(session,function_name, message):
    """
    Appends a new log entry to the global DataFrame.
    """
    global log_df  # Declare that we're using the global variable
    new_entry = pandas.DataFrame([{ 'timestamp': pandas.Timestamp.now(), 'function': function_name,  'message': message}])
    sql_cmd = f"""Insert into validation_regression.regression_execution_log values (current_timestamp,'{function_name}','{str(message).replace("'","''")}')"""
    session.sql(sql_cmd).collect()
    log_df = pandas.concat([log_df, new_entry], ignore_index=True)



def parse_release_notes(session) -> str:
    """
    Parse the latest release notes and read the list of impacted models and columns in that release.
    These models and columns need to participate in the regression process
    """ 
    log_message(session,'parse_release_notes',f"""Function Initiated""")
    # Read the latest file and parse
    try:
        sql_cmd = f"""Select * from (
                        SELECT METADATA$FILENAME 
                        FROM @validation_regression.configs (PATTERN => 'release_v*.*.json') t
                        order by METADATA$FILE_LAST_MODIFIED desc
                        ) limit 1;"""
        release_file_df = session.sql(sql_cmd).to_pandas()
        release_file = '@validation_regression.configs/' + release_file_df.iloc[0,0]
        log_message(session,'parse_release_notes',f"""Selected release file: {release_file}""")
        sql_cmd = f"""Select  $1:releases[0]:models_impacted  release_items from {release_file} """
        log_message(session,'parse_release_notes',f"""SQL cmd prepared: {sql_cmd}""")
        list_release_items_df = session.sql(sql_cmd).to_pandas()
        list_release_items = list_release_items_df.iloc[0,0]
        log_message(session,'parse_release_notes',f"""Release Items From Release file: {release_file} are : {list_release_items}.""")
        return str(list_release_items)
    except:
        log_message(session,'parse_release_notes',f""" Error parsing Release file: {release_file}.""")
        return 'None'



def create_pandas_cmd(session,config:dict,exclude_column_list:list) :
    """
    Prepare the pandas command that reads columns which are participating in the regression from a dbt model.
    Columns which have gone through a change in the MR, do not participate in the regression
    """
    log_message(session,'create_pandas_cmd',f"""Function Initiated""")
    try:
        # Step 1 : parse config
        has_all_filter = 0
        has_sort = 0
        base_keys = {"name" : 1, "database" : 2, "schema" : 3}
        filter_keys = {"filter_column" : 1, "filter_operator" : 2 , "filter_column_value" : 3}

        if "name" and "database" and "schema" in config:
            if config["name"] and config["database"] and config["schema"]:
                database = config['database']
                model = config['name']
                schema = config['schema']
                model_col_list = str(get_model_columns(session,database, model, schema,exclude_column_list))
        if all(key in config.keys() for key in filter_keys):
            if config["filter_column"] and config["filter_operator"] and config["filter_column_value"]:
                has_all_filter = 1
                filter_Column = config['filter_column'].upper()
                filter_Operator = config['filter_operator']
                filter_Column_Value = config['filter_column_value']
        
        # Step 2 : create pandas command
        if has_all_filter == 1 and has_sort == 1 :
            pandas_cmd = "df[df['" + filter_Column + "']" + filter_Operator + " '" + filter_Column_Value + "']" + model_col_list + ".sort_values(by="+ model_col_list +")"
        if has_all_filter == 1 and has_sort == 0 :
            pandas_cmd = "df[df['" + filter_Column + "'] " + filter_Operator + " '" + filter_Column_Value + "']" + model_col_list +".sort_values(by="+ model_col_list+")"
        if has_all_filter == 0 and has_sort == 1 :
            pandas_cmd = "df" + model_col_list + ".sort_values(by="+model_col_list+")"
        if has_all_filter == 0 and has_sort == 0 :
            pandas_cmd = "df" + model_col_list + ".sort_values(by="+model_col_list+")"    
        log_message(session,'create_pandas_cmd',f""" pandas_cmd : {pandas_cmd}.""")

    except:
        log_message(session,'create_pandas_cmd',f""" Error creating pandas cmd.""")
        return (database,model,schema,'None')
    
    return (database,model,schema,pandas_cmd)
    



def get_model_columns(session,database:str, model:str, schema:str, exclude_col_list) -> list:
    """
    Read column names from a dbt model minus the list of columns that were changed
    """
    log_message(session,'get_model_columns',f"""Function Initiated""")
    model_name_ref = database + '.' + schema + '.' + model
    df = pd.read_snowflake(model_name_ref)
    model_col_list = list(df.columns)
    model_col_list = sorted (list( set(list(df.columns)) - set(exclude_col_list)))
    log_message(session,'get_model_columns',f""" Ref Model : {str(model_name_ref)}. Columns in attention : {model_col_list}.""")
    return model_col_list



def regression_process(session,database:str, model:str, schema:str, pandas_cmd:str) -> str:
    """
    The regression process that compares two dataframes
    """
    log_message(session,'get_model_columns',f"""Function Initiated""")
    try:
        model_name_ref = database + '.' + schema + '.' + model
        model_name_regression = database + '.' + schema  + '_regression.' + model
        log_message(session,'regression_process',f""" Ref Model : {str(model_name_ref)}. Regression Model : {str(model_name_regression)}.""")
        log_message(session,'regression_process',f""" pandas_cmd : {pandas_cmd}.""")
        
        # Calculating the size of the ref model dataframe and the regression model dataframe
            
        df_ref = pd.read_snowflake(model_name_ref)
        df_ref_sorted = eval(pandas_cmd.replace('df','df_ref'))
        df_ref_sorted.reset_index(drop=True,inplace=True)
        
        df_regression = pd.read_snowflake(model_name_regression)
        df_regression_sorted = eval(pandas_cmd.replace('df','df_regression'))
        df_regression_sorted.reset_index(drop=True,inplace=True)
        
        log_message(session,'regression_process',f""" Ref Model Size : {str(df_ref_sorted.size)}. Regression Model Size: {str(df_regression_sorted.size)}.""")

        #Dataframe compare works only if the sizes of the dataframe sizes are equal
        if (df_ref_sorted.size == df_regression_sorted.size):
            log_message(session,'regression_process',f"""The data frames are equal in size.""")
            df_results = df_ref_sorted.compare(df_regression_sorted).to_pandas() 
            df_results.reset_index(drop=True,inplace=True)    
            if not df_results.empty:
                regression_resultset = df_results.head(10).to_string().replace("'","''")
            else:
                regression_resultset = "The data frames are equal"
            log_message(session,'regression_process',f"""Lenth of the comparison resultset : {len(regression_resultset)}.""")
        else:
            df_ref_sorted_num_rows = len(df_ref_sorted)
            df_ref_sorted_num_cols = df_ref_sorted.shape[1]
            df_regression_sorted_num_rows = len(df_regression_sorted)
            df_regression_sorted_num_cols = df_regression_sorted.shape[1]
            regression_resultset = f"""The data frames are not equal in size. 
                                    Size of the Ref dataset : {df_ref_sorted_num_rows}X{df_ref_sorted_num_cols} 
                                    Size of the Regression dataset: {df_regression_sorted_num_rows} X {df_regression_sorted_num_cols} """
            log_message(session,'regression_process',f"""Lenth of the comparison resultset : {len(regression_resultset)}.""")
    except:
        log_message(session,'regression_process',f""" Error processing regression""")
        return ('None')
    
    return regression_resultset



def save_regression_result (session,database:str, model:str, result_schema:str, resultset:str):
    """
    Save regression resultset in a table as string element
    """
    try:
        log_message(session,'save_regression_result',f"""Function Initiated""")
        log_message(session,'save_regression_result',f"""Final result for model {model} is {resultset}""")
        sql_cmd = "Create or replace table " + database + "." + result_schema + "." + model + " (results text) "
        session.sql(sql_cmd).collect()
        sql_cmd = "Insert into " + database + "." + result_schema + "." + model + " (results) values ('" + resultset + "')"
        log_message(session,'save_regression_result',f"""Insert statement : {sql_cmd} """)
        session.sql(sql_cmd).collect()
        log_message(session,'save_regression_result',f"""Regression results logged for model: {model} in schema: {result_schema} """)
    except:
        log_message(session,'save_regression_result',f""" Error saving regression result""")
        return ('False')
    return True



def check_schema_and_config_file_existence(session,schema:str, file_path:str, stage_name:str)->bool:
    """
    Checks if a database schema and a file in a Snowflake stage exist.
    """
    log_message(session,'check_schema_and_config_file_existence',f"""Function Initiated""")

    try:     
        log_message(session,'check_schema_and_config_file_existence',f"""Function Initiated""")
        # 1. Check if the schema exists
        sql_cmd = f"""select count(*) count_rec from information_schema.schemata where schema_name in (upper('{schema}'))"""
        df_count = session.sql(sql_cmd).to_pandas()
        schema_count = df_count.iloc[0,0]
        schema_exists = True if schema_count == 2 else False
        log_message(session,'check_schema_and_config_file_existence',f"""Schema {schema} exists = {schema_exists}""")

        # 2. Check if the file exists in the stage
        sql_cmd = f"""LIST @{schema}.{stage_name}/{file_path}"""
        df_files = session.sql(sql_cmd).to_pandas()
        file_exists = len(df_files) > 0
        log_message(session,'check_schema_and_config_file_existence',f"""File {file_path} exists in stage {stage_name} = {file_exists}.""")

        if schema_exists == False and file_exists == False:
            log_message(session,'check_schema_and_config_file_existence',f"""Schemas '{schema}' should exist before model execution.""")
            return False
        else:    
            return True
        
    except Exception as e:
        log_message(session,'check_schema_and_config_file_existence',f""" Error executing check_schema_and_config_file_existence""")
        return False



def model(dbt, session):
    dbt.config(
        packages = ["snowflake-snowpark-python","yaml","pandas","modin"],
        python_version="3.12",
        materialized = "table"
    )

    #Creating a log table where every log message is written instantaneouly
    sql_cmd = "CREATE OR REPLACE TABLE validation_regression.regression_execution_log ( time timestamp, function_name varchar, log_message text ) "
    session.sql(sql_cmd).collect()
    log_message(session,'main',f"""Function Initiated""")

    # Check for schema and config file in a stage
    prechecks = check_schema_and_config_file_existence(session,'validation_regression','regression_config.json','configs')

    if prechecks == False : 
        log_message(session,'main',f"""schema and config file check. Function outcome: {prechecks}""")
    else : 
        log_message(session,'main',f"""schema and config file check. Function outcome: {prechecks}""")

        # Parse release notes
        release_items_str = parse_release_notes(session) #{'sku_current': ['COMPONENT_PERCENTAGE']} # dbt.config.get("release_items_dict")
        log_message(session,'main',f"""Release items from the release file : {release_items_str} """) #with type {type(release_items_str)}

        release_items_dict = json.loads(release_items_str)
        release_items_dict = { key.upper() : list(map(str.upper, release_items_dict[key])) for key in release_items_dict }
        release_models_list = {key for key in release_items_dict}
        log_message(session,'main',f"""Release models from the release notes : {release_models_list} """)

        # Parse regression config
        config_file = '@validation_regression.configs/regression_config.json'
        result_schema = 'validation_regression'
        sql_cmd = "CREATE OR REPLACE FILE FORMAT validation_regression.config_format TYPE = \'json\'"
        session.sql(sql_cmd).collect()
        sql_cmd = "Select $1 from " + config_file + "(file_format => validation_regression.config_format)"
        config_items_row = session.sql(sql_cmd).collect()

        for index, config in enumerate(config_items_row[0]):
            config_items_str = config
        log_message(session,'main',f"""Regression Config : '{config_items_str}' """)

        config_items_list = json.loads(config_items_str)
        config_models_list = []
        
        for index, config_block in enumerate(config_items_list):
            config_models_list.append(config_block['name'].upper())
        log_message(session,'main',f"""Config models list: {config_models_list} """)


        if set(release_models_list).issubset(config_models_list):
            
            log_message(session,'main',f"""Processing Each Model in the release notes """)

            for index, config_block in enumerate(config_items_list):

                log_message(session,'main',f"""Iteration: {index}. Procesing {config_block['name'].upper()} """)

                if config_block['name'].upper() in release_models_list:
                    exclude_column_list = release_items_dict.get(config_block['name'].upper())
                    database, model, schema, pandas_cmd = create_pandas_cmd(session,config_block,exclude_column_list)
                    log_message(session,'main', f""" Full model name under process: {(database + '.' + schema + '.' + model)}""" )
                    regression_resultset = regression_process(session,database, model, schema, pandas_cmd)
                    if len(regression_resultset) != 0:
                        save_regression_result(session,database, model, result_schema, regression_resultset)
                else:
                    log_message(session,'main',f"""Skipping model : {config_block['name'].upper()} """)
        else:
            log_message(session,'main', "List of models in the release note is not a subset of regression config")

        
        return_result = session.sql(f"""select table_name from information_schema.tables 
                                    where table_schema = 'VALIDATION_REGRESSION' and 
                                    table_type = 'BASE TABLE' 
                                    AND table_name not like 'REGRESSION%' """).to_pandas()
        log_message(session,'main', f"""List of  models processed for regression : {return_result.iloc[0,0]}""")

    
    # Final result
    return log_df