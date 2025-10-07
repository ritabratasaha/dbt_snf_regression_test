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
from typing import Tuple


# Global DataFrame to hold log information
final_result_df = pandas.DataFrame(columns=['timestamp', 'model','status', 'message'])
log_df = pandas.DataFrame(columns=['timestamp', 'function', 'message'])


def log_message(session,function_name, message):
    """
    Appends a new log entry to the global log DataFrame.
    """
    global log_df  # Declare that we're using the global variable
    new_entry = pandas.DataFrame([{ 'timestamp': pandas.Timestamp.now(), 'function': function_name,  'message': message}])
    sql_cmd = f"""Insert into validation_regression.data_type_validation_log values (current_timestamp,'{function_name}','{str(message).replace("'","''")}')"""
    session.sql(sql_cmd).collect()
    log_df = pandas.concat([log_df, new_entry], ignore_index=True)


def final_result(model,status, message):
    """
    Appends a new log entry to the final global result dataframe.
    """
    global final_result_df  # Declare that we're using the global variable
    new_entry = pandas.DataFrame([{ 'timestamp': pandas.Timestamp.now(), 'model': model, 'status':status , 'message': message}])
    final_result_df = pandas.concat([final_result_df, new_entry], ignore_index=True)



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



def data_type_validation_process(session,config:dict) -> Tuple[str, pd.DataFrame]:
    """
    The regression process that compares two dataframes
    """
    log_message(session,'data_type_validation_process',f"""Function Initiated""")
    try:
        if config["name"] and config["database"] and config["schema"]:
                database = config['database'].upper()
                model = config['name'].upper()
                schema = config['schema'].upper()
        
        sql_cmd = f"""with df_ref as (
                        Select 
                        table_name,column_name,ordinal_position,data_type,
                        character_maximum_length,numeric_precision,numeric_precision_radix
                        from INFORMATION_SCHEMA.COLUMNS 
                        WHERE table_name = '{model}' and table_schema = '{schema}' order by ordinal_position
                        ),
                        df_current as (
                        Select 
                        table_name,column_name,ordinal_position,data_type,
                        character_maximum_length,numeric_precision,numeric_precision_radix
                        from INFORMATION_SCHEMA.COLUMNS 
                        WHERE table_name = '{model}' and table_schema = '{schema}_REGRESSION' order by ordinal_position
                        ),
                        df_join as (
                        Select 
                        df_ref.column_name,
                        df_ref.data_type  as data_type_ref,
                        df_current.data_type,
                        df_ref.character_maximum_length as character_maximum_length_ref,
                        df_current.character_maximum_length,
                        df_ref.numeric_precision as numeric_precision_ref,
                        df_current.numeric_precision,
                        df_ref.numeric_precision_radix as numeric_precision_radix_ref,
                        df_current.numeric_precision_radix,
                        from 
                        df_ref left join df_current 
                        on df_ref.column_name=df_current.column_name
                        )
                        Select *,
                        case when 
                            (nvl(data_type_ref,'') = nvl(data_type,''))  and 
                            (nvl(character_maximum_length_ref,'0') = nvl(character_maximum_length,'0'))  and 
                            (nvl(numeric_precision_ref,'0') = nvl(numeric_precision,'0'))  and
                            (nvl(numeric_precision_radix_ref,'0') = nvl(numeric_precision_radix,'0'))    
                        then 'Pass' else 'Fail' end as data_type_match
                        from df_join;
                        """
        df_results = session.sql(sql_cmd).to_pandas()
        data_type_match_list = (df_results['DATA_TYPE_MATCH'].unique()).tolist()         
        if 'Fail' in data_type_match_list :
            status = 'Fail'
            df_results_filter = df_results[df_results['DATA_TYPE_MATCH']=='Fail']       
            df_results_col  = df_results_filter[['COLUMN_NAME', 'DATA_TYPE_MATCH']]
        else:
            status = 'Pass'
            df_results_col  = df_results[['COLUMN_NAME', 'DATA_TYPE_MATCH']]
        log_message(session,'data_type_validation_process',f"""Data type check for model : {model} is : {status}""")
    
    except:
        log_message(session,'data_type_validation_process',f""" Error processing data type validation """)
        return status,df_results_col
    
    return status,df_results_col



def model(dbt, session):
    dbt.config(
        packages = ["snowflake-snowpark-python","yaml","pandas","modin"],
        python_version="3.12",
        materialized = "table"
    )

    #Creating a log table where every log message is written instantaneouly
    sql_cmd = "CREATE OR REPLACE TABLE validation_regression.data_type_validation_log ( time timestamp, function_name varchar, log_message text ) "
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
            
            # Iterate over each config model name
            for index, config_block in enumerate(config_items_list):
                log_message(session,'main',f"""Iteration: {index}. Procesing {config_block['name'].upper()} """)

                if config_block['name'].upper() in release_models_list:
                    model = config_block['name'].upper()
                    log_message(session,'main', f""" Model name under process: {model}""" )
                    status, result_df = data_type_validation_process(session,config_block)
                    final_result(model,status,result_df.to_string())
                    
                else:
                    log_message(session,'main',f"""Skipping model : {config_block['name'].upper()} """)
        else:
            log_message(session,'main', "List of models in the release note is not a subset of regression config")
    
    # Final result
    return final_result_df