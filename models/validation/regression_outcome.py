
import snowflake.snowpark.functions as F
from snowflake.snowpark import Session
import modin.pandas as pd
import snowflake.snowpark.modin.plugin
from snowflake.connector.pandas_tools import write_pandas
import logging

def model(dbt, session):
    
    dbt.config(
        packages = ["snowflake-snowpark-python","yaml","pandas","modin"],
        python_version="3.12",
        materialized = "table"
    )

    df_table_list = session.sql(f"""select table_schema||'.'||table_name table_name from information_schema.tables 
                                where table_schema = 'VALIDATION_REGRESSION' and 
                                table_type = 'BASE TABLE' and 
                                table_name not like 'REGRESSION%'""").to_pandas()
    
    session.sql(""" create or replace  table validation_regression.regression_temptable(len_records int) """).collect()

    if not df_table_list.empty:
        for index, row in df_table_list.iterrows():
            sql_stmt =  'select results from ' + row['TABLE_NAME']
            session.sql(sql_stmt).collect()
            session.sql(f"""Insert into validation_regression.regression_temptable 
                        select len($1) from table(result_scan(last_query_id())) """).collect()

    df_num_of_records = session.sql(f"""select count(distinct len_records) count 
                                    from validation_regression.regression_temptable""").to_pandas()

    num_of_records = df_num_of_records['COUNT'].iloc[0]
    
    if num_of_records == 1:
        df_value = session.sql(f"""select distinct len_records value 
                               from validation_regression.regression_temptable""").to_pandas()
        value = df_value['VALUE'].iloc[0]

        if value == 25:
            result = 'TRUE'
        else:
            result = 'FALSE'
    else:
        result = 'FALSE'
    return_result = session.create_dataframe([(result)], schema = ["result"])

    return return_result