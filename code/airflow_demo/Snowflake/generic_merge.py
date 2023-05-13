

# This python script is a generic script to merge from source to destination using the data available in the source stream.
# It makes use INFORMATION_SCHEMA to determine the source and destination tables (NB! Ensure the ordinal position of the table's columns are the same)

import snowflake.snowpark as snowpark

def main(session: snowpark.Session): 
    # Process table names to split out usefull info
    source_table = 'DEMO.DEMO_SCHEMA.STREAM_DIM_CUSTOMER' 
    dest_table = 'DEMO.CONFORMED.DIM_CUSTOMER'
    break_out_source_table_name = source_table.split('.')
    break_out_destination_table_name = dest_table.split('.')
    # Run the information schema query and process results into df
    source_table_colnames_sql = session.sql(f"""
    SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_NAME LIKE '{break_out_destination_table_name[2]}' AND TABLE_SCHEMA LIKE '{break_out_destination_table_name[1]}' AND COLUMN_NAME NOT LIKE '%_PK'
     ORDER BY ORDINAL_POSITION
    """)

    source_table_pks = session.sql(f"""
    SELECT COLUMN_NAME
      FROM INFORMATION_SCHEMA.COLUMNS
     WHERE TABLE_NAME LIKE '{break_out_destination_table_name[2]}' AND TABLE_SCHEMA LIKE '{break_out_destination_table_name[1]}' AND COMMENT = 'JOIN' 
     ORDER BY ORDINAL_POSITION
    """)
    
    # Extract column names from df to list for use in merge statement.
    col_list = []
    [col_list.append(column.COLUMN_NAME) for column in source_table_colnames_sql.to_local_iterator()]
    
    pk_list = []
    [pk_list.append(column.COLUMN_NAME) for column in source_table_pks.to_local_iterator()]
    
    column_names_for_select = ','.join([str(col) for col in col_list])
    join_key = ','.join([f'DESTINATION.{col} = SOURCE.{col}' for col in pk_list])
    column_names_for_when = ','.join([f'DESTINATION.{col} = SOURCE.{col}' for col in col_list])
    column_names_for_values = ','.join([f'SOURCE.{col}' for col in col_list])
    
    
    

    
    sql_statement = f"""
    MERGE INTO {dest_table} AS DESTINATION
    USING 
    (
    SELECT {column_names_for_select}
      FROM {source_table}           
     WHERE NOT (ACTION  = 'DELETE' AND ISUPDATE = TRUE)
    ) AS SOURCE
    ON {join_key}
    
    WHEN MATCHED AND SOURCE.ACTION = 'INSERT' AND SOURCE.ISUPDATE 
    THEN UPDATE SET 
    {column_names_for_when}

    WHEN MATCHED AND SOURCE.ACTION = 'DELETE' 
    THEN DELETE

    WHEN NOT MATCHED AND SOURCE.ACTION = 'INSERT' 
    THEN INSERT
    (
    {column_names_for_select}
    )
    VALUES
    (
    {column_names_for_values}
    )
    """
    try:
       executable = session.sql(sql_statement)    
        
    except Exception as e:
       output_message = e
   
    return sql_statement


    