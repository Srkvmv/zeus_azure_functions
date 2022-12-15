import logging as log

import azure.functions as func

# My libraries
import pandas as pd
import numpy as np
import requests
import json
import csv
from hashlib import md5
from typing import Optional, Iterable

#### Check table for primary key incosistencies
def Check_PKey_incosistencies(To_check_df, primary_key_column):
    ############################
    # This is for logging purposes in the future
    ##########################
    if To_check_df.duplicated(subset=primary_key_column, keep=False).agg(func='max'):
        log.warn("There are records with duplicate value of primary key, do you want to continue")
    
    ############################
    # If still continued then we keep the last record with the primary key value and drop all the other duplicate values
    # ########################
    return To_check_df.drop_duplicates(subset=primary_key_column, keep='last')


#### Comparing Meta data
# Assumes that original dataframe has equal number of or more columns than modified dataframe
def check_metadata(original_df, modified_df):
    modified_df_columns = modified_df.columns

    for column in modified_df_columns:
        try:
            original_df[column].dtype != modified_df[column].dtype
        except:
            log.error("Columns in modified are not present in original")
            return False
        else:
            if original_df[column].dtype != modified_df[column].dtype:
                log.error("Data type of {} column is not same in original and modified dataframes".format(column))
                return False
    return True

#### Get Rows
def get_rows(modified_df, primary_key_column, key_list):
    return modified_df[modified_df[primary_key_column].isin(key_list)]


#### Serialization Requirements
# Changing Pandas Timestamp values to Py_date and then into string for serialization
def change_timestamp_to_string(dict_val):
    for i in dict_val:
        if(type(dict_val[i]) == pd._libs.tslibs.timestamps.Timestamp):
            dict_val[i] = str(dict_val[i].to_pydatetime())
    
    return dict_val

#### Function to return IDs of modified rows
def ModChecker(original_df_stringified, modified_df_stringified, primary_key_column, new_col_name = 'ModCheck'):
    insert_key_list = list()
    update_key_list = list()
    for key in modified_df_stringified[primary_key_column]:
        if key not in original_df_stringified[primary_key_column].values:
            insert_key_list.append(key)
        else:
            if(original_df_stringified[original_df_stringified[primary_key_column] == key][new_col_name].values[0] != modified_df_stringified[modified_df_stringified[primary_key_column] == key][new_col_name].values[0]):
                update_key_list.append(key)

    return insert_key_list,update_key_list


#### Creating a new attribute which is a concatenated string of values in a row
# Though all the values in a row can be concatenated, we are only taking values present in columns  
# that are common to both the original and the modified table
def stringify_table(original_table, modified_table, new_col_name='ModCheck', columns_to_stringify=[]):

    if len(columns_to_stringify) == 0:
        # original_stringify_list is a hashed string representaion of a concatenated string of all values in columns common between original and modified dataframes seperated by '||'.
        original_stringify_list = original_table[modified_table.columns].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        # original_stringify_list = original_table[modified_table.columns].astype(str).apply(lambda row: '||'.join(row), axis = 1)  ### For text only with no encoding
        original_df_stringified = original_table.copy()
        original_df_stringified[new_col_name] = original_stringify_list

        # modified_stringify_list is a hashed string representaion of a concatenated string of all values in a row seperated by '||'.
        modified_stringify_list = modified_table.astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        # modified_stringify_list = modified_table.astype(str).apply(lambda row: '||'.join(row), axis = 1)                          ### For text only with no encoding
        modified_df_stringified = modified_table.copy()
        modified_df_stringified[new_col_name] = modified_stringify_list

    else:
        # original_stringify_list is a hashed string representaion of a concatenated string of all values in columns specified by columns_to_stringify of a row seperated by '||'.
        original_stringify_list = original_table[columns_to_stringify].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        # original_stringify_list = original_table[columns_to_stringify].astype(str).apply('||'.join, axis = 1)                     ### For text only with no encoding
        original_df_stringified = original_table.copy()
        original_df_stringified[new_col_name] = original_stringify_list

        # modified_stringify_list is a hashed string representaion of a concatenated string of values in columns specified by columns_to_stringify  of a row seperated by '||'.
        modified_stringify_list = modified_table[columns_to_stringify].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        # modified_stringify_list = modified_table[columns_to_stringify].astype(str).apply('||'.join, axis = 1)                     ### For text only with no encoding
        modified_df_stringified = modified_table.copy()
        modified_df_stringified[new_col_name] = modified_stringify_list

    return original_df_stringified, modified_df_stringified, new_col_name


#### API Call

# Flag - 1 for insert
# Flag - 2 for update
# Flag - 3 for delete list of ids
# Flag - 4 for insert list of rows
# Flag - 5 for update list of rows
# Flag - 6 for delete
# rows should be passed as dictionaries
# for multiple rows pass list of dictionaries, where each dictionary represents a row
def call_API(row_as_dict=[], list_row_as_dict=[], id_to_delete=0, list_id_to_delete=[], insert_update_delete_flag=0, schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
    temp = dict({
        "schema": "",
        "table": "",
        "insert": [],
        "update": [],
        "ids": ""
    })

    temp['schema'] = schema
    temp['table'] = table

    # insert a row
    if(insert_update_delete_flag == 1):
        temp['insert'].append(row_as_dict)
    # update a row
    elif(insert_update_delete_flag == 2):
        temp['update'].append(row_as_dict)
    # delete rows in bulk
    elif(insert_update_delete_flag == 3):
        temp['ids'] = ','.join(list_id_to_delete)
    # insert rows in bulk
    elif(insert_update_delete_flag == 4):
        temp['insert'].extend(list_row_as_dict)
    # update rows in bulk
    elif(insert_update_delete_flag == 5):
        temp['update'].extend(list_row_as_dict)
    # delete a row
    elif(insert_update_delete_flag == 6):
        temp['ids'].append(id_to_delete)


    # API call - specific to envoy API , to change call please change request as needed
    response = requests.post(url = 'https://envoy-web-api.azurewebsites.net/Table/SaveTableData', json=temp)

    return response

#### Merge Function
def merge(original_df, modified_df, primary_key_columns, columns_to_drop=[], full_merge = False, schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
    # Meta data check
    inserted = 0
    updated = 0
    deleted = 0
    insert_error = ""
    update_error = ""
    delete_error = ""

    # Primay Key Column not Defined
    if len(primary_key_columns) < 1:
        insert_error = insert_error + "Primary key is empty"
        return inserted, updated, deleted, insert_error, update_error, delete_error


    # Primary Key columns have Null Vakue
    for column in primary_key_columns:
        if modified_df[column].isna().max() == True:
            insert_error = insert_error + "Primary key has null Values"
            return inserted, updated, deleted, insert_error, update_error, delete_error

    #Drop unwanted columns, like auto generated columns etc.
    if len(columns_to_drop) > 0:
        original_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)
        modified_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

    if check_metadata(original_df, modified_df):

        # Adding a concatenated string of primary key values as primary key hash
        original_df_stringified, modified_df_stringified, primary_key_hash = stringify_table(original_table=original_df, modified_table=modified_df, columns_to_stringify=primary_key_columns, new_col_name='Primary_Hash')

        # Checking modified dataframe for inconsistencies in primary key hash
        checked_modified_df = Check_PKey_incosistencies(modified_df_stringified, primary_key_hash)

        # Adding a concatenated string of values in a encoded manner as final column which is to be considered as hash of the row
        original_df_stringified, modified_df_stringified, Mod_check_col = stringify_table(original_table=original_df_stringified, modified_table=checked_modified_df)

        # Get list of keys of modified and inserted rows
        insert_key_list, update_key_list = ModChecker(original_df_stringified=original_df_stringified, modified_df_stringified=modified_df_stringified, primary_key_column=primary_key_hash, new_col_name=Mod_check_col)

        # Inserted and updated rows with primary hash dropped and all np.NaN values replaced with None
        inserted_rows_df = get_rows(modified_df=checked_modified_df, primary_key_column=primary_key_hash, key_list=insert_key_list).drop(columns=primary_key_hash, errors='ignore').replace({np.nan: ''})
        updated_rows_df = get_rows(modified_df=checked_modified_df, primary_key_column=primary_key_hash, key_list=update_key_list).drop(columns=primary_key_hash, errors='ignore').replace({np.nan: ''})

        # ###############################################
        # # In future, from here the Insert and Update API will be called for all rows in insert_rows_df and updated_rows_df respectively
        # ############################################

        # Calling API for all the inserted rows
        for i in range(len(inserted_rows_df)):
            intermediary = inserted_rows_df.iloc[i].to_dict()
            intermediary = change_timestamp_to_string(dict_val=intermediary)
            for column in intermediary:
                if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
                    intermediary[column] = str(intermediary[column].to_pydatetime())
            
            response = call_API(row_as_dict=intermediary, insert_update_delete_flag=1, table=table, schema=schema)
            status_code = response.status_code
            log.info("API called for insert and returned status of {}".format(status_code))
            # Checking for successful inserts
            response_data = json.loads(response.text)[0]
            if status_code == 200 and response_data['insert_errors'] == "":
                inserted += 1
            elif status_code == 200:
                update_error += " {} : {},".format(i, response_data['insert_errors'])
            else:
                log.error("API call error")

        # Calling API for all the updated rows
        for i in range(len(updated_rows_df)):
            intermediary = updated_rows_df.iloc[i].to_dict()
            intermediary = change_timestamp_to_string(dict_val=intermediary)
            for column in intermediary:
                if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
                    intermediary[column] = str(intermediary[column].to_pydatetime())

            response = call_API(row_as_dict=intermediary, insert_update_delete_flag=2, table=table, schema=schema)
            status_code = response.status_code
            log.info("API called for update and returned status of {}".format(status_code))
            # Checking for successful inserts
            response_data = json.loads(response.text)[0]
            if status_code == 200 and response_data['update_errors'] == "":
                updated += 1
            elif status_code == 200:
                update_error += " {} : {},".format(i, response_data['update_errors'])
            else:
                log.error("API call error")

        # For now only valid for a single primary key value, future work will be done for composite key
        if full_merge == True:
            df_full= original_df.merge(modified_df[primary_key_columns].drop_duplicates(), on=primary_key_columns, how='left', indicator=True)
            # deleted_row_ids = df_full[df_full['_merge'] == 'left_only'][primary_key_columns[0]].to_list()
            deleted_row_ids = df_full[df_full['_merge'] == 'left_only'][primary_key_columns[0]].astype(str).to_list()
            # Calling API to delete all the ids not in modified table
            if(len(deleted_row_ids) > 0):
                response = call_API(list_id_to_delete = deleted_row_ids, insert_update_delete_flag=3, table=table, schema=schema)
                status_code = response.status_code
                response_data = json.loads(response.text)[0]
                log.info("API called for delete and returned status of {}".format(status_code))
                if status_code == 200:
                    deleted += len(deleted_row_ids)
                    delete_error += response_data['delete_errors']

            log.info(deleted_row_ids)
            log.warn(response_data['delete_errors'])

    else:
        ################################
        # Non matching meta datas, Unable to perform merge. Log or error message 
        #############################
        log.error("Cannot Merge")

    return inserted, updated, deleted, insert_error, update_error, delete_error




def main(req: func.HttpRequest) -> func.HttpResponse:
    log.info('Python HTTP trigger function processed a request.')

    # For future
    # datatype_mapping = {
    #     'i' : 'int',
    #     's' : 'str',
    #     'f' : 'float',
    #     'b' : 'bool',
    #     'd' : 'datetime64'
    # }

    inserted = 0
    updated = 0
    deleted = 0
    insert_error = ""
    update_error = ""
    delete_error = ""

    output =   dict({
                    "inserted_rows": 0,
                    "insert_errors": '',
                    "updated_rows": 0,
                    "update_errors": '',
                    "deleted_rows": 0,
                    "delete_errors": ''
                })

    #API tp get table data
    response = requests.get("https://envoy-web-api.azurewebsites.net/Table/GetTablesData?schema=" + req.form['schema'] + "&table=" + req.form['table'])
    Table_Data = dict(response.json())
    Table_data_df = pd.DataFrame(Table_Data['Columns'])

    original_df = pd.DataFrame.from_dict(dict(response.json())['TableData']).replace({np.nan: ''})
    modified_df = pd.read_csv(req.files['modified']).replace({np.nan: ''})
    
    for column in modified_df.columns:
        temp = Table_data_df[Table_data_df['ColumnName'].str.casefold() == column.casefold()]
        try:
            modified_df.rename(columns={column:temp['ColumnName'].item()}, inplace=True)
        except:
            log.error("Column in Modified not found in original dataframe")

            # To Change later
            output['inserted_rows'] = inserted
            output['updated_rows'] = updated
            output['deleted_rows'] = deleted
            output['insert_errors'] = insert_error + "Column in Modified not found in original dataframe"
            output['update_errors'] = update_error
            output['delete_errors'] = delete_error
            return func.HttpResponse(
             json.dumps(output),
             status_code=200
             ) 

        try:
            modified_df[temp['ColumnName'].item()] = modified_df[temp['ColumnName'].item()].astype(original_df[temp['ColumnName'].item()].dtype)
        except:
            log.warn("Unable to apply conversion to {} column and original data type is {}".format(column, original_df[temp['ColumnName'].item()].dtype))

             # To Change later
            output['inserted_rows'] = inserted
            output['updated_rows'] = updated
            output['deleted_rows'] = deleted
            output['insert_errors'] = insert_error + "Unable to apply conversion to {} column and original data type is {}".format(column, original_df[temp['ColumnName'].item()].dtype)
            output['update_errors'] = update_error
            output['delete_errors'] = delete_error
            return func.HttpResponse(
             json.dumps(output),
             status_code=200
             )

    primary_key_column = []
    primary_key_column.append(req.form['primaryKey'])

    try:
        full_merge = bool(req.form['fullMerge'])
        log.warn("Full merge value given")
    except:
        log.info("Not full merge")
        full_merge = False

    inserted, updated, deleted, insert_error, update_error, delete_error = merge(original_df=original_df, modified_df=modified_df, primary_key_columns=primary_key_column, full_merge=full_merge, table=req.form['table'], schema=req.form['schema'])


    output['inserted_rows'] = inserted
    output['updated_rows'] = updated
    output['deleted_rows'] = deleted
    output['insert_errors'] = insert_error
    output['update_errors'] = update_error
    output['delete_errors'] = delete_error

    log.critical(output)

    return func.HttpResponse(
             json.dumps(output),
             status_code=200
        )