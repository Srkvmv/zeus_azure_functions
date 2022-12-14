import logging

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
    # if To_check_df.duplicated(subset=primary_key_column, keep=False).agg(func='max'):
        # print("There are records with duplicate value of primary key, do you want to continue")
    
    ############################
    # If still continued then we keep the last record with the primary key value and drop all the other duplicate values
    # ########################
    return To_check_df.drop_duplicates(subset=primary_key_column, keep='last')


#### Comparing Meta data
# Assumes that original dataframe has equal number of or more columns than modified dataframe
def check_metadata(original_df, modified_df):
    modified_df_columns = modified_df.columns

    for column in modified_df_columns:
        # temp = table_data_df[table_data_df['ColumnName'].str.casefold() == column.casefold()]
        # print(original_df[column].dtype, modified_df[column].dtype)
        if original_df[column].dtype != modified_df[column].dtype:
        # try:
        # original_df[column] = original_df[column].astype(temp['Datatype'])
        # original_df.rename(columns={column:temp['ColumnName']}, inplace=True)
        # except:
        #     return False

        # try:
        # modified_df[column] = modified_df[column].astype(temp['Datatype'])
        # modified_df.rename(columns={column:temp['ColumnName']}, inplace=True)
        # except:
        #     return False
            return False
        # else:
        #     original_df.rename(columns={column:temp['ColumnName']}, inplace=True)
        #     modified_df.rename(columns={column:temp['ColumnName']}, inplace=True)
        
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
        # In future if only concatenated string is required
        # original_stringify_list = original_table[modified_table.columns].astype(str).apply('||'.join, axis = 1)

        # original_stringify_list is a hashed string representaion of a concatenated string of all values in columns common between original and modified dataframes seperated by '||'.
        original_stringify_list = original_table[modified_table.columns].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        original_df_stringified = original_table.copy()
        original_df_stringified[new_col_name] = original_stringify_list

        # In future if only concatenated string is required
        # modified_stringify_list = modified_table.astype(str).apply('||'.join, axis = 1)

        # modified_stringify_list is a hashed string representaion of a concatenated string of all values in a row seperated by '||'.
        modified_stringify_list = modified_table.astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        modified_df_stringified = modified_table.copy()
        modified_df_stringified[new_col_name] = modified_stringify_list

    else:
        # In future if only concatenated string is required
        # original_stringify_list = original_table[modified_table.columns].astype(str).apply('||'.join, axis = 1)

        # original_stringify_list is a hashed string representaion of a concatenated string of all values in columns specified by columns_to_stringify of a row seperated by '||'.
        original_stringify_list = original_table[columns_to_stringify].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
        original_df_stringified = original_table.copy()
        original_df_stringified[new_col_name] = original_stringify_list

        # In future if only concatenated string is required
        # modified_stringify_list = modified_table.astype(str).apply('||'.join, axis = 1)

        # modified_stringify_list is a hashed string representaion of a concatenated string of values in columns specified by columns_to_stringify  of a row seperated by '||'.
        modified_stringify_list = modified_table[columns_to_stringify].astype(str).apply(lambda row: md5('||'.join(row).encode('utf-8')).hexdigest(), axis = 1)
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
def call_API(row_as_dict=[], list_row_as_dict=[], id_to_delete=0, list_id_to_delete="", insert_update_delete_flag=0, schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
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
        # print(temp)
    # update a row
    elif(insert_update_delete_flag == 2):
        temp['update'].append(row_as_dict)
    # delete rows in bulk
    elif(insert_update_delete_flag == 3):
        temp['ids'] + ','.join(list_id_to_delete)
    # insert rows in bulk
    elif(insert_update_delete_flag == 4):
        temp['insert'].extend(list_row_as_dict)
    # update rows in bulk
    elif(insert_update_delete_flag == 5):
        temp['update'].extend(list_row_as_dict)
    # delete a row
    elif(insert_update_delete_flag == 6):
        temp['ids'].append(id_to_delete)

    # print(temp)
    # out_file = open(".\API\\myfile2.json", "w")
  
    # json.dump(temp, out_file, indent = 4)


    # API call - specific to envoy API , to change call please change request as needed
    response = requests.post(url = 'https://envoy-web-api.azurewebsites.net/Table/SaveTableData', json=temp)

    return response.status_code

#### Merge Function
def merge(original_df, modified_df, primary_key_columns, columns_to_drop=[], full_merge = False, schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
    # Meta data check
    inserted = 0
    updated = 0
    deleted = 0

    #Drop unwanted columns, like auto generated columns etc.
    if len(columns_to_drop) > 0:
        original_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)
        modified_df.drop(columns=columns_to_drop, errors='ignore', inplace=True)

    if check_metadata(original_df, modified_df):
        # print(original_df)
    # if True:

        # Adding a concatenated string of values as final column
        original_df_stringified, modified_df_stringified, primary_key_hash = stringify_table(original_table=original_df, modified_table=modified_df, columns_to_stringify=primary_key_columns, new_col_name='Primary_Hash')

        # Checking modified dataframe for inconsistencies in primary key value
        checked_modified_df = Check_PKey_incosistencies(modified_df_stringified, primary_key_hash)

        # Adding a concatenated string of values as final column
        original_df_stringified, modified_df_stringified, Mod_check_col = stringify_table(original_table=original_df_stringified, modified_table=checked_modified_df)

        # Get list of keys of modified and inserted rows
        insert_key_list, update_key_list = ModChecker(original_df_stringified=original_df_stringified, modified_df_stringified=modified_df_stringified, primary_key_column=primary_key_hash, new_col_name=Mod_check_col)

        # print(update_key_list)

        # Inserted and updated rows with primary hash dropped and all np.NaN values replaced with None
        inserted_rows_df = get_rows(modified_df=checked_modified_df, primary_key_column=primary_key_hash, key_list=insert_key_list).drop(columns=primary_key_hash, errors='ignore').replace({np.nan: ''})
        updated_rows_df = get_rows(modified_df=checked_modified_df, primary_key_column=primary_key_hash, key_list=update_key_list).drop(columns=primary_key_hash, errors='ignore').replace({np.nan: ''})

        # ###############################################
        # # In future, from here the Insert and Update API will be called for all rows in insert_rows_df and updated_rows_df respectively
        # ############################################

        # print(len(inserted_rows_df))
        # print(len(updated_rows_df))

        # Calling API to insert for all the inserted rows
        for i in range(len(inserted_rows_df)):
            intermediary = inserted_rows_df.iloc[i].to_dict()
            intermediary = change_timestamp_to_string(dict_val=intermediary)
            for column in intermediary:
                if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
                    intermediary[column] = str(intermediary[column].to_pydatetime())
            
            status_code = call_API(row_as_dict=intermediary, insert_update_delete_flag=1, table=table, schema=schema)
            # print(status_code)
            if status_code == 200:
                inserted += 1
            # print(intermediary)

        # Calling API to insert for all the updated rows
        for i in range(len(updated_rows_df)):
            intermediary = updated_rows_df.iloc[i].to_dict()
            intermediary = change_timestamp_to_string(dict_val=intermediary)
            for column in intermediary:
                if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
                    intermediary[column] = str(intermediary[column].to_pydatetime())

            status_code = call_API(row_as_dict=intermediary, insert_update_delete_flag=2, table=table, schema=schema)
            # print(status_code)
            if status_code == 200:
                updated += 1
            # print(intermediary)

        # For now only valid for a single primary key value, future work will be done for composite key
        if(full_merge):
            df_full= original_df.merge(modified_df[primary_key_columns].drop_duplicates(), on=primary_key_columns, how='left', indicator=True)
            # print(df_full['_merge'])
            deleted_row_ids = df_full[df_full['_merge'] == 'left_only'][primary_key_columns[0]].to_list()
            # print(len(deleted_row_ids))
            
            # Calling API to delete all the ids not in modified table
            if(len(deleted_row_ids) > 0):
                status_code = call_API(list_id_to_delete=deleted_row_ids, insert_update_delete_flag=3, table=table, schema=schema)
                # print(status_code)
            if status_code == 200:
                updated += len(deleted_row_ids)
            # print(deleted_row_ids)

            
        # return inserted_rows_df, updated_rows_df, original_df_stringified, modified_df_stringified
    #     return("{} rows were inserted, {} rows were updated and {} rows were deleted".format(inserted, updated, deleted))

    # else:
    #     ################################
    #     # Non matching meta datas, Unable to perform merge. Log or error message 
    #     #############################
    #     return("Cannot Merge")
    
    # pd.merge(original_df, modified_df, how="left", on=primary_key_column, sort=False)

    return inserted, updated, deleted




def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # original_df = pd.read_excel('C:\\Users\\kbk27\\Desktop\\Zeus\\Merge_NewTry\\API\\Original_data\\Audit_Temp_API.xlsx', sheet_name='Sheet1', header = 1, usecols= 'B:CP')
    # modified1_df = pd.read_excel('C:\\Users\\kbk27\\Desktop\\Zeus\\Merge_NewTry\\API\\Modified_data_InsertRecordWithAllColumns\\Audit_Temp_API.xlsx', sheet_name='Sheet1', header = 1, usecols= 'B:CP')
    # modified2_df = pd.read_excel('C:\\Users\\kbk27\\Desktop\\Zeus\\Merge_NewTry\\API\\Modified_data_Deleted_columns\\Audit_Temp_API.xlsx', sheet_name='Sheet1', header = 1, usecols= 'B:CL')

    # statement = merge(original_df=original_df, modified_df=modified1_df, primary_key_columns=['DataAuditId'], full_merge=True)  

    #API tp get table data
    # https://envoy-web-api.azurewebsites.net/Table/GetTablesData?schema=ZeusDataAudit&table=DATA_AUDIT_TEMP&loggedInUserName=siva.sekaran%40zeussolutionsinc.com&historyView=false
    response = requests.get("https://envoy-web-api.azurewebsites.net/Table/GetTablesData?schema=" + req.form['Schema'] + "&table=" + req.form['Table'])

    Table_Data = dict(response.json())

    Table_data_df = pd.DataFrame(Table_Data['Columns'])



    # print(Table_data_df)

    # print (req.form['Table'])

    # print(req.files['Original'])

    # csvreader = csv.reader(req.files['Original'])

    original_df = pd.read_csv(req.files['Original'])
    modified_df = pd.read_csv(req.files['Modified'])

    for column in original_df.columns:
        temp = Table_data_df[Table_data_df['ColumnName'].str.casefold() == column.casefold()]
        # print(column, temp['ColumnName'])
        original_df.rename(columns={column:temp['ColumnName'].item()}, inplace=True)
    
    for column in modified_df.columns:
        temp = Table_data_df[Table_data_df['ColumnName'].str.casefold() == column.casefold()]
        # print(column, temp['ColumnName'])
        modified_df.rename(columns={column:temp['ColumnName'].item()}, inplace=True)

    # print(original_df.info(), modified_df.info())

    primary_key_column = []
    primary_key_column.append(req.form['Primary_Key'])

    inserted, updated, deleted = merge(original_df=original_df, modified_df=modified_df, primary_key_columns=primary_key_column, full_merge=False, table=req.form['Table'], schema=req.form['Schema'])
    
    # print(statement)

    # name = req.params.get('name')
    # if not name:
    #     try:
    #         req_body = req.get_json()
    #     except ValueError:
    #         pass
    #     else:
    #         name = req_body.get('name')

    # if name:
    #     # return func.HttpResponse(statement)
    #     return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    # else:
        # return func.HttpResponse(
        #      "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
        #      status_code=200
        # )

    output =   dict({
                    "inserted_rows": 0,
                    "insert_errors": '',
                    "updated_rows": 0,
                    "update_errors": '',
                    "deleted_rows": 0,
                    "delete_errors": ''
                })

    output['inserted_rows'] = inserted
    output['updated_rows'] = updated
    output['deleted_rows'] = deleted

    print(output)

    return func.HttpResponse(
             json.dumps(output),
             status_code=200
        )