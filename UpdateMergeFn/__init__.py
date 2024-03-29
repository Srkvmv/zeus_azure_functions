import logging as log

import azure.functions as func

# My libraries
import pandas as pd
import numpy as np
import requests
import json
import datetime
from azure.storage.blob import BlobServiceClient
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
# def ModChecker(original_df_stringified, modified_df_stringified, primary_key_column, new_col_name = 'ModCheck'):
#     insert_key_list = list()
#     update_key_list = list()
#     for key in modified_df_stringified[primary_key_column]:
#         if key not in original_df_stringified[primary_key_column].values:
#             insert_key_list.append(key)
#         else:
#             if(original_df_stringified[original_df_stringified[primary_key_column] == key][new_col_name].values[0] != modified_df_stringified[modified_df_stringified[primary_key_column] == key][new_col_name].values[0]):
#                 update_key_list.append(key)

#     return insert_key_list,update_key_list
def ModChecker(original_df_stringified, modified_df_stringified, primary_key_column, new_col_name = 'ModCheck'):
    temp_df = pd.merge(original_df_stringified[[primary_key_column, new_col_name]], modified_df_stringified[[primary_key_column]], how='outer', on=primary_key_column, indicator=True)
    
    insert_key_list = temp_df[temp_df['_merge'] == 'right_only'][primary_key_column].to_list()
    delete_key_list = temp_df[temp_df['_merge'] == 'left_only'][primary_key_column].to_list()

    temp_df2 = pd.merge(temp_df[temp_df['_merge'] == 'both'].drop(columns=['_merge']), modified_df_stringified[[new_col_name]], how='outer', on=new_col_name, indicator=True)
    update_key_list = temp_df2[temp_df2['_merge'] == 'left_only'][primary_key_column].to_list()

    return insert_key_list, update_key_list, delete_key_list


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
def call_API(row_as_dict=[], list_row_as_dict=[], id_to_delete=0, list_id_to_delete=[], insert_update_delete_flag=0, base_url= "https://envoy-web-api.azurewebsites.net", schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
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
    response = requests.post(url = base_url + '/Table/SaveTableData', json=temp)

    return response


#### To write logs into ADLS
def write_dataframe_to_datalake(df, user="admin", operator="all"):
    STORAGEACCOUNTURL = "https://adlszeus.blob.core.windows.net"
    STORAGEACCOUNTKEY = "ksL9a2OZFCiKFYPn6hzTNJcY4WI2Nq2xSsRlUD8cDH3dBBEvePAhJqErSP6QKN27so/2ayW3DnO7O8s4uPtUZA=="
    CONTAINERNAME = "envoy-upload-logs"
    # BLOBNAME = "AzureFunctionLogs/temp1.csv"
    BLOBNAME = operator + "/" + "uploadMergeLog" + "_" + user + "_" + str(datetime.datetime.utcnow()) + ".csv"

    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)

    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)

    blob_client_instance.upload_blob(data=df.to_csv(index=False))

    return True


#### Merge Function
def merge(original_df, modified_df, primary_key_columns, columns_to_drop=[], full_merge = False, operator="all", user="admin", base_url= "https://envoy-web-api.azurewebsites.net", schema="ZeusDataAudit", table="DATA_AUDIT_TEMP"):
    # Meta data check
    inserted = 0
    updated = 0
    deleted = 0
    insert_error = 0
    update_error = 0
    delete_error = 0

    Status_Code = []
    Action = []
    Message = []

    inserted_errors = []
    updated_errors = []

    # Primay Key Column not Defined
    if len(primary_key_columns) < 1:
        # insert_error = insert_error + "Primary key is empty"
        log.error("Primary key is empty")
        return inserted, updated, deleted, insert_error, update_error, delete_error


    # Primary Key columns have Null Vakue
    for column in primary_key_columns:
        if modified_df[column].isna().max() == True:
            # insert_error = insert_error + "Primary key has null Values"
            log.error("Primary key has null Values")
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
        insert_key_list, update_key_list, delete_key_list = ModChecker(original_df_stringified=original_df_stringified, modified_df_stringified=modified_df_stringified, primary_key_column=primary_key_hash, new_col_name=Mod_check_col)

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
            # for column in intermediary:
            #     if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
            #         intermediary[column] = str(intermediary[column].to_pydatetime())
            
            response = call_API(row_as_dict=intermediary, insert_update_delete_flag=1, base_url=base_url, table=table, schema=schema)
            status_code = response.status_code
            # log.info("API called for insert and returned status of {}".format(status_code))
            # Checking for successful inserts
            if status_code != 200:
                # log.error("API call error - {}".format(status_code))
                # log.error("Error in inserting row")
                # log.error(inserted_rows_df.iloc[i][primary_key_columns].to_dict())
                insert_error += 1

                #For Log Table
                Status_Code.append(status_code)
                Action.append('insert_error')
                Message.append("Error in inserting row")
            else:
                try:
                    response_data = json.loads(response.text)[0]
                except:
                    # log.error("API call error - {}".format(status_code))
                    # log.error("Error in inserting row")
                    # log.error(inserted_rows_df.iloc[i][primary_key_columns].to_dict())
                    insert_error +=1

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('insert_error')
                    Message.append("Error in inserting row")

                    continue
                if response_data['insert_errors'] != None:
                    # insert_error += " {} : {},".format(inserted_rows_df.iloc[i][primary_key_columns], response_data['insert_errors'])
                    # log.error("API call error - {}".format(status_code))
                    # log.error(response_data['insert_errors'])
                    # log.error(inserted_rows_df.iloc[i][primary_key_columns].to_dict())
                    insert_error += 1

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('insert_error')
                    Message.append(response_data['insert_errors'])

                else:
                    inserted += 1

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('inserted')
                    Message.append("successful insertion")

        # Calling API for all the updated rows
        for i in range(len(updated_rows_df)):
            intermediary = updated_rows_df.iloc[i].to_dict()
            intermediary = change_timestamp_to_string(dict_val=intermediary)
            # for column in intermediary:
            #     if(type(intermediary[column]) == pd._libs.tslibs.timestamps.Timestamp):
            #         intermediary[column] = str(intermediary[column].to_pydatetime())

            response = call_API(row_as_dict=intermediary, insert_update_delete_flag=2, base_url=base_url, table=table, schema=schema)
            status_code = response.status_code
            # log.info("API called for update and returned status of {}".format(status_code))
            # Checking for successful updates
            if status_code != 200:
                update_error += 1
                # log.error("API call error - {}".format(status_code))
                # log.error("Error in updating row")
                # log.error(updated_rows_df.iloc[i][primary_key_columns].to_dict())

                #For Log Table
                Status_Code.append(status_code)
                Action.append('update_error')
                Message.append("Error in updating row")

            else:
                try:
                    response_data = json.loads(response.text)[0]
                except:
                    update_error += 1
                    # log.error("API call error - {}".format(status_code))
                    # log.error("Error in updating row")
                    # log.error(updated_rows_df.iloc[i][primary_key_columns].to_dict())

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('update_error')
                    Message.append("Error in updating row")

                    continue

                if response_data['update_errors'] != None:
                    # update_error += " {} : {},".format(updated_rows_df.iloc[i][primary_key_columns], response_data['update_errors'])
                    # log.error("API call error - {}".format(status_code))
                    # log.error(response_data['update_errors'])
                    # log.error(updated_rows_df.iloc[i][primary_key_columns].to_dict())
                    update_error += 1

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('update_error')
                    Message.append(response_data['update_errors'])

                else:
                    updated += 1

                    #For Log Table
                    Status_Code.append(status_code)
                    Action.append('updated')
                    Message.append("successful updation")

        # For now only valid for a single primary key value, future work will be done for composite key
        if full_merge == True:
            # df_full= original_df.merge(modified_df[primary_key_columns].drop_duplicates(), on=primary_key_columns, how='left', indicator=True)
            # deleted_row_ids = df_full[df_full['_merge'] == 'left_only'][primary_key_columns[0]].to_list()
            deleted_rows_df = get_rows(modified_df=original_df_stringified[modified_df_stringified.columns], primary_key_column=primary_key_hash, key_list=delete_key_list).drop(columns=[primary_key_hash, Mod_check_col], errors='ignore').replace({np.nan: ''})

            # deleted_row_ids = df_full[df_full['_merge'] == 'left_only'][primary_key_columns[0]].astype(str).to_list()
            deleted_row_ids = deleted_rows_df[primary_key_columns[0]].astype(str).to_list()

            # Calling API to delete all the ids not in modified table
            if(len(deleted_row_ids) > 0):
                response = call_API(list_id_to_delete = deleted_row_ids, insert_update_delete_flag=3, base_url=base_url, table=table, schema=schema)
                status_code = response.status_code
                log.info("API called for delete and returned status of {}".format(status_code))
                if status_code == 200:
                    try:
                        response_data = json.loads(response.text)[0]
                        log.info(deleted_row_ids)
                        log.warn(response_data['delete_errors'])

                        #For Log Table
                        Status_Code.extend([status_code] * len(deleted_row_ids))
                        Action.extend(['deleted'] * len(deleted_row_ids))
                        Message.extend([response_data['delete_errors']] * len(deleted_row_ids))
                        
                    except:
                        log.warn("Problem with delete")
                        log.warn(deleted_row_ids)

                        #For Log Table
                        Status_Code.extend([status_code] * len(deleted_row_ids))
                        Action.extend(['deleted'] * len(deleted_row_ids))
                        Message.extend(["Problem with delete, Not all relevant Ids were deleted"] * len(deleted_row_ids))

                    deleted += len(deleted_row_ids)
                    # delete_error += response_data['delete_errors']
                    
                else:
                    log.error(deleted_row_ids)
                    try:
                        response_data = json.loads(response.text)[0]
                        log.error(response_data['delete_errors'])

                        #For Log Table
                        Status_Code.extend([status_code] * len(deleted_row_ids))
                        Action.extend(['delete_error'] * len(deleted_row_ids))
                        Message.extend([response_data['delete_errors']] * len(deleted_row_ids))

                    except:
                        log.error("Problem with delete")

                        #For Log Table
                        Status_Code.extend([status_code] * len(deleted_row_ids))
                        Action.extend(['delete_error'] * len(deleted_row_ids))
                        Message.extend(["Problem with delete, Not all relevant Ids were deleted"] * len(deleted_row_ids))

                    delete_error += len(deleted_row_ids)


    else:
        ################################
        # Non matching meta datas, Unable to perform merge. Log or error message 
        #############################
        log.error("Cannot Merge")

    try:
        log.info("Inserted : {}    Updated : {}    Deleted : {}".format(len(inserted_rows_df),len(updated_rows_df), len(deleted_rows_df)))
        log_df = pd.concat([inserted_rows_df, updated_rows_df, deleted_rows_df], ignore_index=True)
    except:
        log.info("Delete was missing as full merge was not called")
        log.info("Inserted : {}    Updated : {}".format(len(inserted_rows_df),len(updated_rows_df)))
        log_df = pd.concat([inserted_rows_df, updated_rows_df], ignore_index=True)

    log.info("log df shape : {}   status code len : {}   Action len : {}    Message len : {}".format(log_df.shape[0], len(Status_Code), len(Action), len(Message)))
    log_df['StatusCode'] = Status_Code
    log_df['Action'] = Action
    log_df['Message'] = Message
    # For local testing purpose
    # log_df.to_csv(path_or_buf=".\Log\\temp.csv", index=False)
    write_dataframe_to_datalake(df=log_df, user=user, operator=operator)

    return inserted, updated, deleted, insert_error, update_error, delete_error

#################################
#   MAIN
#############################

def main(req: func.HttpRequest) -> func.HttpResponse:
    log.info('Python HTTP trigger function processed a request.')
    flag = 0
    base_url = ""

    # Checking for availability of user, operator and environment variables
    try:
        log.info("{} : {}".format(req.form['user'], req.form['operator']))        
    except:
        log.warn("Operator or User not Received")
        flag = 1

    try:
        log.info("{}".format(req.form['environment']))
        if (req.form['environment'] == 'OASIS'):
            base_url = 'https://oasis-dx-api.azurewebsites.net';

        elif (req.form['environment'] == 'ENVOY'):
            base_url = 'https://envoy-web-api.azurewebsites.net';
        
        else:
            log.error("Unknown Environment")
            flag = 1
    except:
        log.warn("Environment not Received")
        flag = 1

    log.info("{}".format(base_url))

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

    # Break the functions in case of missing operator, user or environment variable
    if flag != 0:
        log.error("Either environment, operator or user details are not available")
        # To Change later
        output['inserted_rows'] = inserted
        output['updated_rows'] = updated
        output['deleted_rows'] = deleted
        output['insert_errors'] = insert_error + "Either environment, operator or user details are not available"
        output['update_errors'] = update_error
        output['delete_errors'] = delete_error
        return func.HttpResponse(
            json.dumps(output),
            status_code=200
            )

    #API tp get table data
    # response = requests.get("https://envoy-web-api.azurewebsites.net/Table/GetTablesData?schema=" + req.form['schema'] + "&table=" + req.form['table'])
    response = requests.get(base_url + "/Table/GetTablesData?schema=" + req.form['schema'] + "&table=" + req.form['table'])
    Table_Data = dict(response.json())
    Table_data_df = pd.DataFrame(Table_Data['Columns'])

    original_df = pd.DataFrame.from_dict(dict(response.json())['TableData']).replace({np.nan: ''})
    try:
        modified_df = pd.read_csv(req.files['modified']).replace({np.nan: ''})
    except:
        log.error("Uploaded file is not in a proper format of a csv, possibly more/less values than header or a completely different file format")

    
    
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

    ##########################
    # Operator Check
    ########################
    try:
        operator = str(req.form['operator'])
        if operator == "all":
            log.info("User has access to all data")
        elif operator.strip() == "":
            log.error("Operator details not provided")
            # To Change later
            output['inserted_rows'] = inserted
            output['updated_rows'] = updated
            output['deleted_rows'] = deleted
            output['insert_errors'] = insert_error + "Unable to apply upload merge functionality due to missing operator information"
            output['update_errors'] = update_error
            output['delete_errors'] = delete_error
            return func.HttpResponse(
             json.dumps(output),
             status_code=200
             )
        else:
            myOperatorlist = pd.Series(operator.split(','))
            log.info("{}".format(myOperatorlist))
            original_df = original_df[original_df['Operator'].str.casefold().isin(myOperatorlist.str.casefold())]
            numOfRecords = len(modified_df)
            modified_df = modified_df[modified_df['Operator'].str.casefold().isin(myOperatorlist.str.casefold())]
            if numOfRecords > len(modified_df):
                log.info("There were records containing illegal operator names that you don't have permission to modify")
    except:
        log.error("Operator not specified")
        # To Change later
        output['inserted_rows'] = inserted
        output['updated_rows'] = updated
        output['deleted_rows'] = deleted
        output['insert_errors'] = insert_error + "Unable to apply upload merge functionality due to missing operator information"
        output['update_errors'] = update_error
        output['delete_errors'] = delete_error
        return func.HttpResponse(
            json.dumps(output),
            status_code=200
            )

    inserted, updated, deleted, insert_error, update_error, delete_error = merge(original_df=original_df, modified_df=modified_df, primary_key_columns=primary_key_column, full_merge=full_merge, user=req.form['user'], operator=req.form['operator'], base_url= base_url, table=req.form['table'], schema=req.form['schema'])


    output['inserted_rows'] = inserted
    output['updated_rows'] = updated
    output['deleted_rows'] = deleted
    output['insert_errors'] = 'insert_errors : ' + str(insert_error)
    output['update_errors'] = 'update_errors : ' + str(update_error)
    output['delete_errors'] = 'delete_errors : ' + str(delete_error)

    log.critical(output)

    return func.HttpResponse(
             json.dumps(output),
             status_code=200
        )