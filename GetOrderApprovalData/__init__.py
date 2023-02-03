import json
import logging
import pyodbc
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        operator = req_body.get('operatorCode')
        auditIds = req_body.get('auditIds')
        selectedReports = req_body.get('selectedReports')
        workflowType = req_body.get('workflowType')
        database = req_body.get('database')

    if operator and auditIds and selectedReports and database:
        response = getApprovalData(database, operator, auditIds ,workflowType ,selectedReports)
    else:
        response = {}
    return func.HttpResponse(
             json.dumps(response),
             status_code=200
        )

def getApprovalData(database:str, operator:str, auditIds:str, workflowType:str, selectedReports:str):

    cnxn = connectDatabase(database)
    schema = 'ZeusDataAudit'
    if(database == 'oasis'):
        schema = 'DataAudit'
    reportNames = selectedReports.split(',')
    reportNames.append('All')
    auditList = auditIds.split(',')
    reportNameClause = ""
    if len(reportNames) == 1:
        reportNameClause = "s.ReportName = '{}'".format(reportNames[0])
    elif len(reportNames) > 1:
        reportNameClause = reportNameClause + "s.ReportName LIKE '%" + "%' OR s.ReportName LIKE '%".join(reportNames) + "%'"

    # Cross join settings and audit data for the selected report names
    join_query = ("SELECT s.SettingsId,d.DataAuditId,s.API,s.CRS,s.County,s.WellName,s.Operator,s.ReportName "
    + ",s.ApproverEmail1,s.ApproverEmail2,s.ApproverEmail3,s.CopyEmail1,s.CopyEmail2,s.CopyEmail3,s.LastModifiedDate "
    + "FROM orders.ORDER_MANAGEMENT_SETTINGS s CROSS JOIN {}.DATA_AUDIT d ".format(schema)
    + "where ({}) AND (s.WorkflowType = '{}') AND s.active_yn = 'Y' AND (s.Operator = '{}') ORDER BY d.DataAuditId".format(reportNameClause,workflowType,operator))
    print(join_query)
    join_df = pd.read_sql(join_query, cnxn)
    join_df_count = join_df.shape[0]

    # Retrieve audit data
    audit_query = ("select d.DataAuditId,d.API,d.CRS,d.County,d.WellName,d.Operator FROM {}.DATA_AUDIT d".format(schema)
    +" WHERE d.DataAuditId IN ({}) ORDER BY d.DataAuditId".format((',').join(auditList)))
    print(audit_query)
    audit_df = pd.read_sql(audit_query, cnxn)
    audit_df_count = audit_df.shape[0]

    # merge every audit data withe the correspoding row in settings 
    merged_df = pd.merge(join_df, audit_df, how='inner', on = 'DataAuditId')
    
    merged_df.loc[(merged_df["ReportName"] == 'ALL') | (merged_df["ReportName"] == 'All'), "ReportName"] = selectedReports
    merged_df_count = merged_df.shape[0]

    
    def compare_function(row):
        return str(int(bool(row['{}_x'.format(i)] == row['{}_y'.format(i)]) and (bool(row['{}_x'.format(i)] != None) and bool(row['{}_x'.format(i)]) != None)))

    if(join_df_count > 0 and audit_df_count > 0 and merged_df_count > 0):
        # Calculate binary rank based on the precedence
        col_list = ['API', 'CRS', 'WellName', 'County', 'Operator' ]
        merged_df['Precedence'] = ""
        
        for i in col_list:
            merged_df['Precedence'] = merged_df['Precedence'] + merged_df.apply(compare_function, axis=1)
        
        merged_df['Score'] = merged_df.apply(lambda x: int(x['Precedence'],2), axis=1)

        # Explode rows with multiple reports into seperate rows
        final_df = merged_df.assign(ReportName=merged_df['ReportName'].str.split(',')).explode('ReportName')
        final_df = final_df.sort_values(by=['Score','LastModifiedDate'], ascending = [False, False]).drop_duplicates(['ReportName', 'DataAuditId'])        
        # convert final dataframe to json format
        final_df['WellName'] = final_df.pop('WellName_y')
        final_df['Operator'] = final_df.pop('Operator_y')
        final_df['API'] = final_df.pop('API_y')
        final_df = final_df.sort_values(by=['WellName'], ascending = [True])
        return final_df[['API','SettingsId','DataAuditId','WellName','Operator','ReportName','ApproverEmail1','ApproverEmail2','ApproverEmail3']].to_dict(orient ='records')
    else:
        audit_df['ReportName'] = selectedReports;
        return audit_df[['API','DataAuditId','WellName','Operator','ReportName']].to_dict(orient ='records')


def connectDatabase(database : str):
    userId = 'zenvoadmin'
    pwd = 'Kesh1v1@4321$%'
    cnxn_str = ("Driver=ODBC Driver 17 for SQL Server;"
                "Server=envoyapp.database.windows.net;"
                "Database={};"
                "UID={};"
                "PWD={};".format(database,userId,pwd))
    cnxn = pyodbc.connect(cnxn_str)
    return cnxn
