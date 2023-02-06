import json
import logging
import pyodbc
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function to validate placed order.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass
    else:
        operator = req_body.get('operatorCode')
        auditIds = req_body.get('auditIds')
        selectedReports = req_body.get('selectedReports')
        database = req_body.get('database')

    if operator and auditIds and selectedReports and database:
        response = validate(database, operator, auditIds, selectedReports)
    else:
        response = {}
    return func.HttpResponse(
             json.dumps(response),
             status_code=200
        )

def validate(database:str, operator:str, auditIds:str, selectedReports:str):

    cnxn = connectDatabase(database)
    schema = 'ZeusDataAudit'
    if(database == 'oasis'):
        schema = 'DataAudit'
    reportNames = selectedReports.split(',')
    auditList = auditIds.split(',')
    reportNameClause = ""
    if len(reportNames) == 1:
        reportNameClause = "s.ReportName = '{}'".format(reportNames[0])
    elif len(reportNames) > 1:
        reportNameClause = reportNameClause + "s.ReportName LIKE '%" + "%' OR s.ReportName LIKE '%".join(reportNames) + "%'"

    # Retrieve audit data
    audit_query = ("select d.* FROM {}.DATA_AUDIT d".format(schema)
    +" WHERE d.DataAuditId IN ({}) ORDER BY d.DataAuditId".format((',').join(auditList)))
    print(audit_query)
    audit_df = pd.read_sql(audit_query, cnxn)
    audit_df_count = audit_df.shape[0]
        
    def compare_function(row):
        if(row[report] != None):
            return report +':'+ str(int(bool(row[report] == 1)))

    if(audit_df_count > 0 ):
        audit_df['Result'] = ""        
        for idx, report in enumerate(reportNames):
            if(idx == len(reportNames) - 1):
                audit_df['Result'] = audit_df['Result'] + audit_df.apply(compare_function, axis=1)
            else:
                audit_df['Result'] = audit_df['Result'] + audit_df.apply(compare_function, axis=1) + ', '
        return audit_df[['DataAuditId','API','WellName','Operator','Result']].to_dict(orient ='records')

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
