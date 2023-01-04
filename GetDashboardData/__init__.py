import json
import logging
import pyodbc
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function to retrieve dashboard data.')
    response = getDashboardData()
    return func.HttpResponse(
        json.dumps(response,default=str),
             status_code=200
        )

def getDashboardData():
    # Establisc DB connection
    cnxn = connectDatabase()

    # Cross join settings and audit data for the selected report names
    join_query = ("SELECT s.Id as SettingsId,d.DataAuditId,s.API,s.CRS,s.County,s.WellName,s.Operator,s.ReportName, "
    + "s.OwnerEmail1,s.OwnerEmail2,s.OwnerEmail3,s.CopyEmail1,s.CopyEmail2,s.CopyEmail3 "
    + "FROM orders.ORDER_MANAGEMENT_SETTINGS s CROSS JOIN ZeusDataAudit.DATA_AUDIT d "
    + "ORDER BY d.DataAuditId")
    join_df = pd.read_sql(join_query, cnxn)
    return join_df.to_dict(orient ='records')
    
def connectDatabase():
    cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=envoyapp.database.windows.net;"
                "Database=envoy_dev;"
                "UID=zenvoadmin;"
                "PWD=Kesh1v1@4321$%;")
    cnxn = pyodbc.connect(cnxn_str)
    return cnxn