import json
import logging
import pyodbc
import pandas as pd
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function to retrieve orders with approver details.')

    response = getOrdersWithApprovers()
       
    return func.HttpResponse(
             json.dumps(response,default=str),
             status_code=200
        )

def getOrdersWithApprovers():
    # Establisc DB connection
    cnxn = connectDatabase()

    # Cross join settings and audit data table
    join_query = ("SELECT s.Id as SettingsId,d.DataAuditId,s.API,s.CRS,s.County,s.WellName,s.Operator,s.ReportName "
        + ",s.OwnerEmail1,s.OwnerEmail2,s.OwnerEmail3,s.CopyEmail1,s.CopyEmail2,s.CopyEmail3,s.LastModifiedDate "
        + "FROM orders.ORDER_MANAGEMENT_SETTINGS s CROSS JOIN ZeusDataAudit.DATA_AUDIT d "
        + "where d.Operator = s.Operator ORDER BY d.DataAuditId")
    join_df = pd.read_sql(join_query, cnxn)
    join_df_count = join_df.shape[0]
    if(join_df_count <= 0):
        return {}
    #method to extract auditIds from Data column
    def extract_auditIds(row):
        df_nested_list = pd.json_normalize(row['Data'])
        return df_nested_list['DataAuditId'].tolist()

    #method to mark matching records b/w audit and order tabe
    def match_auditIds(row):
        return row['DataAuditId'] in row['AuditIds']
        
    order_query= ("select OrderId,TradeGroupId,TradeType,StartDate,EndDate,Amount,Comments, "
    +"RequestedBy,RequestedTo,SelectedReports AS ReportName,Data,OrderStatus,OrderedBy,OrderedDate from orders.ORDER_MANAGEMENT")
    order_df = pd.read_sql(order_query, cnxn)
    order_df_count = order_df.shape[0]
    if(order_df_count <= 0):
        return {}
    order_df['Data'] = order_df['Data'].apply(json.loads)
    order_df['AuditIds'] =order_df.apply(extract_auditIds, axis=1)

    # Retrieve audit data
    audit_query = ("select DataAuditId,API,CRS,County,WellName,Operator "
    +" FROM ZeusDataAudit.DATA_AUDIT ORDER BY DataAuditId")
    audit_df = pd.read_sql(audit_query, cnxn)
    audit_df_count = audit_df.shape[0]
    if(audit_df_count <= 0):
        return {}

    merged_df = pd.merge(order_df,audit_df,how='inner', left_on=['RequestedTo'],right_on=['Operator'])
    merged_df['Match'] = merged_df.apply(match_auditIds, axis=1)
    merged_df = merged_df[merged_df['Match'] == True]
    merged_df_count = merged_df.shape[0]
    if(merged_df_count <= 0):
        return {}
    

    # merge join_df and merged_df
    result_df = pd.merge(join_df, merged_df, how='right', on = 'DataAuditId')
    result_df_count = result_df.shape[0]
    if(result_df_count <= 0):
        return {}
    result_df.loc[(result_df["ReportName_x"] == 'ALL') | (result_df["ReportName_x"] == 'All'), "ReportName_x"] = result_df['ReportName_y']
    
    def compare_function(row):
        return str(int(bool(row['{}_x'.format(i)] == row['{}_y'.format(i)]) and (bool(row['{}_x'.format(i)] != None) and bool(row['{}_x'.format(i)]) != None)))

    # Calculate binary rank based on the precedence
    col_list = ['ReportName','API', 'CRS', 'WellName', 'County']
    result_df['Precedence'] = ""

    for i in col_list:
        result_df['Precedence'] = result_df['Precedence'] + result_df.apply(compare_function, axis=1)

    result_df['Score'] = result_df.apply(lambda x: int(x['Precedence'],2), axis=1)
    
    # Sort Score and LastModifiedDate in descending
    final_df = result_df.sort_values(by=['Score','LastModifiedDate'], ascending = [False, False]).drop_duplicates(['ReportName_y', 'DataAuditId'])     # convert final dataframe to json format
    final_df['SelectedReports'] = final_df.pop('ReportName_y')
    return final_df[['OrderId','TradeGroupId','TradeType','StartDate','EndDate','Amount','Comments',
    'RequestedBy','RequestedTo','SelectedReports','Data','OrderStatus','OrderedBy','OrderedDate','OwnerEmail1','OwnerEmail2','OwnerEmail3']].to_dict(orient ='records')

def connectDatabase():
    cnxn_str = ("Driver={ODBC Driver 17 for SQL Server};"
                "Server=envoyapp.database.windows.net;"
                "Database=envoy_dev;"
                "UID=zenvoadmin;"
                "PWD=Kesh1v1@4321$%;")
    cnxn = pyodbc.connect(cnxn_str)
    return cnxn