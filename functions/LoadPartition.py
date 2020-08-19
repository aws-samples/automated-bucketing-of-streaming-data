import json
import boto3
from datetime import datetime
import os

def lambda_handler(event, context):
    
    #get current date and hour
    now = datetime.now()
    dt = now.strftime("%Y-%m-%d-%H")
    
    # athena client
    client = boto3.client('athena')    
    
    # athena settings
    database = os.environ['DATABASE']
    athena_result_bucket = os.environ['ATHENA_QUERY_RESULTS_LOCATION']
    SourceTable = os.environ['SOURCE_TABLE']
    #database = "sensorDB"
    #athena_result_bucket = "s3://sensor-athena-blog/athena_results"
    
    LoadPartionQuery="""
        ALTER TABLE %s.%s
        ADD IF NOT EXISTS
        PARTITION (dt='%s');
        """ % (database,SourceTable,dt)
    
    LoadPartitionresponse = client.start_query_execution(
    QueryString=LoadPartionQuery,
    QueryExecutionContext={
    'Database': database
    },
    ResultConfiguration={
    'OutputLocation': athena_result_bucket,
    }
    )

    
    query_execution_id = LoadPartitionresponse["QueryExecutionId"]
    query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
    query_execution_status = query_status["QueryExecution"]["Status"]["State"]
    
    while True:
        if query_execution_status == 'SUCCEEDED':
            print ("Done")
            break
        if query_execution_status == 'FAILED':
            raise Exception("STATUS:" + query_execution_status)
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status["QueryExecution"]["Status"]["State"]    
        
    


