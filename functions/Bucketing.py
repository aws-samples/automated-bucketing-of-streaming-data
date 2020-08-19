import json
import boto3
import datetime
from time import sleep
import os

def lambda_handler(event, context):
    
    #get current date and last hour
    last_hour = datetime.datetime.now() - datetime.timedelta(hours = 1)
    dt = last_hour.strftime("%Y-%m-%d-%H")
    
    # athena client
    client = boto3.client('athena')    
    
    # athena settings
    database = os.environ['DATABASE']
    athena_result_bucket = os.environ['ATHENA_QUERY_RESULTS_LOCATION']
    SourceTable = os.environ['SOURCE_TABLE']
    TargetTable = os.environ['TARGET_TABLE']
    TargetTableLocation = os.environ['TARGET_TABLE_LOCATION']
    BucketingKey = os.environ['BUCKETING_KEY']
    BucketCount = os.environ['BUCKET_COUNT']
    
    # Create temp table
    CTASQuery = """
    CREATE TABLE TempTable
    WITH (
      format = 'PARQUET', 
      external_location = '%s/dt=%s/', 
      bucketed_by = ARRAY['%s'], 
      bucket_count = %s) 
    AS SELECT *
    FROM %s
    WHERE dt='%s';
    """ % (TargetTableLocation,dt,BucketingKey,BucketCount,SourceTable,dt)
    
    # execute first query to create previous hour table 
    CTASresponse = client.start_query_execution(
        QueryString=CTASQuery,
        QueryExecutionContext={
           'Database': database
        },
        ResultConfiguration={
            'OutputLocation': athena_result_bucket,
        }
    )
    query_execution_id = CTASresponse["QueryExecutionId"]
    query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
    query_execution_status = query_status["QueryExecution"]["Status"]["State"]
    
    # wait for first query to be executed successfully before executing second query
    while True:
        if query_execution_status == 'SUCCEEDED':
            print ("Loading Partitions")
            LoadPartionQuery="""
                ALTER TABLE %s
                ADD IF NOT EXISTS
                PARTITION (
                    dt='%s'
                );
                """ % (TargetTable,dt)
            LoadPartitionresponse = client.start_query_execution(
            QueryString=LoadPartionQuery,
            QueryExecutionContext={
                'Database': database
            },
            ResultConfiguration={
                'OutputLocation': athena_result_bucket,
            }
            )
            break
        
        if query_execution_status == 'FAILED': # or query_execution_status == 'CANCELLED':
            raise Exception("STATUS:" + query_execution_status)
            
        print (query_execution_status)
       # Sleep 10 ms to avoid throttling
        sleep(0.1)
        
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_execution_status = query_status["QueryExecution"]["Status"]["State"]

    # Drop temp table  
    DropTableQuery="DROP TABLE TempTable;"
    response = client.start_query_execution(
    QueryString=DropTableQuery,
    QueryExecutionContext={
        'Database': database
    },
    ResultConfiguration={
        'OutputLocation': athena_result_bucket,
    }
    )
