# Automated Bucketing of Streaming Data

  
This repository accompanies the [Automating bucketing of streaming data using Amazon Athena and AWS Lambda](https://aws-preview.aka.amazon.com/blogs/big-data/automating-bucketing-of-streaming-data-using-amazon-athena-and-aws-lambda/) blog post. It contains an [AWS Serverless Application Model (AWS SAM)](https://aws.amazon.com/serverless/sam/) template that deploys two [AWS Lambda](https://aws.amazon.com/lambda) functions; LoadPartiton and Bucketing functions.

The [LoadPartition](./functions/LoadPartition.py) function, runs every hour and reads the new folder created under /raw folder and loads this folder as a new partition to the SourceTable. 

The [Bucketing](./functions/Bucketing.py) function, runs every hour and copies previous hour's data from /raw to /curated by executing [Create Table AS Select (CTAS)](https://docs.aws.amazon.com/athena/latest/ug/ctas.html) query on [Amazon Athena](https://aws.amazon.com/athena). The copied data is a new sub-folder under /curated. The function will then load the new folder as a Partition to TargetTable.  

```bash
├── README.MD <-- This instructions file

├── functions <-- Two lambda functions used to bucket streaming data
```

  

## Requirements

  

* AWS CLI already configured with Administrator permission
* Source and target tables created in Athena
* Streaming data is writing into Amazon S3 bucket and partitioned like this: dt=YYYY-mm-dd-HH



  

## Installation Instructions

1. [Install SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) if you do not have it.
2. Clone the repo onto your local development machine using `git clone https://github.com/aws-samples/automated-bucketing-of-streaming-data.git`.
3.  [OPTIONAL]The lambda functions included here, works on data that is partitioned on hourly basis. It will work with flat partition strategy that looks like the following; dt=YYYY-mm-dd-HH. If your data has a different structure, edit the lambda functions accordingly.


  
4. From the command line, change directory to SAM template directory

```

sam build

sam deploy --guided

```

Follow the prompts in the deploy process to set the stack name, AWS Region and other parameters.

  

## Parameter Details

  

* S3BucketName: the name of data lake S3 bucket for this application 
* CuratedKeyPrefix: Prefix of new bucketed files that are written by Function2. This is the Amazon S3 location of TargetTable without 's://<s3_bucket_name>'.
**Do not add the trailing slash**. For example, /curated
* AthenaResultLocation: Full S3 location where Athena will store query results in. For example, s3://<s3_bucket_name>/athena_results
* DatabaseName: Data Catalog Database name that holds SourceTable and TargetTable
* SourceTableName: Source Table Name that points to raw data
* TargetTableName: Target Table name that points to curated data
* BucketingKey: The column used as a bucketing key. The solution supports a single bucketing key, to add more edit the lambda function.
* BucketCount: Number of hive buckets to create within a partition. This has to be the same number that was used when creating TargetTable.

  

## How it works

  

* Start writing streaming data to S3 bucket
* Create SourceTable and TargetTable in Athena

* After an hour of the SAM deployment, you will see new data written ***CuratedKeyPrefix***. The data will be bucketed and could be queried from TargetTable in Athena.
## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the [LICENSE](LICENSE) file.

