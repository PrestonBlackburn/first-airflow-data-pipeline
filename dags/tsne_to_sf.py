from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago, timedelta

from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook


import snowflake.connector

import boto3
import pandas as pd
from time import gmtime, strftime, sleep


args = {
    'owner': 'pb',
    'start_date': days_ago(1)
}

dag = DAG(dag_id = 'TSNE_TO_SNOWFLAKE',
        description = 'perform the tsne decomposition \
            then write the results to snowflake',
        default_args=args, 
        schedule_interval=None)


dag_config = Variable.get("data_creds", deserialize_json=True)
boto_bucket_name = dag_config['aws_boto_credentials']['bucket_name']
boto_secret_key = dag_config['aws_boto_credentials']['secret_key']
boto_access_key = dag_config['aws_boto_credentials']['access_key']

sagemaker_access_key = dag_config['aws_sm_credentials']['access_key']
sagemaker_secret_key = dag_config['aws_sm_credentials']['secret_key']

sf_user = dag_config['snowflake_credentials']['user']
sf_password = dag_config['snowflake_credentials']['password']
sf_account = dag_config['snowflake_credentials']['account']

source_bucket_key = "censustsne.csv"

def trigger_processing_job():
    client = boto3.client(service_name="sagemaker", region_name = 'us-east-2', aws_access_key_id=sagemaker_access_key,aws_secret_access_key=sagemaker_secret_key, )

    pipeline_execution_name = "airflow-execution" + strftime("%Y-%m-%d-%H-%M-%S", gmtime())

    response = client.start_pipeline_execution(
        PipelineName = 'USCensusPipeline',
        PipelineExecutionDisplayName=pipeline_execution_name,
        PipelineParameters =[],
        PipelineExecutionDescription = 'task triggered from airflow to generate tsne results',
    )
  
    print(response)



def s3_to_snowflake():
    s3 = boto3.client('s3',
                aws_access_key_id = boto_access_key,
                aws_secret_access_key= boto_secret_key)
    
    census_tsne_response = s3.get_object(Bucket = boto_bucket_name,
                                        Key = source_bucket_key)


    census_tsne_df = pd.read_csv(census_tsne_response.get("Body"))

    # Get columns and data types to create snowflake table
    sf_data_types = ["varchar(250)" if pd_dtype=="object" else "float" for pd_dtype in census_tsne_df.dtypes]
    col_names = census_tsne_df.columns.values


    # use the s3 to snowflake operator to move data from s3 stage to snowflake db
    # Create the snowflake table
    create_table_info = ', '.join([str(col_name) + " " + sf_dtype for
                                col_name,sf_dtype in zip(col_names, sf_data_types)])

    sf_create_table_query = f"CREATE OR REPLACE TABLE TSNE_CENSUS ({create_table_info});"

    
    # upload csv with no headers
    s3_stage_to_sf = f"COPY INTO TSNE_CENSUS FROM @census_s3_stage \
                            FILE_FORMAT = (type= csv skip_header=1) \
                            pattern = '.*{source_bucket_key}';"

    ctx = snowflake.connector.connect(
        user= sf_user,
        password = sf_password,
        account = sf_account,
        database = "US_CENSUS_DATA", 
        schema = "Public"
    )

    cs = ctx.cursor()
    print("connected to snowflake")

    cs.execute(sf_create_table_query)
    print("created snowflake table")

    cs.execute(s3_stage_to_sf)
    print("loaded data to snowflake table")

    top_n = 5
    cs.execute(f"SELECT * FROM TSNE_CENSUS LIMIT {top_n}")
    top_rows = [row for row in cs]
    print(top_rows)

    cs.close()
    ctx.close()



with dag:
    start_processing_job = PythonOperator(
        task_id = "start_sagemaker_processing_job",
        python_callable=trigger_processing_job,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=2)
    )

    processing_delay_task = PythonOperator(
        task_id="wait_for_processing_job",
        python_callable=lambda: sleep(900))

    push_to_snowflake = PythonOperator(
        task_id = "transformed_data_to_snowflake",
        python_callable = s3_to_snowflake,
        provide_context=True,
        retries=0,
        retry_delay=timedelta(seconds=2)
    )


start_processing_job >> processing_delay_task >> push_to_snowflake
