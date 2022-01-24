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
from io import StringIO

args = {
    'owner': 'pb',
    'start_date': days_ago(1)
}

dag = DAG(dag_id = 'Aurora_DB_To_S3_CSV',
        description = 'Copy the us demographics table \
            and metadata table to an s3 stage for Snowflake',
        default_args=args, 
        schedule_interval=None)

dag_config = Variable.get("data_creds", deserialize_json=True)
boto_bucket_name = dag_config['aws_boto_credentials']['bucket_name']
boto_secret_key = dag_config['aws_boto_credentials']['secret_key']
boto_access_key = dag_config['aws_boto_credentials']['access_key']

sf_user = dag_config['snowflake_credentials']['user']
sf_password = dag_config['snowflake_credentials']['password']
sf_account = dag_config['snowflake_credentials']['account']


upload_bucket_key = "us_census_data_extract_from_airflow.csv"
transformed_census_data_file = 'transformed_census_data_from_airflow.csv'


def test_aurora_db_source():
    request = "SELECT * FROM US_DEMOGRAPHICS LIMIT 5;"
    mysql_hook = MySqlHook(mysql_conn_id="mysql_aurora_connection")
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    top_5 = [row for row in cursor]
    print(top_5)

s3_query = "SELECT * FROM US_DEMOGRAPHICS;"

def transform_s3_data():
    s3 = boto3.client('s3',
                    aws_access_key_id = boto_access_key,
                    aws_secret_access_key= boto_secret_key)
    
    census_table_metadata_response = s3.get_object(Bucket = boto_bucket_name,
                                         Key ="us_census_metadata_extract.csv")

    census_table_metadata_df = pd.read_csv(census_table_metadata_response.get("Body"))
    census_table_data_response = s3.get_object(Bucket = boto_bucket_name, Key = upload_bucket_key)
    census_table_data_df = pd.read_csv(census_table_data_response.get("Body"))

    # do some minor cleanup
    # Update column names
    # remove columns with no data in them
    # remove duplicate columns
    census_table_data_df.columns = census_table_metadata_df['Description'].values
    census_table_data_df = census_table_data_df.dropna(axis=1, how='all')
    census_table_data_df = census_table_data_df.loc[:,~census_table_data_df.columns.duplicated()]

    # prep column names to be imported into snowflake:
    # 1. must contain only letters, numbers or underscores
    # 2. must begin with letter or underscore
    # 3. must be less than 251 characters
    updated_column_names = census_table_data_df.columns.values.tolist()

    # I should probably just use regex for this - revisit later
    updated_column_names = [label.replace(" ", "_") for label in updated_column_names]
    updated_column_names = [label.replace(".", "") for label in updated_column_names]
    updated_column_names = [label.replace("-", "") for label in updated_column_names]
    updated_column_names = [label.replace("'", "") for label in updated_column_names]
    updated_column_names = [label.replace(",", "") for label in updated_column_names]
    updated_column_names = [label.replace(")", "") for label in updated_column_names]
    updated_column_names = [label.replace("(", "") for label in updated_column_names]
    updated_column_names = ["_"+label if label[0].isdigit() else label for label in updated_column_names]
    updated_column_names = [label[:250] for label in updated_column_names]

    census_table_data_df.columns = updated_column_names

    census_table_data_with_headers = census_table_data_df
    # Write file back to s3 from memory
    csv_buffer = StringIO()
    census_table_data_df.to_csv(csv_buffer, index=False, header=True)
    response = s3.put_object(Bucket = boto_bucket_name, Key=transformed_census_data_file, Body=csv_buffer.getvalue())
    print("saved csv with header to s3, response: ", response['ResponseMetadata']['HTTPStatusCode'])


def s3_to_snowflake():
    s3 = boto3.client('s3',
                aws_access_key_id = boto_access_key,
                aws_secret_access_key= boto_secret_key)
    
    census_table_transformed_response = s3.get_object(Bucket = boto_bucket_name,
                                        Key = transformed_census_data_file)


    census_table_transf_df = pd.read_csv(census_table_transformed_response.get("Body"))

    # Get columns and data types to create snowflake table
    sf_data_types = ["varchar(250)" if pd_dtype=="object" else "float" for pd_dtype in census_table_transf_df.dtypes]
    col_names = census_table_transf_df.columns.values


    # use the s3 to snowflake operator to move data from s3 stage to snowflake db
    # Create the snowflake table
    create_table_info = ', '.join([str(col_name) + " " + sf_dtype for
                                col_name,sf_dtype in zip(col_names, sf_data_types)])

    sf_create_table_query = f"CREATE OR REPLACE TABLE US_DEMOGRAPHICS ({create_table_info});"

    
    # upload csv with no headers
    s3_stage_to_sf = f"COPY INTO US_DEMOGRAPHICS FROM @census_s3_stage \
                            FILE_FORMAT = (type= csv skip_header=1) \
                            pattern = '.*{transformed_census_data_file}';"

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
    cs.execute(f"SELECT * FROM US_DEMOGRAPHICS LIMIT {top_n}")
    top_rows = [row for row in cs]
    print(top_rows)

    cs.close()
    ctx.close()

    
def delete_s3_data():
    # delete any files from the previous runs

    s3 = boto3.client('s3',
            aws_access_key_id = boto_access_key,
            aws_secret_access_key= boto_secret_key)

    s3.delete_object(Bucket=boto_bucket_name, Key=upload_bucket_key)
    s3.delete_object(Bucket=boto_bucket_name, Key=transformed_census_data_file)


def snowflake_tsne_analysis_call():
    pass




with dag:
    start_task = PythonOperator(
        task_id = "previous_file_cleanup_task",
        python_callable=delete_s3_data,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=2)
    )

    read_db = PythonOperator(
        task_id ='select_aurora_db_data',
        python_callable=test_aurora_db_source,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=2)
    )

    write_to_s3 = MySQLToS3Operator(
        task_id = "export_mysql_to_s3",
        query=s3_query, 
        s3_bucket= boto_bucket_name,
        s3_key= upload_bucket_key,
        mysql_conn_id = "mysql_aurora_connection",
        aws_conn_id = "s3_connection",
        header = True
    )

    transform_s3 = PythonOperator(
        task_id = "transform_s3_data",
        python_callable = transform_s3_data,
        provide_context=True,
        retries=0,
        retry_delay=timedelta(seconds=2)
    )

    push_to_snowflake = PythonOperator(
        task_id = "transformed_data_to_snowflake",
        python_callable = s3_to_snowflake,
        provide_context=True,
        retries=0,
        retry_delay=timedelta(seconds=2)
    )


start_task >> read_db >> write_to_s3 >> transform_s3 >> push_to_snowflake
