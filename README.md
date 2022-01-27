# first-airflow-data-pipeline
An intro to airflow and EtLT pipeline for US census data

### Folders: 
Dags - Dags that were used in airflow  
Sagemaker - Sagemaker processing job code to generate t-sne data  
Table Creation Github - Creating the initial data in aurora db from csv files  

#### Note:
Added "apache-airflow-providers-snowflake" as an additional dependency in the docker compose file  
This allows for an easy setup of the snowflake connection.


#### Other:
Using airflow v2.0

See the walkthrough of the project here: https://www.prestonblackburn.com/projects/CensusDataEngPipeline 
