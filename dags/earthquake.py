import pandas as pd
from urllib.request import urlopen
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage
from datetime import datetime
import os
from airflow.providers.google.cloud.operators.bigquery import (BigQueryCreateEmptyDatasetOperator,
                                                               BigQueryDeleteDatasetOperator,BigQueryCreateExternalTableOperator)


dag = DAG(dag_id='earthquake_dag', start_date=datetime(2024,4,16))

airflow_home = os.environ.get("AIRFLOW_HOME")
# project_id="earthquake-170648"
# BUCKET_NAME = 'rawdata_earth'
# DATASET = "earthquake"
# LOCATION = "us-central1"
key_path = f"{airflow_home}/application_default_credentials.json"
dataset_name = os.environ.get('rawdata_earth', 'earthquake_dataset')
table_name = os.environ.get('earthquake_data_2014-01.parquet', 'earthquake_table')
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


def earthquake_data_upload():
    list_of_df = []

    for year in range(2014,2017):

        for month in range(1, 6):
            month = '%0.2d' % month
            for day in range(1, 5):
                day_str = '%0.2d' % day
                next_day_str = '%0.2d' % (day + 1)

                # next_month = '%0.2d' % (month + 1)
                with urlopen(
                        f'https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={year}-{month}-{day_str}'
                        f'&endtime={year}-{month}-{next_day_str}') as url:
                    json_data = json.load(url)

                for i in range(0, len(json_data['features'])):
                    features = json_data['features'][i]['properties']
                    df = pd.json_normalize([features])
                    list_of_df.append(df)
            final_df = pd.concat(list_of_df)

            final_df.head()
            # datetime_object = datetime.datetime.strptime(month, "%m")
            # month_name = datetime_object.strftime("%B")

            filename = f'earthquake_data_{year}.parquet'
            final_df.to_parquet(filename, index=False)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client.from_service_account_json(key_path)
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


# # upload_to_gcs(bucket='rawdata_earth', object_name='earthquake',
#               local_file='earthquake_data_2014-02.parquet')


def iterate_upload_to_gcs():
    for year in range(2014,2017):
        # for month in range(1, 13):
        #     month = '%0.2d' % month
        upload_to_gcs(bucket='rawdata_earth', object_name=f'earthquake_data_{year}.parquet',
                      local_file=f'earthquake_data_{year}.parquet')


def cleanup_local_file():
    for year in range(2014,2017):
        # for month in range(1, 13):
        #     month = '%0.2d' % month
        os.remove(f'earthquake_data_{year}.parquet')



with dag:
    earthquake_data_upload_task = PythonOperator(task_id='earthquake_data_upload',
                                                 python_callable=earthquake_data_upload)

    iterate_upload_to_gcs_task = PythonOperator(task_id='upload_to_gcs', python_callable=iterate_upload_to_gcs)

    cleanup_local_file_task = PythonOperator(task_id='cleanup_local_file', python_callable=cleanup_local_file)

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": "earthquake-170648",
                "datasetId": "earthquake_dataset",
                "tableId": "earthquake_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": ["gs://rawdata_earth/*.parquet"]
            },
        },
    )


(earthquake_data_upload_task >> iterate_upload_to_gcs_task >> cleanup_local_file_task >> bigquery_external_table_task
 )




