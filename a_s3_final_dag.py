from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import pandas as pd
import numpy as np
import json

# Import PandasSchema modules
from pandas_schema import Column, Schema
from pandas_schema.validation import MatchesPatternValidation, CustomElementValidation

import logging

from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_S3_CONN_ID = "s3_test"

def s3_extract():
   source_s3_key = "Example_Received_From_AW.csv"
   source_s3_bucket = "af-in"
#    dest_file_path = "/home/superset/airflow/sample_data"
   dest_file_path = "/opt/airflow//project1/sample_data"
   source_s3 = S3Hook(AWS_S3_CONN_ID)
   #s3_temp_file = source_s3.download_file(source_s3_key,source_s3_bucket,dest_file_path)
   s3_temp_file_path = source_s3.download_file(source_s3_key,source_s3_bucket,dest_file_path)
   #print('#############',type(s3_temp_file))
   #print('#############',s3_temp_file)
   return str(s3_temp_file_path)

def create_raw_json(ti):

    print("******* Picking file name ******")
    temp_file_name = ti.xcom_pull(task_ids=['s3_extract'])
    print(type(temp_file_name))
    print('*************', temp_file_name)
    temp_file_name = temp_file_name[0]
    print(type(temp_file_name))
    print('*************', temp_file_name)


    #Load csv Dataset as Pandas df
    print('******** Reading csv ***********')
    raw_df = pd.read_csv(temp_file_name)
    print('******** Processing df *********')
    #Imputing nulls with NaN's
    raw_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    #Replacing NaN's with None
    raw_df = raw_df.where(pd.notnull(raw_df), None)


    current_timestamp = datetime.now().strftime("%Y_%m_%d-%H:%M:%S")
    print(f"raw_json_{current_timestamp}")

    # staging_dir = '/home/superset/airflow/sample_data/'
    staging_dir = '/opt/airflow/project1/sample_data/'
    filename = 'raw_json'
    filetype = '.json'

    cust_filename = f"{filename}_{current_timestamp}{filetype}"
    print(cust_filename)

    fullpath = staging_dir+cust_filename
    print(fullpath)


    # Creating json from raw_df
    raw_df.to_json(path_or_buf=fullpath,orient='records',lines=False)
    print('***** Created raw_json.json *****')

    #json1_path = '/home/superset/airflow/sample_data/json1_raw_df.json'

    s3_filename = fullpath
    s3_key = cust_filename
    s3_bucket = "af-out"
    dest_s3 = S3Hook(AWS_S3_CONN_ID)
    print("******* Loading to s3 ******")
    dest_s3.load_file(s3_filename,s3_key,s3_bucket)
    print("******* Loaded to s3 bucket ******")


def start():
    print('***** Starting DAG *****')

def end():
    print('***** Ending DAG *****')


def create_err_json(ti):

    print('***** Starting json_2 task *****')

    print("******* Picking file name ******")
    temp_file_name = ti.xcom_pull(task_ids=['s3_extract'])
    print(type(temp_file_name))
    print('*************', temp_file_name)
    temp_file_name = temp_file_name[0]
    print(type(temp_file_name))
    print('*************', temp_file_name)

    #Load csv Dataset as Pandas df
    print('******** Reading csv ***********')
    raw_df = pd.read_csv(temp_file_name)
    print('******** Processing df *********')
    #Imputing nulls with NaN's
    raw_df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    #Replacing NaN's with None
    raw_df = raw_df.where(pd.notnull(raw_df), None)
    
    # Json-2(Pandas_Schema)

    #PandasSchema is a module for validating tabulated data, such as CSVs (Comma Separated Value files), and TSVs (Tab Separated Value files). It uses the incredibly powerful data analysis tool Pandas to do so quickly and efficiently.

    ## Defining custom functions to check integer and string datatypes

    # Custom function to check integer datatype
    def check_int(num):
        try:
            int(num)
        except ValueError:
            return False
        return True

    # Custom function to check string datatype
    def check_str(s):
        if isinstance(s, str):
            return True
        else:
            return False

    ## Defining Regex for phone and email

    phone_pattern = r"^0\d{4}([ ])\d{3}([ ])\d{3}$|^0\d{2}([ ])\d{3}([ ])\d{3}([ ])\d{2}$|^0\d{4}([ ])\d{6}$|^7\d{7}$|^44\d{9}$"
    #phone_pattern = r'^(?:(?:\(?(?:0(?:0|11)\)?[\s-]?\(?|\+)44\)?[\s-]?(?:\(?0\)?[\s-]?)?)|(?:\(?0))(?:(?:\d{5}\)?[\s-]?\d{4,5})|(?:\d{4}\)?[\s-]?(?:\d{5}|\d{3}[\s-]?\d{3}))|(?:\d{3}\)?[\s-]?\d{3}[\s-]?\d{3,4})|(?:\d{2}\)?[\s-]?\d{4}[\s-]?\d{4}))(?:[\s-]?(?:x|ext\.?|\#)\d{3,4})?$'
    email_pattern = r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$"

    ## Defining validation elements and schema

    # define validation elements
    string_validation = [CustomElementValidation(lambda s: check_str(s), 'is not string')]
    int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
    null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]

    # define validation schema
    schema = Schema([Column('No', null_validation + int_validation),
                    Column('Task Order', null_validation + string_validation),
                    Column('Property Reference', null_validation + string_validation),
                    Column('Customer Ref', null_validation + int_validation),
                    Column('Customer Name', null_validation + string_validation),
                    Column('CustomerNumber1', [MatchesPatternValidation(phone_pattern)]),
                    Column('Customer Number 2', [MatchesPatternValidation(phone_pattern)]),
                    Column('Customer Email', [MatchesPatternValidation(email_pattern)]),
                    Column('Address1', null_validation + string_validation),
                    Column('Address2', null_validation + string_validation),
                    Column('Address3', null_validation + string_validation),
                    Column('Postcode', null_validation + string_validation),
                    Column('Postal Code', null_validation + string_validation),
                    Column('Prop class', null_validation + string_validation),
                    Column('Cust class', null_validation + string_validation),
                    Column('DMA', null_validation + string_validation),
                    Column('Council', null_validation + string_validation),
                    Column('WRZ', null_validation + string_validation),
                    Column('Co-Ords', null_validation + string_validation),
                    Column('PSR Code', null_validation + string_validation),
                    Column('PSR Comments', null_validation + string_validation),
                    ])

    ## Performing validations on the data

    errors = schema.validate(raw_df)

    ## Parsing the output generated by pandas_schema validation

    x = pd.DataFrame(columns={'row', 'column', 'value', 'message'})
    x_row =[]
    x_col = []
    x_val = []
    x_msg = []
    for error in errors:
        x_row.append(error.row)
        x_col.append(error.column)
        x_val.append(error.value)
        x_msg.append(error.message)
    x['row']=x_row
    x['column']=x_col
    x['value']=x_val
    x['message']=x_msg
    x.loc[x.message.str.startswith('does not match the pattern'),'message']='does not match the required pattern'

    xx = x.groupby(['row', 'column', 'value'],dropna=False)['message'].apply(list).reset_index()

    def merge(vec):
        column = vec[0]
        value=vec[1]
        message=vec[2]
        row = vec[3]
        prm_key = raw_df.iloc[row]['Property Reference']
        return {prm_key:{column:{value:message}}}
    xx['merged'] = xx[['column','value','message','row']].apply(merge,axis=1,raw=True)
    xy = xx.groupby('row')['merged'].agg(list).reset_index()

    json2 = list(xy.merged)

    # Creating sample empty dictionary to append results
    json_2 = {}

    # Merging primary keys
    for ds in json2:
        d1 = {}
        for k in ds[0].keys():
            d1[k] = list(d1[k] for d1 in ds)
        json_2.update(d1)

    ## Generating Json-2

    # Serializing json 
    json_object = json.dumps(json_2, indent = 2)
    
    print('***** Generating json_2 *****')

    current_timestamp = datetime.now().strftime("%Y_%m_%d-%H:%M:%S")
    print(f"err_json_{current_timestamp}")

    # staging_dir = '/home/superset/airflow/sample_data/'
    staging_dir = '/opt/airflow/project1/sample_data/'
    filename = 'err_json'
    filetype = '.json'

    cust_filename = f"{filename}_{current_timestamp}{filetype}"
    print(cust_filename)

    fullpath = staging_dir+cust_filename
    print(fullpath)

    # Writing to sample.json
    with open(fullpath, "w") as outfile:
        outfile.write(json_object)

    #json2_path = '/home/superset/airflow/sample_data/json2_raw_df.json'

    s3_filename = fullpath
    s3_key = cust_filename
    s3_bucket = "af-out"
    dest_s3 = S3Hook(AWS_S3_CONN_ID)
    print("******* Loading to s3 ******")
    dest_s3.load_file(s3_filename,s3_key,s3_bucket)
    print("******* Loaded to s3 bucket ******")



with DAG(dag_id="a_s3_final_dag", start_date=datetime(2023,3,22),
         schedule_interval="@hourly", catchup=False, tags=['af_dbnd']) as dag:
    
    task1 = PythonOperator(
        task_id="start",
        python_callable=start)

    task2 = PythonOperator(
        task_id="s3_extract",
        python_callable=s3_extract)

    task3 = PythonOperator(
        task_id="create_raw_json",
        python_callable=create_raw_json)

    task4 = PythonOperator(
        task_id="create_err_json",
        python_callable=create_err_json)
    
    task5 = PythonOperator(
        task_id="end",
        python_callable=end)

task1 >> task2 >> [task3, task4] >> task5
