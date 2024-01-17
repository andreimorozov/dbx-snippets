# Databricks notebook source
# MAGIC %md # Get survey responses

# COMMAND ----------

# MAGIC %md ## Flow:
# MAGIC - connect to database, resolve namespace
# MAGIC - resolve arguments, secrets
# MAGIC - check for continuation token in progress information store
# MAGIC - build web request_1 to start file compilation in Qualtrics, send request_1
# MAGIC - using returned ProgressId, build and dispatch the web request_2 to see if file compilation process is complete
# MAGIC   - repeat sending web request_2 in 1 second intervals until file compilation is complete
# MAGIC - using returned FileId, download and save the file in S3
# MAGIC - store new continuation token in progress information store

# COMMAND ----------

# DBTITLE 1,connect to database, resolve namespace
# MAGIC %run ./../../../../includes/configuration

# COMMAND ----------

spark.sql(f"USE {namespace}_system;")

# COMMAND ----------

# DBTITLE 1,resolve arguments, secrets, set constants
dbutils.widgets.removeAll()

# reading arguments:
dbutils.widgets.text('aws_bucket_name','nuro-databricks')
aws_bucket_name = getArgument('aws_bucket_name')

dbutils.widgets.text('mount_name','surveys-qualtrics-s3')
mount_name = getArgument('mount_name')

dbutils.widgets.text('survey_id','')
survey_id = getArgument('survey_id')

dbutils.widgets.text('process_timestamp','')
process_timestamp = getArgument('process_timestamp')

#reading secrets:
hostname = dbutils.secrets.get(scope='qualtrics', key = 'hostname')
token = dbutils.secrets.get(scope='qualtrics', key = 'token')
access_key = dbutils.secrets.get(scope='qualtrics', key = 'aws_access_key')
secret_key = dbutils.secrets.get(scope='qualtrics', key = 'aws_secret_key')
encoded_secret_key = secret_key.replace('/','%2F')

# vars:
mount_path = '/mnt/' + mount_name
s3_path = f'surveys/qualtrics/{namespace}/{process_timestamp}/{survey_id}'

api_route_export_responses = f'/API/v3/surveys/{survey_id}/export-responses'

# COMMAND ----------

# DBTITLE 1,check continuation token
import pandas as pd

continuation_token = ''
rows = spark.sql(f"SELECT continuation_token FROM surveys_qualtics_continuation_tokens WHERE survey_id = '{survey_id}' ORDER BY created_at DESC LIMIT (1)")
if(rows.count() == 1):
  continuation_token = rows.first()['continuation_token']

# COMMAND ----------

# DBTITLE 1,build web request_1, send it, on success set progress_id, on failure exit notebook
import http.client
import json

payload = ''
if(continuation_token == ''):
  payload = '{"format": "json", "compress": false, "allowContinuation": true}'
else:
  payload = f'{{"format": "json", "compress": false, "continuationToken": "{continuation_token}"}}'

conn = http.client.HTTPSConnection(hostname)
payload = payload
headers = {
  'Content-Type': 'application/json',
  'X-API-TOKEN': token
}
conn.request('POST', api_route_export_responses, payload, headers)
res = conn.getresponse()
data_response_1 = res.read()
conn.close()

json_response_1 = json.loads(data_response_1)
http_status = json_response_1['meta']['httpStatus']

if(http_status != '200 - OK'):
  dbutils.notebook.exit(json_response_1['meta']['error']['errorMessage'])

progress_id = json_response_1['result']['progressId']
# print(progress_id)

# COMMAND ----------

# DBTITLE 1,using progress_id, check if the file is ready
import time
time.sleep(3)

file_id = ''
counter = 1
conn_2 = http.client.HTTPSConnection(hostname)

while counter < 30:
  # todo: replace with hostname  
  headers = {
    'Content-Type': 'application/json',
    'X-API-TOKEN': token
  }
  api_route_check_progress = f'{api_route_export_responses}/{progress_id}'
  conn_2.request('GET', api_route_check_progress, '', headers)
  res_2 = conn_2.getresponse()
  data_response_2 = res_2.read()
  conn_2.close()

  json_response_2 = json.loads(data_response_2)
#   print(json_response_2)

  http_status = json_response_2['meta']['httpStatus']
  if(http_status != '200 - OK'):
    dbutils.notebook.exit(json_response_2['meta']['error']['errorMessage'])

  status = json_response_2['result']['status']
  
  if(status == 'complete'):
    file_id = json_response_2['result']['fileId']
    continuation_token = json_response_2['result']['continuationToken']
    break
  
  time.sleep(1)
  counter += 1
  
# print(counter)
# print(file_id)
# print(continuation_token)

# COMMAND ----------

# DBTITLE 1,store continuation_token for future use
dbutils.widgets.text('continuation_token', continuation_token)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO surveys_qualtics_continuation_tokens VALUES ('$survey_id','$continuation_token',now());

# COMMAND ----------

# DBTITLE 1,download the prepared file and save it in S3
# get the file using file_id

conn_3 = http.client.HTTPSConnection(hostname)
headers = {
  'Content-Type': 'application/json',
  'X-API-TOKEN': token
}
api_route_get_file = f'{api_route_export_responses}/{file_id}/file'
conn_3.request('GET', api_route_get_file, '', headers)
res_3 = conn_3.getresponse()
data_response_3 = res_3.read()
conn_3.close()

data_response_3_decoded = data_response_3.decode('utf-8')

file_name_survey_responses = 'survey_responses.json'
dbutils.fs.put(f'{mount_path}/{s3_path}/{file_name_survey_responses}', data_response_3_decoded, True)

# COMMAND ----------

# DBTITLE 1,Start download csv for anonymous surveys usage
payload = '{"format": "csv", "compress": false, "limit": 0, "newlineReplacement": ""}' # Limit 0 since we only want the mapping metadata row
csv_conn = http.client.HTTPSConnection(hostname)
headers = {
  'Content-Type': 'application/json',
  'X-API-TOKEN': token
}

csv_conn.request('POST', api_route_export_responses, payload, headers)
csv_res = csv_conn.getresponse()
csv_data_response_1 = csv_res.read()
csv_conn.close()

csv_json_response_1 = json.loads(csv_data_response_1)
http_status = csv_json_response_1['meta']['httpStatus']

if(http_status != '200 - OK'):
  dbutils.notebook.exit(csv_json_response_1['meta']['error']['errorMessage'])

progress_id = csv_json_response_1['result']['progressId']

# COMMAND ----------

# DBTITLE 1,Check for progress on CSV download
time.sleep(3)

file_id = ''
counter = 1
csv_conn_2 = http.client.HTTPSConnection(hostname)

while counter < 30:
  # todo: replace with hostname  
  headers = {
    'Content-Type': 'application/json',
    'X-API-TOKEN': token
  }
  api_route_check_progress = f'{api_route_export_responses}/{progress_id}'
  csv_conn_2.request('GET', api_route_check_progress, '', headers)
  csv_res_2 = csv_conn_2.getresponse()
  csv_data_response_2 = csv_res_2.read()
  conn_2.close()

  csv_json_response_2 = json.loads(csv_data_response_2)
#   print(json_response_2)

  http_status = csv_json_response_2['meta']['httpStatus']
  if(http_status != '200 - OK'):
    dbutils.notebook.exit(csv_json_response_2['meta']['error']['errorMessage'])

  status = csv_json_response_2['result']['status']
  
  if(status == 'complete'):
    file_id = csv_json_response_2['result']['fileId']
    break
  
  time.sleep(1)
  counter += 1
  
# print(counter)
# print(file_id)
# print(continuation_token)

# COMMAND ----------

# get the file using file_id
from re import sub

csv_conn_3 = http.client.HTTPSConnection(hostname)
headers = {
  'Content-Type': 'text/csv',
  'X-API-TOKEN': token
}
api_route_get_file = f'{api_route_export_responses}/{file_id}/file'
csv_conn_3.request('GET', api_route_get_file, '', headers)
csv_res_3 = csv_conn_3.getresponse()
csv_data_response_3 = csv_res_3.read()
csv_conn_3.close()

csv_data_response_3_decoded = csv_data_response_3.decode('utf-8')

dbutils.fs.put(f'{mount_path}/{s3_path}/survey_responses.csv', csv_data_response_3_decoded, True)
