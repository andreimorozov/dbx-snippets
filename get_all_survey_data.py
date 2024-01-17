# Databricks notebook source
# MAGIC %md # Get latest surveys' data

# COMMAND ----------

# MAGIC %md ## flow description
# MAGIC - set constants, vars, resolve secrets
# MAGIC - ensure drive mount
# MAGIC - generate timestamp
# MAGIC - find all the surveys that are active or unfinished
# MAGIC - for each survey get schema, metadata, questions, latest responses and store the data in S3

# COMMAND ----------

# MAGIC %run ../../../../includes/main/parallelization

# COMMAND ----------

# MAGIC %md ## set constants, vars, resolve secrets

# COMMAND ----------

aws_bucket_name = 'nuro-databricks'
mount_name = 'surveys-qualtrics-s3'
mount_path = '/mnt/' + mount_name

#reading secrets:
hostname = dbutils.secrets.get(scope='qualtrics', key = 'hostname')
token = dbutils.secrets.get(scope='qualtrics', key = 'token')

access_key = dbutils.secrets.get(scope='qualtrics', key = 'aws_access_key')
secret_key = dbutils.secrets.get(scope='qualtrics', key = 'aws_secret_key')
encoded_secret_key = secret_key.replace('/','%2F')

api_route = '/API/v3/surveys'

# COMMAND ----------

# MAGIC %md ## ensure drive mount

# COMMAND ----------

if(not any(mount.mountPoint == mount_path for mount in dbutils.fs.mounts())):
  dbutils.fs.mount('s3a://%s:%s@%s' % (access_key, encoded_secret_key, aws_bucket_name), '%s' % mount_path)

# COMMAND ----------

# MAGIC %md ## generate process_timestamp

# COMMAND ----------

from datetime import datetime
process_timestamp = datetime.now().strftime('%Y%m%d %H%M%S')

# COMMAND ----------

# MAGIC %md ## get surveys that are active

# COMMAND ----------

# get surveys

import http.client
import json

conn = http.client.HTTPSConnection(hostname)
payload = ''
headers = {
  'Content-Type': 'application/json',
  'X-API-TOKEN': token
}
conn.request('GET', api_route, payload, headers)
res = conn.getresponse()
data_response = res.read()
data_response_decoded = data_response.decode('utf-8')
conn.close()
surveys = json.loads(data_response_decoded)


# COMMAND ----------

notebooks = []
for survey in surveys['result']['elements']:
  if(survey['isActive']==True):
    notebooks.append(NotebookData('./get_survey_schema',0,{'aws_bucket_name': aws_bucket_name, 'mount_name': mount_name, 'survey_id': survey['id'],'process_timestamp': process_timestamp}))    
    notebooks.append(NotebookData('./get_survey_responses',0,{'aws_bucket_name': aws_bucket_name, 'mount_name': mount_name, 'survey_id': survey['id'],'process_timestamp': process_timestamp})) 

# notebooks = [
#   NotebookData('./get_survey_schema',0,{'aws_bucket_name': aws_bucket_name, 'mount_name': mount_name, 'survey_id': 'SV_bO9FIxRtot01PXE','process_timestamp': process_timestamp}),
#   NotebookData('./get_survey_responses',0,{'aws_bucket_name': aws_bucket_name, 'mount_name': mount_name, 'survey_id': 'SV_bO9FIxRtot01PXE','process_timestamp': process_timestamp})
# ]

res = parallelNotebooks(notebooks,8)
for r in res.done:
  if r.exception() is not None:
    raise r.exception()

# COMMAND ----------

notebooks_ext = []
for survey in surveys['result']['elements']:
  if(survey['isActive']==True):
    notebooks_ext.append(NotebookData('./derived_variables_processor_using_full_json_file_s3',0,{'aws_bucket_name': aws_bucket_name, 'survey_id': survey['id'],'process_timestamp': process_timestamp}))    

# notebooks_ext = [
#   NotebookData('./derived_variables_processor_using_full_json_file_s3',0,{'aws_bucket_name': aws_bucket_name, 'survey_id': 'SV_bO9FIxRtot01PXE','process_timestamp': process_timestamp})]
    
res_ext = parallelNotebooks(notebooks_ext,8)
    
