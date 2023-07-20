#!/bin/bash

PYTHON_ENV=$1
DBURL=$2
DBTOKEN=$3
WORKSPACE=$4
WORKSPACEPATH=$5
ENV=$6

source ${PYTHON_ENV}/bin/activate

pip install urllib3==1.26.15
pip install requests==2.29.0
pip install databricks-cli

databricks -v 

echo "${DBURL}
${DBTOKEN}" | databricks configure --token

databricks workspace import_dir ${WORKSPACE}/Workspace ${WORKSPACEPATH} -o

arquivos=$(ls -1 ${WORKSPACE}/jobs/${ENV})
for arquivo in $arquivos
do
  name_file="${arquivo/.json/}"  

  echo 'Nome JOB => ' $name_file

  job_id=$(databricks jobs list --output json | jq -r --arg name_file $name_file '.jobs[] | select(.settings.name == $name_file) | .job_id')

  echo 'JOB ID=> ' $job_id

  if [ ! "$job_id" ]; then
    echo 'create'
    databricks jobs create --json-file ${WORKSPACE}/jobs/$ENV/$arquivo
  else 
    echo 'reset'
    databricks jobs reset --job-id $job_id --json-file ${WORKSPACE}/jobs/${ENV}/$arquivo
  fi

done