# Databricks notebook source
#pip install databricks_api

# COMMAND ----------

import requests
import json
from databricks_api import DatabricksAPI
import sys
import ast

def databricks_post_request(API_Databricks_arg1="['customer']"):

    token = 'dapi6ece060aa5520b2a7d3a86c3dd479941-3'
    db = DatabricksAPI(
        host="https://adb-2857568921143049.9.azuredatabricks.net/",
        token=token
    )
    job_payload = {
        "run_name": 'just_a_run',
        "existing_cluster_id": '0423-212957-vl2qhpwd',
        "notebook_task":
            {
                "notebook_path": '/Shared/MetaDatarepliaction_Backend_Code/Modular_Replication_Code',
                "base_parameters": {"list1": API_Databricks_arg1}

            }
    }
    resp = requests.post('https://adb-2857568921143049.9.azuredatabricks.net/api/2.0/jobs/runs/submit',
                         json=job_payload, headers={'Authorization': 'Bearer dapi6ece060aa5520b2a7d3a86c3dd479941-3'})
    run_id = int(resp.text[10:-1])
    final_result = db.jobs.get_run_output(run_id=run_id)
    print(final_result, job_payload)



if __name__=="__main__":
    API_Databricks_arg1 = ast.literal_eval(sys.argv[1])
    databricks_post_request(str(API_Databricks_arg1))









