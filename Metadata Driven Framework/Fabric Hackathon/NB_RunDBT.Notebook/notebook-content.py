# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import notebookutils
import requests
import time
import json

token = notebookutils.credentials.getToken("https://analysis.windows.net/powerbi/api")

url = "https://wabi-uk-south-c-primary-redirect.analysis.windows.net/metadata/artifacts/ffb2c842-4681-468f-bd57-ed5d5dd1923c/jobs/DBTItem"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.post(url, headers=headers)
job_instance_id = response.json()["artifactJobInstanceId"]

print("Job Instance ID:", job_instance_id)
print(response.status_code)


jobs_url = "https://wabi-uk-south-c-primary-redirect.analysis.windows.net/metadata/artifacts/ffb2c842-4681-468f-bd57-ed5d5dd1923c/jobs"

while True:
    jobs_response = requests.get(jobs_url, headers=headers)
    jobs = jobs_response.json()

    # find current job
    current_job = next(
        (job for job in jobs if job["artifactJobInstanceId"] == job_instance_id),
        None
    )

    if current_job:
        status = current_job.get("statusString")
        print("Status:", status)

        if status in ["Completed", "Failed", "Cancelled"]:
            print("Final Job:", current_job)
            break
    else:
        print("Job not found yet...")
    time.sleep(60)


# Final handling
status = current_job.get("statusString")

# SUCCESS
if status == "Completed" and current_job.get("isSuccessful") == True:
    result = {
        "status": "Success",
        "jobId": job_instance_id,
        "startTime": current_job.get("jobStartTimeUtc"),
        "endTime": current_job.get("jobEndTimeUtc")
    }

    notebookutils.notebook.exit(json.dumps(result))


# FAILURE
else:
    error_msg = current_job.get("serviceExceptionJson")

    # Try to parse error JSON
    try:
        if error_msg:
            error_msg = json.loads(error_msg).get("ErrorMessage", error_msg)
    except:
        pass

    result = {
        "status": "Failed",
        "jobId": job_instance_id,
        "error": error_msg
    }

    print("DBT FAILED:", result)

    # Fail notebook
    raise Exception(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
