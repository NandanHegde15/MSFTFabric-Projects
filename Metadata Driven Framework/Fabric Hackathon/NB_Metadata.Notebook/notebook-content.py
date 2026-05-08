# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "82c3b6a5-efdf-816b-44e5-a4e268b53355",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     },
# META     "warehouse": {
# META       "default_warehouse": "b15fca6d-23eb-8c6c-4cb3-587a54d31e4b",
# META       "known_warehouses": [
# META         {
# META           "id": "b15fca6d-23eb-8c6c-4cb3-587a54d31e4b",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

schema_name="dbo"
table_name = "SQLLineOfBusiness"  
prompt = "For the SQLLineOfBusiness table, explain the business meaning of the following columns. Provide a concise description in STRICT JSON format with no markdown (max 2 lines per column): [Id],[Name],[Parent_Id],[ShortName],[Order],[DWBusinessKeyHash],[DWAttributeHash],[DWVersionNumber],[DWIsDeleteFl],[DWUpdateTs]"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#%pip install openai

from openai import AzureOpenAI

# --- Azure OpenAI Config ---
endpoint = "https://datamvpazureopenai.openai.azure.com/"   # base endpoint
deployment_name = "gpt-4o-Hackathon"  # your deployment name

kvname = "uksdatasharkxkv"
kvsecretname = "AzureOpenAIAPIKey"
kvurl = f"https://{kvname}.vault.azure.net/" 
api_key = notebookutils.credentials.getSecret(kvurl,kvsecretname)


# --- Initialize Client ---
client = AzureOpenAI(
    azure_endpoint=endpoint,
    api_key=api_key,
    api_version="2025-01-01-preview"
)

# --- Call API ---
response = client.chat.completions.create(
    model=deployment_name,
    messages=[
        {"role": "system", "content": "You are a Data Governance assistant making meta data decisions."},
        {"role": "user", "content": prompt}
    ],
    temperature=0.2
)

# --- Output ---
msg_body = response.choices[0].message.content
print(msg_body)

from pyspark.sql.functions import current_date



import json
data = json.loads(msg_body)


rows = [(schema_name,table_name, col, desc) for col, desc in data.items()]

df = spark.createDataFrame(rows, ["SchemaName","TableName", "ColumnName", "Description"])
df = df.withColumn("UpdateTs", current_date())
display(df)

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants

df.write.mode("append").synapsesql("HackathonWH.MetaData.Dictionary")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
