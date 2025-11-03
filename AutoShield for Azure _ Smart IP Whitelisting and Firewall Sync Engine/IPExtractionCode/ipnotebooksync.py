#!/usr/bin/env python
# coding: utf-8

# ## ipnotebooksync
# 
# New notebook

# In[1]:


DownloadURL ="https://download.microsoft.com/download/7/1/d/71d86715-5596-4529-9b13-da13a5de5b63/ServiceTags_Public_20251020.json"


# In[2]:


import requests

# Download JSON content
response = requests.get(DownloadURL)
response.raise_for_status()  # raise error if download fails

#display(response.text)
# Save into Fabric Lakehouse Files (replace "LakehouseName" with yours)
lakehouse_path = "/lakehouse/default/Files/ServiceTags.json"

with open(lakehouse_path, "wb") as f:
    f.write(response.content)

print("File downloaded to:", lakehouse_path)


# In[3]:


from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import explode, col,udf
from pyspark.sql.types import StructType, StructField, StringType
from netaddr import IPNetwork
import ipaddress
import pandas as pd

import com.microsoft.sqlserver.jdbc.spark

f_url = "jdbc:sqlserver://<<ServerName>>.database.fabric.microsoft.com:1433;database=<<DatabaseName>>;"


# Load the JSON file
df = spark.read.option("multiline", "true").json("Files/ServiceTags.json")
#display(df)

df_exploded = df.select(explode("values").alias("service"))

df_flat = df_exploded.select(
    col("service.name").alias("ComponentName"),
    col("service.properties.region").alias("Region"),
    explode(col("service.properties.addressPrefixes")).alias("IPAddress")
)
df_ipv4 = df_flat.filter(~col("IPAddress").contains(":"))


# Python function to extract start and end IP
def cidr_to_range(cidr):
    try:
        net = IPNetwork(cidr)
        return (str(net[0]), str(net[-1]))
    except:
        return (None, None)

# Register UDF
cidr_udf = udf(cidr_to_range, StructType([
    StructField("StartIP", StringType(), True),
    StructField("EndIP", StringType(), True)
]))

# Apply UDF
df_IPRanges = df_ipv4.withColumn("Range", cidr_udf(col("IPAddress"))) \
              .select("ComponentName", "Region", "IPAddress", col("Range.StartIP"), col("Range.EndIP"))




df_IPRanges.write \
    .mode("overwrite") \
    .option("url", f_url) \
    .mssql("Stage.AzureIPRangesFlat")

print("AzureIPRangesFlat data loaded in Fabric SQL")


# Function to expand IPs
def expand_ips(start_ip, end_ip):
    start_int = int(ipaddress.IPv4Address(start_ip))
    end_int = int(ipaddress.IPv4Address(end_ip))
    return [str(ipaddress.IPv4Address(ip)) for ip in range(start_int, end_int+1)]

# Apply UDF-free approach using flatMap
expanded = (
    df_IPRanges.rdd.flatMap(
        lambda row: [
            Row(
                ComponentName=row.ComponentName,
                Region=row.Region,
                IP=ip
            )
            for ip in expand_ips(row.StartIP, row.EndIP)
        ]
    )
)

df_expanded = spark.createDataFrame(expanded)


df_expanded.write \
    .mode("overwrite") \
    .option("url", f_url) \
    .mssql("dbo.AzureIPRangesExpanded")


print("AzureIPRangesExpanded data loaded in Fabric SQL")

