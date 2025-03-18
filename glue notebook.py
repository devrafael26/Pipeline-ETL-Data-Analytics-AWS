#!/usr/bin/env python
# coding: utf-8

# In[7]:


import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

s3_path = "s3://olist-dados/olist_products_dataset.csv"
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(s3_path)
df.show(5)


# In[8]:


# Selecionando as colunas desejadas
df2 = df.select("product_id", "product_category_name")

s3_output_path = "s3://olist-dados/df2_olist_products.csv"

# Salvando no S3
df2.write.mode("overwrite").option("header", "true").csv(s3_output_path)


# In[9]:


df2.show(5)


