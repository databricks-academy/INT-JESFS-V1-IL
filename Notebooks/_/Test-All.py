# Databricks notebook source
# DBTITLE 1,Basic Config
# MAGIC %python
# MAGIC 
# MAGIC # Job Name / Course Name
# MAGIC job_name = "Run-All-Demo"
# MAGIC 
# MAGIC # The runtime you wish to test against
# MAGIC spark_version = "7.0.x-scala2.12"
# MAGIC 
# MAGIC # Which instance pool to test against (pools makes jobs a 1,000,000x faster)
# MAGIC instance_pool = "1117-113806-juice922-pool-JxljQpdx" # Amazon
# MAGIC #instance_pool = "1117-121723-show503-pool-ubXZ7gOS" # MS Azure