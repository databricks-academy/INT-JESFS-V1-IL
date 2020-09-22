# Databricks notebook source
# MAGIC %md # Basic Config

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # Job Name / Course Name
# MAGIC sku = "INT-JESFS-V1-IL"
# MAGIC 
# MAGIC # The runtime you wish to test against
# MAGIC spark_version = "7.0.x-scala2.12"
# MAGIC 
# MAGIC # Which instance pool to test against (pools makes jobs a 1,000,000x faster)
# MAGIC instance_pool = "1117-113806-juice922-pool-JxljQpdx" # Amazon
# MAGIC #instance_pool = "1117-121723-show503-pool-ubXZ7gOS" # MS Azure
# MAGIC 
# MAGIC tags = sc._jvm.scala.collection.JavaConversions.mapAsJavaMap(dbutils.entry_point.getDbutils().notebook().getContext().tags())
# MAGIC home = f"""/Projects/{tags["user"]}/INT-JESFS-V1-IL/Notebooks"""
# MAGIC 
# MAGIC notebooks = [
# MAGIC   f"{home}/JES 01 - Getting Started",
# MAGIC   f"{home}/JES 02 - Values, Variables, Data Types",
# MAGIC   f"{home}/JES 03 - Conditional and Control Statements",
# MAGIC   f"{home}/JES 04 - Methods, Functions, Packages",
# MAGIC   f"{home}/JES 05 - Collections",
# MAGIC   f"{home}/JES 06 - Classes, Tuples and More",
# MAGIC   
# MAGIC   f"{home}/Optional/JES 07 - Functional Programming",
# MAGIC   f"{home}/Optional/JES 08 - String and Utility Functions",
# MAGIC   f"{home}/Optional/JES 09 - Exceptions",
# MAGIC   
# MAGIC   # Not expected to compile
# MAGIC   # f"{home}/Labs/JES 02L - Values, Variables, Data Types Lab",
# MAGIC   # f"{home}/Labs/JES 03L - Conditional and Control Statements Lab",
# MAGIC   # f"{home}/Labs/JES 04L - Methods, Functions, Packages Lab",
# MAGIC   # f"{home}/Labs/JES 05L - Collections Lab",
# MAGIC   # f"{home}/Labs/JES 06L - Classes, Tuples and More Lab",
# MAGIC   # f"{home}/Labs/JES 07L - Functional Programming Lab",
# MAGIC   # f"{home}/Labs/JES 08L - String and Utility Functions Lab",
# MAGIC   # f"{home}/Labs/JES 09L - Exceptions Lab",
# MAGIC   # f"{home}/Labs/JES 99L - Capstone",
# MAGIC 
# MAGIC   f"{home}/Solutions/JES 02S - Values, Variables, Data Types Lab",
# MAGIC   f"{home}/Solutions/JES 03S - Conditional and Control Statements Lab",
# MAGIC   f"{home}/Solutions/JES 04S - Methods, Functions, Packages Lab",
# MAGIC   f"{home}/Solutions/JES 05S - Collections Lab",
# MAGIC   f"{home}/Solutions/JES 06S - Classes, Tuples and More Lab",
# MAGIC   f"{home}/Solutions/JES 07S - Functional Programming Lab",
# MAGIC   f"{home}/Solutions/JES 08S - String and Utility Functions Lab",
# MAGIC   f"{home}/Solutions/JES 09S - Exceptions Lab",
# MAGIC   f"{home}/Solutions/JES 99S - Capstone",
# MAGIC ]

# COMMAND ----------

# MAGIC %md # REST API & Utility Functions

# COMMAND ----------

# MAGIC %run "./Test-Utilities"

# COMMAND ----------

# MAGIC %md # Remove old jobs

# COMMAND ----------

deleteJobs(sku)

# COMMAND ----------

# MAGIC %md
# MAGIC # All notebooks at once

# COMMAND ----------

resultsMap = testAllNotebooks(sku, notebooks, spark_version, instance_pool)

# COMMAND ----------

for key in resultsMap:
  waitForNotebook(key, resultsMap)

# COMMAND ----------

# MAGIC %md # Clean Up

# COMMAND ----------

deleteJobs(sku)