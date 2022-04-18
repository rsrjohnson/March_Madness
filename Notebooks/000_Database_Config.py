# Databricks notebook source
#database name
database_name = "march_madness2_22"

# COMMAND ----------

spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Creating Database

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))