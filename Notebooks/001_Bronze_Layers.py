# Databricks notebook source
#base files path

base_path1 = "/FileStore/kaggle/MDataFiles_Stage1/"
base_path2 = "/FileStore/kaggle/MDataFiles_Stage2/"

# COMMAND ----------

#database name
database_name = "march_madness2_22"

# COMMAND ----------

spark.catalog.setCurrentDatabase(database_name)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Extracting Paths and FileNames

# COMMAND ----------

paths_filenames =[(p[0],p[1][:-4]) for p in dbutils.fs.ls(base_path2)]

# COMMAND ----------

for tups in paths_filenames:
  df= (
  spark.read
  .option("header",True)
  .option("inferSchema",True)
  .csv(tups[0])
  )
  
  df.write.format("delta").mode("overwrite").saveAsTable(tups[1])