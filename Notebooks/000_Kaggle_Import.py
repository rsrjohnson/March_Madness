# Databricks notebook source
# MAGIC %sh pip install kaggle

# COMMAND ----------

# MAGIC %sh
# MAGIC export KAGGLE_USERNAME=rsrjohnson
# MAGIC export KAGGLE_KEY=892730714cf7b63ef0fe91dd1157888b
# MAGIC kaggle competitions download -c ncaam-march-mania-2021

# COMMAND ----------

display(dbutils.fs.ls("file:/databricks/driver/"))

# COMMAND ----------

import os, zipfile

dir_name = '/databricks/driver'
extension = ".zip"

os.chdir(dir_name) # change directory from working dir to dir with files

for item in os.listdir(dir_name): # loop through items in dir
    if item.endswith(extension): # check for ".zip" extension
        file_name = os.path.abspath(item) # get full path of files
        zip_ref = zipfile.ZipFile(file_name) # create zipfile object
        zip_ref.extractall(dir_name) # extract file to dir
        zip_ref.close() # close file
        os.remove(file_name) # delete zipped file

# COMMAND ----------

display(dbutils.fs.ls("file:/databricks/driver/"))

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/MDataFiles_Stage1/","/FileStore/kaggle/MDataFiles_Stage1",True)

# COMMAND ----------

dbutils.fs.mv("file:/databricks/driver/MDataFiles_Stage2/","/FileStore/kaggle/MDataFiles_Stage2",True)

# COMMAND ----------

