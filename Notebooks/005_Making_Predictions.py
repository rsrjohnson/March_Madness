# Databricks notebook source
# MAGIC %run ./000_Utils

# COMMAND ----------

spark.catalog.setCurrentDatabase("march_madness2_22")

# COMMAND ----------

from pyspark.ml import PipelineModel 

lr_compacted_best = PipelineModel.load(pipelinePath+"lr_compacted/bestModel")

rf_compacted_best = PipelineModel.load(pipelinePath+"rf_compacted/bestModel")

gbt_compacted_best = PipelineModel.load(pipelinePath+"gbt_compacted/bestModel")

lr_detailed_best = PipelineModel.load(pipelinePath+"lr_detailed/bestModel")

rf_detailed_best = PipelineModel.load(pipelinePath+"rf_detailed/bestModel")

gbt_detailed_best = PipelineModel.load(pipelinePath+"gbt_detailed/bestModel")

# COMMAND ----------

df_test_compacted = spark.table("test_set")

df_test_detailed = spark.table("test_set_box")

matches = spark.table("matchups2022")

# COMMAND ----------

pred_lr_compacted = lr_compacted_best.transform(df_test_compacted)

pred_rf_compacted = rf_compacted_best.transform(df_test_compacted)

pred_gbt_compacted = gbt_compacted_best.transform(df_test_compacted)

pred_lr_detailed = lr_detailed_best.transform(df_test_detailed)

pred_rf_detailed = rf_detailed_best.transform(df_test_detailed)

pred_gbt_detailed = gbt_detailed_best.transform(df_test_detailed)

# COMMAND ----------

(
  pred_lr_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("lr_prob"),
    col("prediction").alias("lr_pred")
  )
  .display()
)

# COMMAND ----------

(
  pred_rf_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("rf_prob"),
    col("prediction").alias("rf_pred")
  )
  .display()
)

# COMMAND ----------

(
  pred_gbt_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("gbt_prob"),
    col("prediction").alias("gbt_pred")
  )
  .display()
)

# COMMAND ----------

(
  pred_lr_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("lr_detailed_prob"),
    col("prediction").alias("lr_detailed_pred")
  )
  .display()
)

# COMMAND ----------

(
  pred_rf_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("rf_detailed_prob"),
    col("prediction").alias("rf_detailed_pred")
  )
  .display()
)

# COMMAND ----------

(
  pred_gbt_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("gbt_detailed_prob"),
    col("prediction").alias("gbt_detailed_pred")
  )
  .display()
)

# COMMAND ----------

join_df_test = (
  pred_lr_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("lr_prob"),
    col("prediction").alias("lr_pred")
  )
  .join(
    pred_rf_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("rf_prob"),
    col("prediction").alias("rf_pred")
  ),["TeamA",
    "TeamB"], "inner"
    )
  .join(
    pred_gbt_compacted
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("gbt_prob"),
    col("prediction").alias("gbt_pred")
  ),["TeamA",
    "TeamB"], "inner"
    )
  .join(
    pred_lr_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("lr_detailed_prob"),
    col("prediction").alias("lr_detailed_pred")
  ),["TeamA",
    "TeamB"], "inner"
    )
  .join(
    pred_rf_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("rf_detailed_prob"),
    col("prediction").alias("rf_detailed_pred")
  ),["TeamA",
    "TeamB"], "inner"
    )
  .join(
    pred_gbt_detailed
  .select(
    "TeamA",
    "TeamB",
    col("probability").alias("gbt_detailed_prob"),
    col("prediction").alias("gbt_detailed_pred")
  ),["TeamA",
    "TeamB"], "inner"
    )
  
  
)

# COMMAND ----------

display(join_df_test)

# COMMAND ----------

def count_pred(df):
  
  df=df.withColumn("AwonCompacted",lit(0)).withColumn("BwonCompacted",lit(0))
  df=df.withColumn("AwonDetailed",lit(0)).withColumn("BwonDetailed",lit(0))
  df=df.withColumn("Awon",lit(0)).withColumn("Bwon",lit(0))
  
  cols=["lr_pred",
        "rf_pred",
        "gbt_pred",
        "lr_detailed_pred",
        "rf_detailed_pred",
        "gbt_detailed_pred"]
  
  for c in cols[:3]:
    df =(
      df.withColumn("AwonCompacted",
                       when(col(c)==1,col("AwonCompacted")+1)
                       .otherwise(col("AwonCompacted")))
      .withColumn("BwonCompacted",
                  when(col(c)==0,col("BwonCompacted")+1)
                  .otherwise(col("BwonCompacted")))
    )
  
  for c in cols[3:]:
    df =(
      df.withColumn("AwonDetailed",
                       when(col(c)==1,col("AwonDetailed")+1)
                       .otherwise(col("AwonDetailed")))
      .withColumn("BwonDetailed",
                  when(col(c)==0,col("BwonDetailed")+1)
                  .otherwise(col("BwonDetailed")))
    )
    
    
  
  
  for c in cols:
    df = df.withColumn("Awon",when(col(c)==1,col("Awon")+1).otherwise(col("Awon"))).withColumn("Bwon",when(col(c)==0,col("Bwon")+1).otherwise(col("Bwon")))
    
  df = (
    df
    .withColumn("WinningTeamCompacted",
                when(col("AwonCompacted")>col("BwonCompacted"),col("TeamA"))
                .when(col("AwonCompacted")<col("BwonCompacted"),col("TeamB"))
                .otherwise(-1))
    .withColumn("WinningTeamDetailed",
                when(col("AwonDetailed")>col("BwonDetailed"),col("TeamA"))
                .when(col("AwonDetailed")<col("BwonDetailed"),col("TeamB"))
                .otherwise(-1))
    .withColumn("WinningTeamCombined",
                when(col("Awon")>col("Bwon"),col("TeamA"))
                .when(col("Awon")<col("Bwon"),col("TeamB"))
                .otherwise(-1))
  )
    
  return df

# COMMAND ----------

df_decision = count_pred(join_df_test)

display(df_decision)

# COMMAND ----------

(
  df_decision
  .join(matches,["TeamA","TeamB"])
  .select("TeamA",
          "TeamB",
          "TeamAName",
          "TeamBName",
          "WinningTeamCompacted",
          "WinningTeamDetailed",
          "WinningTeamCombined",
          when(col("WinningTeamCompacted")==col("TeamA"),col("TeamAName"))
          .when(col("WinningTeamCompacted")==col("TeamB"),col("TeamBName"))
          .otherwise("Tie").alias("WinningCompactedName"),
           when(col("WinningTeamDetailed")==col("TeamA"),col("TeamAName"))
          .when(col("WinningTeamDetailed")==col("TeamB"),col("TeamBName"))
          .otherwise("Tie").alias("WinningDetailedName"),
           when(col("WinningTeamCombined")==col("TeamA"),col("TeamAName"))
          .when(col("WinningTeamCombined")==col("TeamB"),col("TeamBName"))
          .otherwise("Tie").alias("WinningCombined")
          
         )
  
).display()

# COMMAND ----------

