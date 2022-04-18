# Databricks notebook source
# MAGIC %run ./000_Utils

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier, MultilayerPerceptronClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import FeatureHasher, VectorAssembler, VectorIndexer,StringIndexer, OneHotEncoder, Normalizer, StandardScaler, MinMaxScaler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics

from pyspark.sql.functions import *

import matplotlib.pyplot as plt 
from pyspark.sql.types import ArrayType, DoubleType
from pyspark.ml.functions import vector_to_array

# COMMAND ----------

rdseed = 8466

# COMMAND ----------

spark.catalog.setCurrentDatabase("march_madness2_22")

# COMMAND ----------

data = spark.table("silver_df")

# COMMAND ----------

data.printSchema()

# COMMAND ----------

display(data)

# COMMAND ----------

cols_to_drop = ['DayNum', 'AScore', 'BScore'] #'Season',

X_train = data.withColumnRenamed("TeamAWon","label").drop(*cols_to_drop)

# COMMAND ----------

feature_cols = X_train.columns[1:]

# COMMAND ----------

display(X_train)

# COMMAND ----------

# cols_to_scale = ['AWCount',
#                  'AWScoreSum',
#                  'AWScoreMean',
#                  'ALCount',
#                  'ALScoreSum',
#                  'ALScoreMean',
#                  'ASeasonTotalScore',
#                  'ASeasonScoreMean',
#                  'BWCount',
#                  'BWScoreSum',
#                  'BWScoreMean',
#                  'BLCount',
#                  'BLScoreSum',
#                  'BLScoreMean',
#                  'BSeasonTotalScore',
#                  'BSeasonScoreMean']

# list_of_means = []

# for f in cols_to_scale:
#   mean_f, std_f= X_train.select(mean(f), stddev(f)).collect()[0][:]
  
#   list_of_means.append((mean_f, std_f))
  
#   X_train = X_train.withColumn(f, (col(f)-mean_f)/std_f)

# vector_assembler = VectorAssembler(inputCols= feature_cols, outputCol='features_raw')
# scaler = Normalizer(inputCol=vector_assembler.getOutputCol(), outputCol="features")
#scaler = StandardScaler(inputCol=vector_assembler.getOutputCols(), outputCol="features")

# COMMAND ----------

cols_to_scale = ["Season",
"TeamA",
"TeamB",
"ASeed",
"BSeed",
                 'AWCount',
                 'AWScoreSum',
                 'AWScoreMean',
                 'ALCount',
                 'ALScoreSum',
                 'ALScoreMean',
                 'ASeasonTotalScore',
                 'ASeasonScoreMean',
                 'AWCountShift',
                 'AWScoreSumShift',
                 'AWScoreMeanShift',
                 'ALCountShift',
                 'ALScoreSumShift',
                 'ALScoreMeanShift',
                 'ASeasonTotalScoreShift',
                 'ASeasonScoreMeanShift',
                 'BWCount',
                 'BWScoreSum',
                 'BWScoreMean',
                 'BLCount',
                 'BLScoreSum',
                 'BLScoreMean',
                 'BSeasonTotalScore',
                 'BSeasonScoreMean',
                 'BWCountShift',
                 'BWScoreSumShift',
                 'BWScoreMeanShift',
                 'BLCountShift',
                 'BLScoreSumShift',
                 'BLScoreMeanShift',
                 'BSeasonTotalScoreShift',
                 'BSeasonScoreMeanShift']



# vector_assembler_scaled = VectorAssembler(inputCols= cols_to_scale, outputCol='features_to_scale')

# std_scaler = StandardScaler(inputCol=vector_assembler_scaled.getOutputCol(), outputCol="features_scaled",withMean=True, withStd=True)

# scale_pipe=Pipeline(stages = [vector_assembler_scaled, std_scaler])

# X_scaled = scale_pipe.fit(X_train).transform(X_train)


X_scaled = prepare_training_set(data,cols_to_drop, True, cols_to_scale)

display(X_scaled)

# COMMAND ----------

# X_scaled = X_scaled.withColumn("vector_array",vector_to_array("features_scaled"))

# for i in range(len(cols_to_scale)):
#   X_scaled = X_scaled.withColumn(cols_to_scale[i],col("vector_array")[i])

# COMMAND ----------

vector_assembler = VectorAssembler(inputCols= feature_cols, outputCol='features')

# COMMAND ----------

X = prepare_training_set(data,cols_to_drop, False, cols_to_scale)

display(X)

# COMMAND ----------

lr = LogisticRegression()

pipeline_lr = Pipeline(stages=[vector_assembler,lr])

paramGrid_lr = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01, 0.001]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1]) \
    .addGrid(lr.maxIter, [50, 100]) \
    .build()

#Setting the cross validation process
crossval_lr = CrossValidator(estimator=pipeline_lr,
                          estimatorParamMaps=paramGrid_lr,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_lr = crossval_lr.fit(X)

# COMMAND ----------

cvModel_lr.avgMetrics

# COMMAND ----------

cvModel_lr.write().overwrite().save(pipelinePath +"lr_compacted")

# COMMAND ----------

rf = RandomForestClassifier()

pipeline_rf = Pipeline(stages=[vector_assembler,rf])

paramGrid_rf = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 200]) \
    .addGrid(rf.maxDepth, [3,5,10] ) \
    .build()

#.addGrid(rf.subsamplingRate, [1, 0.7, 0.5])

#Setting the cross validation process
crossval_rf = CrossValidator(estimator=pipeline_rf,
                          estimatorParamMaps=paramGrid_rf,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_rf = crossval_rf.fit(X)

# COMMAND ----------

cvModel_rf.avgMetrics

# COMMAND ----------

cvModel_rf.write().overwrite().save(pipelinePath +"rf_compacted")

# COMMAND ----------

attr=len(feature_cols)

fig, ax = plt.subplots(figsize=(10,10))
ax.barh(range(attr), cvModel_rf.bestModel.stages[-1].featureImportances.toArray())
ax.set_yticks(range(attr))
ax.set_yticklabels(feature_cols)
ax.set_xlabel('Importances')
ax.set_title('Feature importance')
plt.tight_layout()
plt.show()

# COMMAND ----------

gbt = GBTClassifier()

pipeline_gbt = Pipeline(stages=[vector_assembler,gbt])

paramGrid_gbt = ParamGridBuilder() \
    .addGrid(gbt.maxDepth, [1,2,3,5] ) \
    .addGrid(gbt.maxIter, [20,50] ) \
    .build()

#Setting the cross validation process
crossval_gbt = CrossValidator(estimator=pipeline_gbt,
                          estimatorParamMaps=paramGrid_gbt,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_gbt = crossval_gbt.fit(X)

# COMMAND ----------

cvModel_gbt.avgMetrics

# COMMAND ----------

cvModel_gbt.write().overwrite().save(pipelinePath +"gbt_compacted")

# COMMAND ----------

layer0 = [21,2]
layer1 = [21,4,2]
layer2 = [21,8,4,2]

nn = MultilayerPerceptronClassifier(seed=rdseed)

pipeline_nn = Pipeline(stages=[vector_assembler,nn])

paramGrid_nn = ParamGridBuilder() \
    .addGrid(nn.maxIter, [50] ) \
    .addGrid(nn.stepSize,[0.1, 0.01, 0.001]) \
    .build()
#.addGrid(nn.layers, [layer0,layer1,layer2]) \

#Setting the cross validation process
crossval_nn = CrossValidator(estimator=pipeline_nn,
                          estimatorParamMaps=paramGrid_nn,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
#cvModel_nn = crossval_nn.fit(X_scaled)
#cvModel_nn.avgMetrics

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ####Working with detailed stats

# COMMAND ----------

data_detailed = spark.table("silverBox_df")

# COMMAND ----------

data_detailed.columns

# COMMAND ----------

cols_to_drop = ['DayNum', 
                'AScore', 
                'BScore'] 

#'Season',

X_train_box = data_detailed.withColumnRenamed("TeamAWon","label").drop(*cols_to_drop)

# COMMAND ----------

display(X_train_box)

# COMMAND ----------

feature_cols_box = X_train_box.columns[1:]

# COMMAND ----------

X_train_box.columns

# COMMAND ----------

cols_to_scale_box = ["Season",
"TeamA",
"TeamB",
"ASeed",
"BSeed",
  'AWCount',
 'AWScoreSum',
 'AWScoreMean',
 'ALCount',
 'ALScoreSum',
 'ALScoreMean',
 'ASeasonTotalAst',
 'ASeasonAstMean',
 'ASeasonTotalStl',
 'ASeasonStlMean',
 'ASeasonTotalBlk',
 'ASeasonBlkMean',
 'ASeasonTotalPF',
 'ASeasonPFMean',
 'ASeasonTotalORB',
 'ASeasonORBMean',
 'ASeasonTotalDRB',
 'ASeasonDRBMean',
 'ASeasonTotalScore',
 'ASeasonScoreMean',
 'AWCountShift',
 'AWScoreSumShift',
 'AWScoreMeanShift',
 'ALCountShift',
 'ALScoreSumShift',
 'ALScoreMeanShift',
 'ASeasonTotalAstShift',
 'ASeasonAstMeanShift',
 'ASeasonTotalStlShift',
 'ASeasonStlMeanShift',
 'ASeasonTotalBlkShift',
 'ASeasonBlkMeanShift',
 'ASeasonTotalPFShift',
 'ASeasonPFMeanShift',
 'ASeasonTotalORBShift',
 'ASeasonORBMeanShift',
 'ASeasonTotalDRBShift',
 'ASeasonDRBMeanShift',
 'ASeasonTotalScoreShift',
 'ASeasonScoreMeanShift',
 'BWCount',
 'BWScoreSum',
 'BWScoreMean',
 'BLCount',
 'BLScoreSum',
 'BLScoreMean',
 'BSeasonTotalAst',
 'BSeasonAstMean',
 'BSeasonTotalStl',
 'BSeasonStlMean',
 'BSeasonTotalBlk',
 'BSeasonBlkMean',
 'BSeasonTotalPF',
 'BSeasonPFMean',
 'BSeasonTotalORB',
 'BSeasonORBMean',
 'BSeasonTotalDRB',
 'BSeasonDRBMean',
 'BSeasonTotalScore',
 'BSeasonScoreMean',
 'BWCountShift',
 'BWScoreSumShift',
 'BWScoreMeanShift',
 'BLCountShift',
 'BLScoreSumShift',
 'BLScoreMeanShift',
 'BSeasonTotalAstShift',
 'BSeasonAstMeanShift',
 'BSeasonTotalStlShift',
 'BSeasonStlMeanShift',
 'BSeasonTotalBlkShift',
 'BSeasonBlkMeanShift',
 'BSeasonTotalPFShift',
 'BSeasonPFMeanShift',
 'BSeasonTotalORBShift',
 'BSeasonORBMeanShift',
 'BSeasonTotalDRBShift',
 'BSeasonDRBMeanShift',
 'BSeasonTotalScoreShift',
 'BSeasonScoreMeanShift']

# vector_assembler_scaled_box = VectorAssembler(inputCols= cols_to_scale_box, outputCol='features_to_scale')

# std_scaler_box = StandardScaler(inputCol=vector_assembler_scaled_box.getOutputCol(), outputCol="features_scaled",withMean=True, withStd=True)

# scale_pipe_box=Pipeline(stages = [vector_assembler_scaled_box, std_scaler_box])

# X_scaled_box = scale_pipe_box.fit(X_train_box).transform(X_train_box)

X_detailed = prepare_training_set(data_detailed, cols_to_drop, scaled = False, cols_to_scale = cols_to_scale_box)

display(X_detailed)

# COMMAND ----------

# X_scaled_box = X_scaled_box.withColumn("vector_array",vector_to_array("features_scaled"))

# for i in range(len(cols_to_scale_box)):
#   X_scaled_box = X_scaled_box.withColumn(cols_to_scale_box[i],col("vector_array")[i])

# COMMAND ----------

#display(X_scaled_box)

# COMMAND ----------

vector_assembler_box = VectorAssembler(inputCols= feature_cols_box, outputCol='features')

# COMMAND ----------

lr_box = LogisticRegression()

pipeline_lr_box = Pipeline(stages=[vector_assembler_box,lr_box])

paramGrid_lr_box = ParamGridBuilder() \
    .addGrid(lr_box.regParam, [0.1, 0.01, 0.001]) \
    .addGrid(lr_box.elasticNetParam, [0.0, 0.5, 1.0]) \
    .addGrid(lr_box.maxIter, [50, 100]) \
    .build()

#Setting the cross validation process
crossval_lr_box = CrossValidator(estimator=pipeline_lr_box,
                          estimatorParamMaps=paramGrid_lr_box,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_lr_box = crossval_lr_box.fit(X_detailed)

# COMMAND ----------

cvModel_lr_box.avgMetrics

# COMMAND ----------

cvModel_lr_box.write().overwrite().save(pipelinePath +"lr_detailed")

# COMMAND ----------

rf_box = RandomForestClassifier()

pipeline_rf_box = Pipeline(stages=[vector_assembler_box,rf_box])

paramGrid_rf_box = ParamGridBuilder() \
    .addGrid(rf_box.numTrees, [50, 100, 200]) \
    .addGrid(rf_box.maxDepth, [3,5,10] ) \
    .build()

#.addGrid(rf.subsamplingRate, [1, 0.7, 0.5])

#Setting the cross validation process
crossval_rf_box = CrossValidator(estimator=pipeline_rf_box,
                          estimatorParamMaps=paramGrid_rf_box,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=4)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_rf_box = crossval_rf_box.fit(X_detailed)

# COMMAND ----------

cvModel_rf_box.avgMetrics

# COMMAND ----------

cvModel_rf_box.write().overwrite().save(pipelinePath +"rf_detailed")

# COMMAND ----------

attr=len(feature_cols_box)

fig, ax = plt.subplots(figsize=(20,20))
ax.barh(range(attr), cvModel_rf_box.bestModel.stages[-1].featureImportances.toArray())
ax.set_yticks(range(attr))
ax.set_yticklabels(feature_cols_box)
ax.set_xlabel('Importances')
ax.set_title('Feature importance')
plt.tight_layout()
plt.show()

# COMMAND ----------

gbt_box = GBTClassifier()

pipeline_gbt_box = Pipeline(stages=[vector_assembler_box,gbt_box])

paramGrid_gbt_box = ParamGridBuilder() \
    .addGrid(gbt_box.maxDepth, [1,2,3,5] ) \
    .addGrid(gbt_box.maxIter, [20,50] ) \
    .build()

#Setting the cross validation process
crossval_gbt_box = CrossValidator(estimator=pipeline_gbt_box,
                          estimatorParamMaps=paramGrid_gbt_box,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=4)

#Run cross-validation using the training set, and choose the best set of parameters.
cvModel_gbt_box = crossval_gbt_box.fit(X_detailed)

# COMMAND ----------

cvModel_gbt_box.avgMetrics

# COMMAND ----------

cvModel_gbt_box.write().overwrite().save(pipelinePath +"gbt_detailed")

# COMMAND ----------

layer0 = [21,2]
layer1 = [21,4,2]
layer2 = [21,8,4,2]

nn_box = MultilayerPerceptronClassifier(seed=rdseed)

pipeline_nn_box = Pipeline(stages=[vector_assembler_box,nn_box])

paramGrid_nn_box = ParamGridBuilder() \
    .addGrid(nn_box.maxIter, [50 ,100, 200] ) \
    .addGrid(nn_box.stepSize,[0.1, 0.01, 0.001]) \
    .addGrid(nn_box.layers, [layer0,layer1,layer2]) \
    .build()


#Setting the cross validation process
crossval_nn_box = CrossValidator(estimator=pipeline_nn_box,
                          estimatorParamMaps=paramGrid_nn_box,
                          evaluator=BinaryClassificationEvaluator(metricName="areaUnderROC"),
                          numFolds=10,
                         parallelism=2)

#Run cross-validation using the training set, and choose the best set of parameters.
#cvModel_nn_box = crossval_nn_box.fit(X_scaled_box)
#cvModel_nn_box.avgMetrics