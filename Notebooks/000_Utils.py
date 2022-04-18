# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sh pip install bs4

# COMMAND ----------

#pip install lxml

# COMMAND ----------

spark.catalog.setCurrentDatabase("march_madness2_22")

df_teams_spellings = spark.table("mteamspellings")

# COMMAND ----------

import pandas as pd
import numpy as np
import requests
import json
import re

import requests

from bs4 import BeautifulSoup

# COMMAND ----------

username = "randy.rodes@lovelytics.com"
userhome = "dbfs:/user/{}".format(username)

pipelinePath = userhome + "/models/"

# COMMAND ----------

def process_total_stats(url, year_end):
  page = requests.get(url)

  soup = BeautifulSoup(page.text, 'html')

  table_data = soup.find('table')

  headers = []
  for i in table_data.find_all('th'):
      title = i.text.strip()
      headers.append(title)

  df = pd.DataFrame(columns = headers)

  for j in table_data.find_all('tr'):
          row_data = j.find_all('td')
          row = [tr.text.strip() for tr in row_data]

          if len(row)>0:
              length = len(df)
              df.loc[length] = row
              
  current_year_stats_df = spark.createDataFrame(df)
  
  for c in current_year_stats_df.columns[2:]:
    current_year_stats_df = current_year_stats_df.withColumn(c,col(c).cast("double"))
              
  current_year_stats_df_wID = (
    current_year_stats_df
    .join(df_teams_spellings, lower(col("Team"))==col("TeamNameSpelling"), "left")
    .select(
      lit(year_end).alias("Season"),
      "TeamID",
      "FGM",
      "FGA",
      (col("FGM")/col("FGA")).alias("SeasonFG%"),
      "3PM",
      "3PA",
      (col("3PM")/col("3PA")).alias("Season3P%"),
      "FTM",
      "FTA",
      (col("FTM")/col("FTA")).alias("SeasonFT%"),
      col("TOV").alias("TOSum"),
      (col("TOV")/col("GP")).alias("TOMean"),
      col("PF").alias("SeasonTotalPF"),
      (col("PF")/col("GP")).alias("SeasonPFMean"),
      col("ORB").alias("SeasonTotalORB"),
      (col("ORB")/col("GP")).alias("SeasonORBMean"),
      col("DRB").alias("SeasonTotalDRB"),
      (col("DRB")/col("GP")).alias("SeasonDRBMean"),
      #"REB",
      "AST",
      col("AST").alias("SeasonTotalAst"),
      (col("AST")/col("GP")).alias("SeasonAstMean"),
      col("STL").alias("SeasonTotalStl"),
      (col("STL")/col("GP")).alias("SeasonStlMean"),
      col("BLK").alias("SeasonTotalBlk"),
      (col("BLK")/col("GP")).alias("SeasonBlkMean"),
#       col("PTS").alias("SeasonTotalScore"),
#       (col("PTS")/col("GP")).alias("SeasonScoreMean"),

    )
    .na.drop()
    .orderBy("TeamID")

  )

  return current_year_stats_df_wID

# COMMAND ----------

def process_season(url,year_end):  
  
  page = requests.get(url)

  soup = BeautifulSoup(page.text, 'html')

  table_data = soup.find("pre").find(text=True)

  headers = "Date string, WTeam string, WScore string,LTeam string,LScore string,Location string"
  list_of_games = table_data.splitlines()

  for i in range(len(list_of_games)):
    st = list_of_games[i]
    st = re.sub("@" , "  ", st)
    list_of_games[i]=re.sub("\s{2,}" , "~", st).split("~")

  df = spark.createDataFrame(list_of_games[:-3],headers)

  season_games = (
    df.filter(col("Date").cast("date")<"2022-03-17")
    .withColumn("temp",split("LScore"," "))
    .alias("season")
    .join(df_teams_spellings.alias("team_spell1"), lower(col("season.WTeam"))==col("team_spell1.TeamNameSpelling"), "left")
    .join(df_teams_spellings.alias("team_spell2"), lower(col("season.LTeam"))==col("team_spell2.TeamNameSpelling"), "left")
    .select
    (
      lit(year_end).alias("Season"),
      lit(0).alias("DayNum"),
      col("team_spell1.TeamID").alias("WTeamID"),
      col("WScore").cast("integer"),
      col("team_spell2.TeamID").alias("LTeamID"),
      col("temp")[0].cast("integer").alias("LScore"),
      col("location").alias("WLoc"),
      lit(0).alias("NumOT")

    )
    .na.drop()

  )
  
  
  
  
  return season_games

# COMMAND ----------

def transform_season_compacted(df_regular):
  dfW=(
    df_regular.groupBy("Season","WTeamID")
    .agg(
      count("WTeamID").alias('WCount'),
      sum("WScore").alias("WScoreSum"),
      mean("WScore").alias("WScoreMean")
      )
  )
  
  dfL=(
    df_regular.groupBy("Season","LTeamID")
    .agg(
      count("LTeamID").alias('LCount'),
      sum("LScore").alias("LScoreSum"),
      mean("LScore").alias("LScoreMean")
    )
  )
  
  features_regularDF =(
    dfW.alias("W")
    .join(dfL.alias("L"),
          (col("W.Season") == col("L.Season")) 
          & (col("W.WTeamID")== col("L.LTeamID")),"left")
    .select(
      col("W.Season").alias("Season"),
      col("W.WTeamID").alias("TeamID"),
  #     col("L.Season").alias("CheckSeason"),
  #     col("L.LTeamID").alias("CheckID"),
      "WCount",
      "WScoreSum",
      "WScoreMean",
      "LCount",
      "LScoreSum",
      "LScoreMean"
      )
  #   .withColumn("check",col("Season")==col("CheckSeason"))
  #   .withColumn("check2",col("TeamID")==col("CheckID"))
    .fillna(0)
    .withColumn("SeasonTotalScore", col("WScoreSum") + col("LScoreSum"))
    .withColumn("SeasonScoreMean", col("SeasonTotalScore")/(col("WCount") +col("LCount")))
    .orderBy("Season","TeamID")
              )
  
  return features_regularDF
  

# COMMAND ----------

def transform_season_detailed(df_team_box_score):
  
  dfW_box=(
    df_team_box_score.groupBy("Season","WTeamID")
    .agg(
      count("Daynum").alias('WCount'),
      sum("WScore").alias("WScoreSum"),
      mean("WScore").alias("WScoreMean"),
      sum("WFGM").alias("WFGMSum"),
      sum("WFGA").alias("WFGASum"),
      sum("WFGM3").alias("WFGM3Sum"),
      sum("WFGA3").alias("WFGA3Sum"),
      sum("WFTM").alias("WFTMSum"),
      sum("WFTA").alias("WFTASum"),
      sum("WOR").alias("WORSum"),
      mean("WOR").alias("WORMean"),
      sum("WDR").alias("WDRSum"),
      mean("WDR").alias("WDRMean"),
      sum("WAst").alias("WAstSum"),
      mean("WAst").alias("WAstMean"),
      sum("WTO").alias("WTOSum"),
      mean("WTO").alias("WTOMean"),
      sum("WStl").alias("WStlSum"),
      mean("WStl").alias("WStlMean"),
      sum("WBlk").alias("WBlkSum"),
      mean("WBlk").alias("WBlkMean"),
      sum("WPF").alias("WPFSum"),
      mean("WPF").alias("WPFMean")
      )
  )
  
  dfL_box=(
    df_team_box_score.groupBy("Season","LTeamID")
    .agg(
      count("Daynum").alias('LCount'),
      sum("LScore").alias("LScoreSum"),
      mean("LScore").alias("LScoreMean"),
      sum("LFGM").alias("LFGMSum"),
      sum("LFGA").alias("LFGASum"),
      sum("LFGM3").alias("LFGM3Sum"),
      sum("LFGA3").alias("LFGA3Sum"),
      sum("LFTM").alias("LFTMSum"),
      sum("LFTA").alias("LFTASum"),
      sum("LOR").alias("LORSum"),
      mean("LOR").alias("LORMean"),
      sum("LDR").alias("LDRSum"),
      mean("LDR").alias("LDRMean"),
      sum("LAst").alias("LAstSum"),
      mean("LAst").alias("LAstMean"),
      sum("LTO").alias("LTOSum"),
      mean("LTO").alias("LTOMean"),
      sum("LStl").alias("LStlSum"),
      mean("LStl").alias("LStlMean"),
      sum("LBlk").alias("LBlkSum"),
      mean("LBlk").alias("LBlkMean"),
      sum("LPF").alias("LPFSum"),
      mean("LPF").alias("LPFMean")
      )
  )
  
  features_regularDF_box =(
    dfW_box.alias("W")
    .join(dfL_box.alias("L"),
          (col("W.Season") == col("L.Season")) 
          & (col("W.WTeamID")== col("L.LTeamID")),"left")
    .select(
      col("W.Season").alias("Season"),
      col("W.WTeamID").alias("TeamID"),
      "WCount",
      "WScoreSum",
      "WScoreMean",
      "WFGMSum",
      "WFGASum",
      "WFGM3Sum",
      "WFGA3Sum",
      "WFTMSum",
      "WFTASum",
      "WORSum",
      "WORMean",
      "WDRSum",
      "WDRMean",
      "WAstSum",
      "WAstMean",
      "WTOSum",
      "WTOMean",
      "WStlSum",
      "WStlMean",
      "WBlkSum",
      "WBlkMean",
      "WPFSum",
      "WPFMean",
      "LCount",
      "LScoreSum",
      "LScoreMean",
      "LFGMSum",
      "LFGASum",
      "LFGM3Sum",
      "LFGA3Sum",
      "LFTMSum",
      "LFTASum",
      "LORSum",
      "LORMean",
      "LDRSum",
      "LDRMean",
      "LAstSum",
      "LAstMean",
      "LTOSum",
      "LTOMean",
      "LStlSum",
      "LStlMean",
      "LBlkSum",
      "LBlkMean",
      "LPFSum",
      "LPFMean",
      )
    .fillna(0)
    .withColumn("SeasonTotalScore", col("WScoreSum") + col("LScoreSum"))
    .withColumn("SeasonScoreMean", col("SeasonTotalScore")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalAst", col("WAstSum") + col("LAstSum"))
    .withColumn("SeasonAstMean", col("SeasonTotalAst")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalStl", col("WStlSum") + col("LStlSum"))
    .withColumn("SeasonStlMean", col("SeasonTotalStl")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalBlk", col("WBlkSum") + col("LBlkSum"))
    .withColumn("SeasonBlkMean", col("SeasonTotalBlk")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalPF", col("WPFSum") + col("LPFSum"))
    .withColumn("SeasonPFMean", col("SeasonTotalPF")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalORB", col("WORSum") + col("LORSum"))
    .withColumn("SeasonORBMean", col("SeasonTotalORB")/(col("WCount") +col("LCount")))

    .withColumn("SeasonTotalDRB", col("WDRSum") + col("LDRSum"))
    .withColumn("SeasonDRBMean", col("SeasonTotalDRB")/(col("WCount") +col("LCount")))

    .withColumn("SeasonFG%", (col("WFGMSum") + col("LFGMSum"))/(col("WFGASum")+(col("LFGASum"))))
    .withColumn("Season3P%", (col("WFGM3Sum") + col("LFGM3Sum"))/(col("WFGA3Sum")+(col("LFGA3Sum"))))
    .withColumn("SeasonFT%", (col("WFTMSum") + col("LFTMSum"))/(col("WFTASum")+(col("LFTASum"))))
    .orderBy("Season","TeamID")
              )
  
  return features_regularDF_box

# COMMAND ----------

def transform_tourney(df_tourney):
  df = (
    df_tourney.alias("tourney")
    .withColumn("WTeamIDltLTeamID",col("WTeamID")<col("LTeamID"))
    .withColumn("TeamA",when(col("WTeamIDltLTeamID"),col("WTeamID")).otherwise(col("LTeamID")))
    .withColumn("TeamB",when(~col("WTeamIDltLTeamID"),col("WTeamID")).otherwise(col("LTeamID")))
    .withColumn("AScore",when(col("WTeamIDltLTeamID"),col("WScore")).otherwise(col("LScore")))
    .withColumn("BScore",when(~col("WTeamIDltLTeamID"),col("WScore")).otherwise(col("LScore")))
    .join(df_tourney_seeds.alias("seedsW"),
          (col("tourney.Season") == col("seedsW.Season"))
          & (col("tourney.WTeamID") == col("seedsW.TeamID")),"left")
    .join(df_tourney_seeds.alias("seedsL"),
          (col("tourney.Season") == col("seedsL.Season"))
          & (col("tourney.LTeamID") == col("seedsL.TeamID")),"left")
    .withColumn("ASeed",when(col("WTeamIDltLTeamID"),col("seedsW.Seed")).otherwise(col("seedsL.Seed")))
    .withColumn("BSeed",when(~col("WTeamIDltLTeamID"),col("seedsW.Seed")).otherwise(col("seedsL.Seed")))
    .withColumn("TeamAWon",(col("AScore")>col("BScore")).cast("int"))
    .select(
      col("tourney.Season").alias("Season"),
      col("tourney.DayNum").alias("DayNum"),
  #     col("WTeamID"),
  #     col("WScore"),
  #     col("LTeamID"),
  #     col("LScore"),
       col("WLoc"),
       col("NumOT"),
  #      col("seedsW.Seed").alias("WTeamSeed"),
  #      col("seedsL.Seed").alias("LTeamSeed"),
      "TeamA",
      "TeamB",
      "AScore",
      "BScore",
      "ASeed",
      "BSeed",
      "TeamAWon"
    )
     .na.drop()
  #   #.withColumn("ScoreGap", col("WScore") - col("LScore"))
  #   .withColumn("UpperScoreGap", col("UpperScore") - col("LowerScore"))
  #   .withColumn("UpperSeedWon",(col("WTeamSeed")<col("LTeamSeed")).cast("int"))
  #   .select(
  #     "Season",
  #     "DayNum",
  #     "UpperID",
  #     "LowerID",
  #     "UpperScore",
  #     "LowerScore",
  #     "UpperScoreGap",
  #     "WTeamSeed",
  #     "LTeamSeed",
  #     "UpperSeedWon"
  #   )


    )
  
  return df

# COMMAND ----------

def pairs(l):
    return [[l[i],l[j] ] for i in range(len(l)) for j in range(i+1,len(l))]

# COMMAND ----------

def prepare_training_set(data,cols_to_drop, scaled, cols_to_scale):
  
  X_train = data.withColumnRenamed("TeamAWon","label").drop(*cols_to_drop)
  
  if scaled:
    vector_assembler_scaled = VectorAssembler(inputCols= cols_to_scale, outputCol='features_to_scale')

    #scaler = StandardScaler(inputCol=vector_assembler_scaled.getOutputCol(), outputCol="features_scaled",withMean=True, withStd=True)
    
    scaler = MinMaxScaler(inputCol=vector_assembler_scaled.getOutputCol(), outputCol="features_scaled")

    scale_pipe=Pipeline(stages = [vector_assembler_scaled, scaler])

    X_scaled = scale_pipe.fit(X_train).transform(X_train)
    
    X_train = X_scaled.withColumn("vector_array",vector_to_array("features_scaled"))
    
    for i in range(len(cols_to_scale)):
      X_train = X_train.withColumn(cols_to_scale[i],col("vector_array")[i])
      
  
  return X_train