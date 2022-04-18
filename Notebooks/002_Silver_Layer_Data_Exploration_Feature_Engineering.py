# Databricks notebook source
# MAGIC %run ./000_Utils

# COMMAND ----------

# DBTITLE 1,2020-2021 Data
url_2021_season_stats = "https://basketball.realgm.com/ncaa/team-stats/2021/Totals/Team_Totals/0"

url_2021_season = "https://masseyratings.com/scores.php?s=cb2020&sub=ncaa-d1&all=1&sch=1"

# COMMAND ----------

spark.catalog.setCurrentDatabase("march_madness2_22")

# COMMAND ----------

df_seasons = spark.table("mseasons")
df_seasons.display()

# COMMAND ----------

df_tourney_seeds = spark.table("mncaatourneyseeds")
df_tourney_seeds.groupBy("Season").count().display()

# COMMAND ----------

df_tourney_85_19 = spark.table("mncaatourneycompactresults")
display(df_tourney_85_19)

# COMMAND ----------

# DBTITLE 1,2021 Tournament Results
sch_2021 = "Season integer,DayNum integer,WTeamID integer, WScore integer, LTeamID integer, LScore integer, WLoc string,NumOT integer"

tourney_2021 = [[2021,0,1411,60,1291,52,"0",0],
[2021,0,1179,53,1455,52,"0",0],
[2021,0,1313,54,1111,53,"0",0],
[2021,0,1417,86,1277,80,"0",0],
[2021,0,1196,75,1439,70,"0",0],
[2021,0,1116,85,1159,68,"0",0],
[2021,0,1228,78,1180,49,"0",0],
[2021,0,1403,65,1429,53,"0",0],
[2021,0,1331,75,1326,72,"0",0],
[2021,0,1124,79,1216,55,"0",0],
[2021,0,1260,71,1210,60,"0",0],
[2021,0,1333,70,1397,56,"0",0],
[2021,0,1329,69,1251,60,"0",0],
[2021,0,1458,85,1314,62,"0",0],
[2021,0,1222,87,1156,56,"0",0],
[2021,0,1317,78,1345,69,"0",0],
[2021,0,1353,60,1155,56,"0",0],
[2021,0,1393,78,1361,62,"0",0],
[2021,0,1452,84,1287,67,"0",0],
[2021,0,1437,73,1457,63,"0",0],
[2021,0,1104,68,1233,55,"0",0],
[2021,0,1242,93,1186,84,"0",0],
[2021,0,1199,64,1422,54,"0",0],
[2021,0,1166,63,1364,62,"0",0],
[2021,0,1160,96,1207,73,"0",0],
[2021,0,1261,76,1382,61,"0",0],
[2021,0,1276,82,1411,66,"0",0],
[2021,0,1425,72,1179,56,"0",0],
[2021,0,1332,1,1433,0,"0",0],
[2021,0,1234,86,1213,74,"0",0],
[2021,0,1325,62,1438,58,"0",0],
[2021,0,1268,63,1163,54,"0",0],
[2021,0,1328,72,1281,68,"0",0],
[2021,0,1211,98,1313,55,"0",0],
[2021,0,1417,73,1140,62,"0",0],
[2021,0,1101,53,1400,52,"0",0],
[2021,0,1260,71,1228,58,"0",0],
[2021,0,1124,76,1458,63,"0",0],
[2021,0,1393,75,1452,72,"0",0],
[2021,0,1116,68,1403,66,"0",0],
[2021,0,1222,63,1353,60,"0",0],
[2021,0,1331,81,1196,78,"0",0],
[2021,0,1437,84,1317,61,"0",0],
[2021,0,1333,80,1329,70,"0",0],
[2021,0,1332,95,1234,80,"0",0],
[2021,0,1211,87,1328,71,"0",0],
[2021,0,1417,67,1101,47,"0",0],
[2021,0,1166,72,1325,58,"0",0],
[2021,0,1276,86,1261,78,"0",0],
[2021,0,1199,71,1160,53,"0",0],
[2021,0,1104,96,1268,77,"0",0],
[2021,0,1425,85,1242,51,"0",0],
[2021,0,1333,65,1260,58,"0",0],
[2021,0,1124,62,1437,51,"0",0],
[2021,0,1116,72,1331,70,"0",0],
[2021,0,1222,62,1393,46,"0",0],
[2021,0,1211,83,1166,65,"0",0],
[2021,0,1276,76,1199,58,"0",0],
[2021,0,1417,88,1104,78,"0",0],
[2021,0,1425,82,1332,68,"0",0],
[2021,0,1222,67,1333,61,"0",0],
[2021,0,1124,81,1116,72,"0",0],
[2021,0,1211,85,1425,66,"0",0],
[2021,0,1417,51,1276,49,"0",0],
[2021,0,1124,78,1222,59,"0",0],
[2021,0,1211,93,1417,90,"0",0],
[2021,0,1124,86,1211,70,"0",0]]

df_2021 = spark.createDataFrame(tourney_2021, sch_2021)

# COMMAND ----------

df_tourney = df_tourney_85_19.union(df_2021)
#display(df_tourney.groupBy("Season").count())

display(df_tourney)

# COMMAND ----------

df_tourney2 = transform_tourney(df_tourney)

display(df_tourney2)

# COMMAND ----------

# df_tourney2 = (
#   df_tourney.alias("tourney")
#   .join(df_tourney_seeds.alias("seedsW"),
#         (col("tourney.Season") == col("seedsW.Season"))
#         & (col("tourney.WTeamID") == col("seedsW.TeamID")),"left")
#   .join(df_tourney_seeds.alias("seedsL"),
#         (col("tourney.Season") == col("seedsL.Season"))
#         & (col("tourney.LTeamID") == col("seedsL.TeamID")),"left")
#   .select(
#     col("tourney.Season").alias("Season"),
#     col("tourney.DayNum").alias("DayNum"),
#     col("WTeamID"),
#     col("WScore"),
#     col("LTeamID"),
#     col("LScore"),
#     col("WLoc"),
#     col("NumOT"),
#     col("seedsW.Seed").alias("WTeamSeed"),
#     col("seedsL.Seed").alias("LTeamSeed")
#   )
#   .na.drop()
#   .withColumn("UpperSeedWon",col("WTeamSeed")<col("LTeamSeed"))
#   .withColumn("UpperID",when(col("UpperSeedWon"),col("WTeamID")).otherwise(col("LTeamID")))
#   .withColumn("LowerID",when(~col("UpperSeedWon"),col("WTeamID")).otherwise(col("LTeamID")))
#   .withColumn("UpperScore",when(col("UpperSeedWon"),col("WScore")).otherwise(col("LScore")))
#   .withColumn("LowerScore",when(~col("UpperSeedWon"),col("WScore")).otherwise(col("LScore")))
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
  

#   )
             

# COMMAND ----------

df_regular85_21 = spark.table("mregularseasoncompactresults")
df_regular85_21.display() #.filter(col("Season")==2021) .groupBy("Season").count()

# COMMAND ----------

# season21DF = process_season(url_2021_season,year_end=2021)

# display(season21DF)

# COMMAND ----------

df_regular = df_regular85_21 #df_regular85_21.union(season21DF)

# COMMAND ----------

features_regularDF_temp = transform_season_compacted(df_regular)

display(features_regularDF_temp)

# COMMAND ----------

shift_season = features_regularDF_temp.withColumn("Season",col("Season")+1).select(
  "Season",
  "TeamID",
  col("WCount").alias("WCountShift"),
  col("WScoreSum").alias("WScoreSumShift"),
  col("WScoreMean").alias("WScoreMeanShift"),
  col("LCount").alias("LCountShift"),
  col("LScoreSum").alias("LScoreSumShift"),
  col("LScoreMean").alias("LScoreMeanShift"),
  col("SeasonTotalScore").alias("SeasonTotalScoreShift"),
  col("SeasonScoreMean").alias("SeasonScoreMeanShift"),
)

#display(shift_season.groupBy("Season").count())

# COMMAND ----------

shift_season.write.format("delta").mode("overwrite").saveAsTable("shift_season_compacted")

# COMMAND ----------

#display(features_regularDF.filter(~col("check")))


features_regularDF = (
  features_regularDF_temp
  .join(shift_season,["Season","TeamID"],"left")
  .fillna(0)
  .orderBy("Season","TeamID")
)

display(features_regularDF)

# COMMAND ----------

finalDF = (
  df_tourney2.alias("tourney")
  .join(features_regularDF.alias("regularA"),
        (col("tourney.Season") == col("regularA.Season"))
        & (col("tourney.TeamA") == col("regularA.TeamID")),"left")
  .join(features_regularDF.alias("regularB"),
        (col("tourney.Season") == col("regularB.Season"))
        & (col("tourney.TeamB") == col("regularB.TeamID")),"left")
  .select(
    col("tourney.TeamAWon").alias("TeamAWon"),
    col("tourney.Season").alias("Season"),
    col("tourney.DayNum").alias("DayNum"),
    col("TeamA"),
    col("AScore"),
    col("TeamB"),
    col("BScore"),
    regexp_replace(col("tourney.ASeed"),"[^0-9]","").cast("integer").alias("ASeed"),
    regexp_replace(col("tourney.BSeed"),"[^0-9]","").cast("integer").alias("BSeed"),
    
    col("regularA.WCount").alias("AWCount"),
    col("regularA.WScoreSum").alias("AWScoreSum"),
    col("regularA.WScoreMean").alias("AWScoreMean"),
    col("regularA.LCount").alias("ALCount"),
    col("regularA.LScoreSum").alias("ALScoreSum"),
    col("regularA.LScoreMean").alias("ALScoreMean"),
    col("regularA.SeasonTotalScore").alias("ASeasonTotalScore"),
    col("regularA.SeasonScoreMean").alias("ASeasonScoreMean"),
    
    col("regularA.WCountShift").alias("AWCountShift"),
    col("regularA.WScoreSumShift").alias("AWScoreSumShift"),
    col("regularA.WScoreMeanShift").alias("AWScoreMeanShift"),
    col("regularA.LCountShift").alias("ALCountShift"),
    col("regularA.LScoreSumShift").alias("ALScoreSumShift"),
    col("regularA.LScoreMeanShift").alias("ALScoreMeanShift"),
    col("regularA.SeasonTotalScoreShift").alias("ASeasonTotalScoreShift"),
    col("regularA.SeasonScoreMeanShift").alias("ASeasonScoreMeanShift"),  
    
    col("regularB.WCount").alias("BWCount"),
    col("regularB.WScoreSum").alias("BWScoreSum"),
    col("regularB.WScoreMean").alias("BWScoreMean"),
    col("regularB.LCount").alias("BLCount"),
    col("regularB.LScoreSum").alias("BLScoreSum"),
    col("regularB.LScoreMean").alias("BLScoreMean"),
    col("regularB.SeasonTotalScore").alias("BSeasonTotalScore"),
    col("regularB.SeasonScoreMean").alias("BSeasonScoreMean"),
    
    col("regularB.WCountShift").alias("BWCountShift"),
    col("regularB.WScoreSumShift").alias("BWScoreSumShift"),
    col("regularB.WScoreMeanShift").alias("BWScoreMeanShift"),
    col("regularB.LCountShift").alias("BLCountShift"),
    col("regularB.LScoreSumShift").alias("BLScoreSumShift"),
    col("regularB.LScoreMeanShift").alias("BLScoreMeanShift"),
    col("regularB.SeasonTotalScoreShift").alias("BSeasonTotalScoreShift"),
    col("regularB.SeasonScoreMeanShift").alias("BSeasonScoreMeanShift"),
    
    
    )
  .na.drop()
)

display(finalDF)

# COMMAND ----------

# finalDF = (
#   df_tourney2.alias("tourney")
#   .join(features_regularDF.alias("regularW"),
#         (col("tourney.Season") == col("regularW.Season"))
#         & (col("tourney.UpperID") == col("regularW.TeamID")),"left")
#   .join(features_regularDF.alias("regularL"),
#         (col("tourney.Season") == col("regularL.Season"))
#         & (col("tourney.LowerID") == col("regularL.TeamID")),"left")
#   .select(
#     col("tourney.Season").alias("Season"),
#     col("tourney.DayNum").alias("DayNum"),
#     col("UpperID"),
#     col("UpperScore"),
#     col("LowerID"),
#     col("LowerScore"),
#     col("tourney.WTeamSeed").alias("WTeamSeed"),
#     col("tourney.LTeamSeed").alias("LTeamSeed"),
#     col("regularW.WCount").alias("UpperWCount"),
#     col("regularW.WScoreSum").alias("UpperWScoreSum"),
#     col("regularW.WScoreMean").alias("UpperWScoreMean"),
#     col("regularW.LCount").alias("UpperLCount"),
#     col("regularW.LScoreSum").alias("UpperLScoreSum"),
#     col("regularW.LScoreMean").alias("UpperLScoreMean"),
#     col("regularW.SeasonTotalScore").alias("UpperSeasonTotalScore"),
#     col("regularW.SeasonScoreMean").alias("UpperSeasonScoreMean"),
#     col("regularL.WCount").alias("LowerWCount"),
#     col("regularL.WScoreSum").alias("LowerWScoreSum"),
#     col("regularL.WScoreMean").alias("LowerWScoreMean"),
#     col("regularL.LCount").alias("LowerLCount"),
#     col("regularL.LScoreSum").alias("LowerLScoreSum"),
#     col("regularL.LScoreMean").alias("LowerLScoreMean"),
#     col("regularL.SeasonTotalScore").alias("LowerSeasonTotalScore"),
#     col("regularL.SeasonScoreMean").alias("LowerSeasonScoreMean"),
#     col("tourney.UpperSeedWon").alias("UpperSeedWon")
#     )
#   .na.drop()
# )

# COMMAND ----------

#df_tourney_slots = spark.table("mncaatourneyslots")

#df_tourney_round_slots = spark.table("mncaatourneyseedroundslots")

# COMMAND ----------

df_team_box_score = spark.table("mregularseasondetailedresults")
display(df_team_box_score)
#display(df_team_box_score.groupBy("Season").count())

# COMMAND ----------

# season_2021_stats = process_total_stats(url_2021_season_stats, year_end = 2021)

# display(season_2021_stats)

# COMMAND ----------

# season_2021_stats_plus = (
#   features_regularDF_temp
#   .join(season_2021_stats,["Season", "TeamID"])
# )

# display(season_2021_stats_plus)

# COMMAND ----------

features_regularDF_box = transform_season_detailed(df_team_box_score)


display(features_regularDF_box)

# COMMAND ----------

features_regularDF_box.columns

# COMMAND ----------

shift_season_box = features_regularDF_box.withColumn("Season",col("Season")+1).select(
  "Season",
  "TeamID",
  col("WCount").alias("WCountShift"),
  col("WScoreSum").alias("WScoreSumShift"),
  col("WScoreMean").alias("WScoreMeanShift"),
  col("LCount").alias("LCountShift"),
  col("LScoreSum").alias("LScoreSumShift"),
  col("LScoreMean").alias("LScoreMeanShift"),
  col("SeasonTotalScore").alias("SeasonTotalScoreShift"),
  col("SeasonScoreMean").alias("SeasonScoreMeanShift"),
   
  col("SeasonTotalAst").alias('SeasonTotalAstShift'),
  col("SeasonAstMean").alias('SeasonAstMeanShift'),
  col("SeasonTotalStl").alias('SeasonTotalStlShift'),
  col("SeasonStlMean").alias('SeasonStlMeanShift'),
  col("SeasonTotalBlk").alias('SeasonTotalBlkShift'),
  col("SeasonBlkMean").alias('SeasonBlkMeanShift'),
  col("SeasonTotalPF").alias('SeasonTotalPFShift'),
  col("SeasonPFMean").alias('SeasonPFMeanShift'),
  col("SeasonTotalORB").alias('SeasonTotalORBShift'),
  col("SeasonORBMean").alias('SeasonORBMeanShift'),
  col("SeasonTotalDRB").alias('SeasonTotalDRBShift'),
  col("SeasonDRBMean").alias('SeasonDRBMeanShift'),
  col("SeasonFG%").alias('SeasonFG%Shift'),
  col("Season3P%").alias('Season3P%Shift'),
  col("SeasonFT%").alias('SeasonFT%Shift'),
  
)

# COMMAND ----------

shift_season_box.write.format("delta").mode("overwrite").saveAsTable("shift_season_detailed")

# COMMAND ----------

features_regularDF_box = (
  features_regularDF_box
  .join(shift_season_box,["Season","TeamID"],"left")
  .fillna(0)
  .orderBy("Season","TeamID")
)

# COMMAND ----------

display(features_regularDF_box)

# COMMAND ----------

features_regularDF_box.columns

# COMMAND ----------

finalDF_box = (
  df_tourney2.alias("tourney")
  .join(features_regularDF_box.alias("regularA"),
        (col("tourney.Season") == col("regularA.Season"))
        & (col("tourney.TeamA") == col("regularA.TeamID")),"left")
  .join(features_regularDF_box.alias("regularB"),
        (col("tourney.Season") == col("regularB.Season"))
        & (col("tourney.TeamB") == col("regularB.TeamID")),"left")
  .select(
    col("tourney.TeamAWon").alias("TeamAWon"),
    col("tourney.Season").alias("Season"),
    col("tourney.DayNum").alias("DayNum"),
    col("TeamA"),
    col("AScore"),
    col("TeamB"),
    col("BScore"),
    regexp_replace(col("tourney.ASeed"),"[^0-9]","").cast("integer").alias("ASeed"),
    regexp_replace(col("tourney.BSeed"),"[^0-9]","").cast("integer").alias("BSeed"),
    
    col("regularA.WCount").alias("AWCount"),
    col("regularA.WScoreSum").alias("AWScoreSum"),
    col("regularA.WScoreMean").alias("AWScoreMean"),
    
#     col("regularA.WFGMSum").alias("AWFGMSum"),
#     col("regularA.WFGASum").alias("AWFGASum"),
#     col("regularA.WFGM3Sum").alias("AWFGM3Sum"),
#     col("regularA.WFGA3Sum").alias("AWFGA3Sum"),
#     col("regularA.WFTMSum").alias("AWFTMSum"),
#     col("regularA.WFTASum").alias("AWFTASum"),
#     col("regularA.WORSum").alias("AWORSum"),
#     col("regularA.WORMean").alias("AWORMean"),
#     col("regularA.WDRSum").alias("AWDRSum"),
#     col("regularA.WDRMean").alias("AWDRMean"),
#     col("regularA.WAstSum").alias("AWAstSum"),
#     col("regularA.WAstMean").alias("AWAstMean"),
#     col("regularA.WTOSum").alias("AWTOSum"),
#     col("regularA.WTOMean").alias("AWTOMean"),
#     col("regularA.WStlSum").alias("AWStlSum"),
#     col("regularA.WStlMean").alias("AWStlMean"),
#     col("regularA.WBlkSum").alias("AWBlkSum"),
#     col("regularA.WBlkMean").alias("AWBlkMean"),
#     col("regularA.WPFSum").alias("AWPFSum"),
#     col("regularA.WPFMean").alias("AWPFMean"),
    
    
    
    
    col("regularA.LCount").alias("ALCount"),
    col("regularA.LScoreSum").alias("ALScoreSum"),
    col("regularA.LScoreMean").alias("ALScoreMean"),
    
    
#     col("regularA.LFGMSum").alias("ALFGMSum"),
#     col("regularA.LFGASum").alias("ALFGASum"),
#     col("regularA.LFGM3Sum").alias("ALFGM3Sum"),
#     col("regularA.LFGA3Sum").alias("ALFGA3Sum"),
#     col("regularA.LFTMSum").alias("ALFTMSum"),
#     col("regularA.LFTASum").alias("ALFTASum"),
#     col("regularA.LORSum").alias("ALORSum"),
#     col("regularA.LORMean").alias("ALORMean"),
#     col("regularA.LDRSum").alias("ALDRSum"),
#     col("regularA.LDRMean").alias("ALDRMean"),
#     col("regularA.LAstSum").alias("ALAstSum"),
#     col("regularA.LAstMean").alias("ALAstMean"),
#     col("regularA.LTOSum").alias("ALTOSum"),
#     col("regularA.LTOMean").alias("ALTOMean"),
#     col("regularA.LStlSum").alias("ALStlSum"),
#     col("regularA.LStlMean").alias("ALStlMean"),
#     col("regularA.LBlkSum").alias("ALBlkSum"),
#     col("regularA.LBlkMean").alias("ALBlkMean"),
#     col("regularA.LPFSum").alias("ALPFSum"),
#     col("regularA.LPFMean").alias("ALPFMean"),
    
    
    col("regularA.SeasonTotalAst").alias("ASeasonTotalAst"),
    col("regularA.SeasonAstMean").alias("ASeasonAstMean"),
    col("regularA.SeasonTotalStl").alias("ASeasonTotalStl"),
    col("regularA.SeasonStlMean").alias("ASeasonStlMean"),
    col("regularA.SeasonTotalBlk").alias("ASeasonTotalBlk"),
    col("regularA.SeasonBlkMean").alias("ASeasonBlkMean"),
    col("regularA.SeasonTotalPF").alias("ASeasonTotalPF"),
    col("regularA.SeasonPFMean").alias("ASeasonPFMean"),
    col("regularA.SeasonTotalORB").alias("ASeasonTotalORB"),
    col("regularA.SeasonORBMean").alias("ASeasonORBMean"),
    col("regularA.SeasonTotalDRB").alias("ASeasonTotalDRB"),
    col("regularA.SeasonDRBMean").alias("ASeasonDRBMean"),
    
    
    col("regularA.SeasonFG%").alias("ASeasonFG%"),
    col("regularA.Season3P%").alias("ASeason3P%"),
    col("regularA.SeasonFT%").alias("ASeasonFT%"),
    
    col("regularA.SeasonTotalScore").alias("ASeasonTotalScore"),
    col("regularA.SeasonScoreMean").alias("ASeasonScoreMean"),
    
    
    
    col("regularA.WCountShift").alias("AWCountShift"),
    col("regularA.WScoreSumShift").alias("AWScoreSumShift"),
    col("regularA.WScoreMeanShift").alias("AWScoreMeanShift"),    
    
    col("regularA.LCountShift").alias("ALCountShift"),
    col("regularA.LScoreSumShift").alias("ALScoreSumShift"),
    col("regularA.LScoreMeanShift").alias("ALScoreMeanShift"),
    
    
    col("regularA.SeasonTotalAstShift").alias("ASeasonTotalAstShift"),
    col("regularA.SeasonAstMeanShift").alias("ASeasonAstMeanShift"),
    col("regularA.SeasonTotalStlShift").alias("ASeasonTotalStlShift"),
    col("regularA.SeasonStlMeanShift").alias("ASeasonStlMeanShift"),
    col("regularA.SeasonTotalBlkShift").alias("ASeasonTotalBlkShift"),
    col("regularA.SeasonBlkMeanShift").alias("ASeasonBlkMeanShift"),
    col("regularA.SeasonTotalPFShift").alias("ASeasonTotalPFShift"),
    col("regularA.SeasonPFMeanShift").alias("ASeasonPFMeanShift"),
    col("regularA.SeasonTotalORBShift").alias("ASeasonTotalORBShift"),
    col("regularA.SeasonORBMeanShift").alias("ASeasonORBMeanShift"),
    col("regularA.SeasonTotalDRBShift").alias("ASeasonTotalDRBShift"),
    col("regularA.SeasonDRBMeanShift").alias("ASeasonDRBMeanShift"),
    
    
    col("regularA.SeasonFG%Shift").alias("ASeasonFG%Shift"),
    col("regularA.Season3P%Shift").alias("ASeason3P%Shift"),
    col("regularA.SeasonFT%Shift").alias("ASeasonFT%Shift"),
    
    col("regularA.SeasonTotalScoreShift").alias("ASeasonTotalScoreShift"),
    col("regularA.SeasonScoreMeanShift").alias("ASeasonScoreMeanShift"),
    
    
    col("regularB.WCount").alias("BWCount"),
    col("regularB.WScoreSum").alias("BWScoreSum"),
    col("regularB.WScoreMean").alias("BWScoreMean"),
    
#     col("regularB.WFGMSum").alias("BWFGMSum"),
#     col("regularB.WFGASum").alias("BWFGASum"),
#     col("regularB.WFGM3Sum").alias("BWFGM3Sum"),
#     col("regularB.WFGA3Sum").alias("BWFGA3Sum"),
#     col("regularB.WFTMSum").alias("BWFTMSum"),
#     col("regularB.WFTASum").alias("BWFTASum"),
#     col("regularB.WORSum").alias("BWORSum"),
#     col("regularB.WORMean").alias("BWORMean"),
#     col("regularB.WDRSum").alias("BWDRSum"),
#     col("regularB.WDRMean").alias("BWDRMean"),
#     col("regularB.WAstSum").alias("BWAstSum"),
#     col("regularB.WAstMean").alias("BWAstMean"),
#     col("regularB.WTOSum").alias("BWTOSum"),
#     col("regularB.WTOMean").alias("BWTOMean"),
#     col("regularB.WStlSum").alias("BWStlSum"),
#     col("regularB.WStlMean").alias("BWStlMean"),
#     col("regularB.WBlkSum").alias("BWBlkSum"),
#     col("regularB.WBlkMean").alias("BWBlkMean"),
#     col("regularB.WPFSum").alias("BWPFSum"),
#     col("regularB.WPFMean").alias("BWPFMean"),
    
    col("regularB.LCount").alias("BLCount"),
    col("regularB.LScoreSum").alias("BLScoreSum"),
    col("regularB.LScoreMean").alias("BLScoreMean"),
    
#     col("regularB.LFGMSum").alias("BLFGMSum"),
#     col("regularB.LFGASum").alias("BLFGASum"),
#     col("regularB.LFGM3Sum").alias("BLFGM3Sum"),
#     col("regularB.LFGA3Sum").alias("BLFGA3Sum"),
#     col("regularB.LFTMSum").alias("BLFTMSum"),
#     col("regularB.LFTASum").alias("BLFTASum"),
#     col("regularB.LORSum").alias("BLORSum"),
#     col("regularB.LORMean").alias("BLORMean"),
#     col("regularB.LDRSum").alias("BLDRSum"),
#     col("regularB.LDRMean").alias("BLDRMean"),
#     col("regularB.LAstSum").alias("BLAstSum"),
#     col("regularB.LAstMean").alias("BLAstMean"),
#     col("regularB.LTOSum").alias("BLTOSum"),
#     col("regularB.LTOMean").alias("BLTOMean"),
#     col("regularB.LStlSum").alias("BLStlSum"),
#     col("regularB.LStlMean").alias("BLStlMean"),
#     col("regularB.LBlkSum").alias("BLBlkSum"),
#     col("regularB.LBlkMean").alias("BLBlkMean"),
#     col("regularB.LPFSum").alias("BLPFSum"),
#     col("regularB.LPFMean").alias("BLPFMean"),
    
    col("regularB.SeasonTotalAst").alias("BSeasonTotalAst"),
    col("regularB.SeasonAstMean").alias("BSeasonAstMean"),
    col("regularB.SeasonTotalStl").alias("BSeasonTotalStl"),
    col("regularB.SeasonStlMean").alias("BSeasonStlMean"),
    col("regularB.SeasonTotalBlk").alias("BSeasonTotalBlk"),
    col("regularB.SeasonBlkMean").alias("BSeasonBlkMean"),
    col("regularB.SeasonTotalPF").alias("BSeasonTotalPF"),
    col("regularB.SeasonPFMean").alias("BSeasonPFMean"),
    col("regularB.SeasonTotalORB").alias("BSeasonTotalORB"),
    col("regularB.SeasonORBMean").alias("BSeasonORBMean"),
    col("regularB.SeasonTotalDRB").alias("BSeasonTotalDRB"),
    col("regularB.SeasonDRBMean").alias("BSeasonDRBMean"),
    
    
    col("regularB.SeasonFG%").alias("BSeasonFG%"),
    col("regularB.Season3P%").alias("BSeason3P%"),
    col("regularB.SeasonFT%").alias("BSeasonFT%"),
    
    
    col("regularB.SeasonTotalScore").alias("BSeasonTotalScore"),
    col("regularB.SeasonScoreMean").alias("BSeasonScoreMean"),
    
     col("regularB.WCountShift").alias("BWCountShift"),
    col("regularB.WScoreSumShift").alias("BWScoreSumShift"),
    col("regularB.WScoreMeanShift").alias("BWScoreMeanShift"),    
    
    col("regularB.LCountShift").alias("BLCountShift"),
    col("regularB.LScoreSumShift").alias("BLScoreSumShift"),
    col("regularB.LScoreMeanShift").alias("BLScoreMeanShift"),
    
    
    col("regularB.SeasonTotalAstShift").alias("BSeasonTotalAstShift"),
    col("regularB.SeasonAstMeanShift").alias("BSeasonAstMeanShift"),
    col("regularB.SeasonTotalStlShift").alias("BSeasonTotalStlShift"),
    col("regularB.SeasonStlMeanShift").alias("BSeasonStlMeanShift"),
    col("regularB.SeasonTotalBlkShift").alias("BSeasonTotalBlkShift"),
    col("regularB.SeasonBlkMeanShift").alias("BSeasonBlkMeanShift"),
    col("regularB.SeasonTotalPFShift").alias("BSeasonTotalPFShift"),
    col("regularB.SeasonPFMeanShift").alias("BSeasonPFMeanShift"),
    col("regularB.SeasonTotalORBShift").alias("BSeasonTotalORBShift"),
    col("regularB.SeasonORBMeanShift").alias("BSeasonORBMeanShift"),
    col("regularB.SeasonTotalDRBShift").alias("BSeasonTotalDRBShift"),
    col("regularB.SeasonDRBMeanShift").alias("BSeasonDRBMeanShift"),
    
    
    col("regularB.SeasonFG%Shift").alias("BSeasonFG%Shift"),
    col("regularB.Season3P%Shift").alias("BSeason3P%Shift"),
    col("regularB.SeasonFT%Shift").alias("BSeasonFT%Shift"),
    
    col("regularB.SeasonTotalScoreShift").alias("BSeasonTotalScoreShift"),
    col("regularB.SeasonScoreMeanShift").alias("BSeasonScoreMeanShift"),
    
    )
  .na.drop()
)

display(finalDF_box)

# COMMAND ----------

finalDF.columns

# COMMAND ----------

finalDF_box.columns

# COMMAND ----------

finalDF.write.format("delta").mode("overwrite").saveAsTable("silver_df")

# COMMAND ----------

finalDF_box.write.format("delta").mode("overwrite").saveAsTable("silverBox_df")

# COMMAND ----------

