# Databricks notebook source
# MAGIC %run ./000_Utils

# COMMAND ----------

spark.catalog.setCurrentDatabase("march_madness2_22")

# COMMAND ----------

# DBTITLE 1,Team Spellings (use to Join IDs)
df_teams_spellings = spark.table("mteamspellings")

display(df_teams_spellings)

# COMMAND ----------

# DBTITLE 1,Importing Current Season Total Stats
url_2122_stats = "https://basketball.realgm.com/ncaa/team-stats/2022/Totals/Team_Totals/0"

current_year_stats_df_wID = process_total_stats(url_2122_stats,year_end = 2022)

display(current_year_stats_df_wID)

# COMMAND ----------

# DBTITLE 1,Current Season Games
url_2122 = "https://masseyratings.com/scores.php?s=379387&sub=11590&all=1&mode=3&sch=on&format=0"

current_year_season_games = process_season(url_2122, year_end = 2022)

display(current_year_season_games)

# COMMAND ----------

# DBTITLE 1,Final 68 Teams
# url = "https://www.espn.com/espn/feature/story/_/page/bracketology/ncaa-bracketology-projecting-2022-march-madness-men-field"
# page = requests.get(url)

# soup = BeautifulSoup(page.text, 'html')

# table_data = soup.findAll('li', {'class' :'bracket__item'})

# headers = "Seed string, Team string"

# teams_list = [i.text for i in table_data]

# teams_parsed = []

# for item in teams_list[8:]:
#   teams_parsed.append(item.split(" ",1))

                     
                      
# df_raw = spark.createDataFrame(teams_parsed, headers)

# to_replace=['Nicholls - aq/ Alcorn St. - aq', 
#             'Xavier/ Wyoming',
#             '-  UNC Wilmingtonaq -  Bryant/','Rutgers SMU/',
#             '-  Wright St.aq -  Bryant/', 
#             'Wake Forest/ Xavier',
#             'Wyoming SMU/',
#            'Wyoming/ Wake Forest',
#            'Xavier Indiana/',
#            'TSU - aq/ Texas A&M CC - aq',
#            'Michigan Texas A&M/',
#            'Rutgers/ Notre Dame']

# replacement = ["Nicholls~Alcorn St", 
#                "Xavier~Wyoming", 
#                "UNC Wilmington~Bryant", 
#                "Rutgers~SMU", 
#                "Wright St~Bryant", 
#                "Wake Forest~Xavier", 
#                "Wyoming~SMU",
#               "Wyoming~Wake Forest",
#               "Xavier~Indiana",
#               "TSU~Texas A&M CC",
#               "Michigan~Texas A&M",
#               "Rutgers~Notre Dame"]

# to_rename = ["South Dakota St", "Miami","Loyola Chicago", "Saint Mary's","TSU","CSU Fullerton"]
# renamed = ["South Dakota", "Miami FL","Loyola-Chicago", "St Mary's CA", "Texas Southern", "CS Fullerton"]

# final_teams=(
#   df_raw
#   .replace(to_replace, replacement, "Team")
#   .withColumn("aq",
#               when(col("Team").contains("aq"),1)
#               .when(col("Seed").contains("aq"),1)
#               .otherwise(0)
#              )
#   .withColumn("Team",regexp_replace("Team"," - aq",""))
#   .withColumn("Team",regexp_replace("Team","- ",""))
#   .withColumn("Seed",regexp_replace("Seed","aq","").cast("integer"))
#   .withColumn("aq_not_parsed",col("Team").contains("aq"))
#   .withColumn("Team",trim(regexp_replace("Team","aq","")))
#   .withColumn("Team",explode(split("Team","~")))
#   .withColumn("Team",regexp_replace("Team","State","St"))
#   .withColumn("Team",regexp_replace("Team","St.","St"))
#   .replace(to_rename, renamed, "Team")
# )



# display(final_teams)

# COMMAND ----------

#df_raw.filter(col("Team").contains("/")).collect()

# COMMAND ----------

# df_current_id = (
#   final_teams
#   .join(df_teams_spellings, lower(col("Team"))==col("TeamNameSpelling"), "left")
# )

# display(df_current_id)

# COMMAND ----------

#team, aq

current_year_temp =[
  ("Gonzaga", 1),
  ("UNCW",11),
  ("New Orleans" , 11),
  ("Colorado St", 0),
  ("Notre Dame",0),
  ("Alabama", 0),
  ("North Texas", 1),
  ("UCLA", 0),
  ("South Dakota", 1), #orig South Dakota St
  ("USC",0),
  ("Creighton",0),
  ("Wisconsin",0),
  ("New Mexico St", 1),
  ("Boise St",1),
  ("North Carolina",0),
  ("Texas Tech",0),
  ("Long Beach St",1),
  ("Auburn",1),
  ("Norfolk St",11),
  ("Texas Southern",11),
  ("Iowa St",0), #State to St
  ("Wake Forest",0),
  ("Ohio St",0), #State to St
  ("Iona",1),
  ("Providence",1),
  ("Vermont",1),
  ("Arkansas",0),
  ("Michigan",10),
  ("Memphis", 10),
  ("Duke",1),
  ("Princeton",1),
  ("Marquette",0),
  ("TCU",0),
  ("Purdue" ,1),
  ("Colgate" ,1),
  ("Arizona",1),
  ("Longwood",1),
  ("Seton Hall",0),
  ("Wyoming",0),
  ("Texas",0),
  ("Davidson",1),
  ("Tennessee",0),
  ("Chattanooga",1),
  ("LSU",0),
  ("Loyola-Chicago",1), #orig Loyola Chicago
  ("Illinois",0),
  ("Texas St",1),
  ("Xavier",0),
  ("Rutgers",0),
  ("Baylor",0),
  ("Montana St",1),
  ("Kansas",1),
  ("Cleveland St",1),
  ("St Mary's CA",0), #orig Saint Mary's
  ("Murray St",1),
  ("UConn",0),
  ("Indiana",10),
  ("San Diego St",10),
  ("Houston",1),
  ("Toledo",1),
  ("Michigan St",0),
  ("San Francisco",0),
  ("Villanova",0),
  ("Wagner",1),
  ("Iowa",0),
  ("Miami FL",0), #FL or OH? orig only Miami
  ("Kentucky",0),
  ("Jacksonville St",1)
]

current_year = [(1,"Gonzaga"),
(16,"Georgia St"),
(8,"Boise St"),
(9,"Memphis"),
(5,"UConn"),
(12,"New Mexico St"),
(4,"Arkansas"),
(13,"Vermont"),
(6,"Alabama"),
(11,"Notre Dame"),
(3,"Texas Tech"),
(14,"Montana St"),
(7,"Michigan St"),
(10,"Davidson"),
(2,"Duke"),
(15,"CS Fullerton"),
(1,"Arizona"),
(16,"Wright St"),
(8,"Seton Hall"),
(9,"TCU"),
(5,"Houston"),
(12,"UAB"),
(4,"Illinois"),
(13,"Chattanooga"),
(6,"Colorado St"),
(11,"Michigan"),
(3,"Tennessee"),
(14,"Longwood"),
(7,"Ohio St"),
(10,"Loyola-Chicago"),
(2,"Villanova"),
(15,"Delaware"),
(1,"Baylor"),
(16,"Norfolk St"),
(8,"North Carolina"),
(9,"Marquette"),
(5,"Saint Mary's"),
(12,"Indiana"),
(4,"UCLA"),
(13,"Akron"),
(6,"Texas"),
(11,"Virginia Tech"),
(3,"Purdue"),
(14,"Yale"),
(7,"Murray St"),
(10,"San Francisco"),
(2,"Kentucky"),
(15,"Saint Peter's"),
(1,"Kansas"),
(16,"Texas Southern"),
(8,"San Diego St"),
(9,"Creighton"),
(5,"Iowa"),
(12,"Richmond"),
(4,"Providence"),
(13,"South Dakota St."),
(6,"LSU"),
(11,"Iowa St"),
(3,"Wisconsin"),
(14,"Colgate"),
(7,"USC"),
(10,"Miami FL"),
(2,"Auburn"),
(15,"Jacksonville")]


final_teams = spark.createDataFrame(current_year, "Seed integer, Team string")

display(final_teams)

# df_current_id = (
#   df_current
#   .join(df_teams_id, "TeamName", "left")
# )

# display(df_current_id)

#aq codes 1 - aq, 11 - aq last 4, 0 not aq, 10 not aq last 4

#http://www.sevenovertimes.com/

# COMMAND ----------

df_current_id = (
  final_teams
  .join(df_teams_spellings, lower(col("Team"))==col("TeamNameSpelling"), "left")
)

display(df_current_id)

# COMMAND ----------

list_ids = list(df_current_id.select('TeamID').orderBy('TeamID').toPandas()['TeamID'])

# COMMAND ----------

list_pairs = pairs(list_ids)

# COMMAND ----------

df_test =(
  spark.createDataFrame(list_pairs,"TeamA integer, TeamB integer").alias("temp")
  .join(df_current_id.alias("A"),col("temp.TeamA")==col("A.TeamID"),"left")
  .join(df_current_id.alias("B"),col("temp.TeamB")==col("B.TeamID"),"left")
  .select(
    lit(2022).alias("Season"),
    "TeamA",
    "TeamB",
    col("A.Seed").alias("ASeed"),
    col("B.Seed").alias("BSeed"),
    col("A.Team").alias("TeamAName"),
    col("B.Team").alias("TeamBName")
    
  )
  .orderBy("TeamAName")
)

# COMMAND ----------

display(df_test)

# COMMAND ----------

df_test.select("TeamA",
    "TeamB",
    "TeamAName",
    "TeamBName").write.format("delta").mode("overwrite").saveAsTable("matchups2022")

# COMMAND ----------

# features_regularDF_current =(
#   dfW_current.alias("W")
#   .join(dfL_current.alias("L"),
#         col("W.WTeamID")== col("L.LTeamID"),"left")
#   .select(
#     col("W.WTeamID").alias("TeamID"),
# #     col("L.Season").alias("CheckSeason"),
# #     col("L.LTeamID").alias("CheckID"),
#     "WCount",
#     "WScoreSum",
#     "WScoreMean",
#     "LCount",
#     "LScoreSum",
#     "LScoreMean"
#     )
# #   .withColumn("check",col("Season")==col("CheckSeason"))
# #   .withColumn("check2",col("TeamID")==col("CheckID"))
#   .na.drop()
#   .withColumn("SeasonTotalScore", col("WScoreSum") + col("LScoreSum"))
#   .withColumn("SeasonScoreMean", col("SeasonTotalScore")/(col("WCount") +col("LCount")))
#   .orderBy("TeamID")
#             )

# display(features_regularDF_current)

# COMMAND ----------

features_regularDF_current_temp=transform_season_compacted(current_year_season_games)

#display(features_regularDF_current_temp)

# COMMAND ----------

shift_season = spark.table("shift_season_compacted")

# COMMAND ----------

display(features_regularDF_current_temp)

# COMMAND ----------

features_regularDF_current = (
  features_regularDF_current_temp
  .join(shift_season,["Season","TeamID"],"left")
  .fillna(0)
  .orderBy("Season","TeamID")
)

display(features_regularDF_current)

# COMMAND ----------

finalDF_test = (
  df_test.alias("tourney")
  .join(features_regularDF_current.alias("regularA"),
        col("tourney.TeamA") == col("regularA.TeamID"),"left")
  .join(features_regularDF_current.alias("regularB"),
        col("tourney.TeamB") == col("regularB.TeamID"),"left")
  .select(
    "tourney.Season",
    col("TeamA"),
    col("TeamB"),
    "ASeed",
    "BSeed",
    
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

display(finalDF_test)

# COMMAND ----------

finalDF_test.columns

# COMMAND ----------

finalDF_test.write.format("delta").mode("overwrite").saveAsTable("test_set")

# COMMAND ----------

display(features_regularDF_current_temp)

# COMMAND ----------

shift_season_box = spark.table("shift_season_detailed")

display(shift_season_box.filter(col("Season")==2022))

# COMMAND ----------

current_year_stats_df_wID_plus = (
  features_regularDF_current_temp
  .join(current_year_stats_df_wID,["Season","TeamID"],"left")
  .join(shift_season_box,["Season","TeamID"],"left")
  .fillna(0)
  .orderBy("Season","TeamID")
)

display(current_year_stats_df_wID_plus)

# COMMAND ----------

finalDF_test_box = (
  df_test.alias("tourney")
  .join(current_year_stats_df_wID_plus.alias("regularA"),
        col("tourney.TeamA") == col("regularA.TeamID"),"left")
  .join(current_year_stats_df_wID_plus.alias("regularB"),
        col("tourney.TeamB") == col("regularB.TeamID"),"left")
  .select(
    "tourney.Season",
    "TeamA",
    "TeamB",
    "ASeed",
    "BSeed",
    
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






display(finalDF_test_box)

# COMMAND ----------

finalDF_test_box.columns

# COMMAND ----------

finalDF_test_box.write.format("delta").mode("overwrite").saveAsTable("test_set_box")