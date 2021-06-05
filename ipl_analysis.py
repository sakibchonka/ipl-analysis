# Databricks notebook source
# MAGIC %md ###Loading datasets into dataframes

# COMMAND ----------

ipl_matches_df = spark.read.csv("/FileStore/tables/ipl_matches.csv", header="true", inferSchema="true")
ipl_venue_df = spark.read.csv("/FileStore/tables/ipl_venue.csv", header="true", inferSchema="true")
ipl_ball_by_ball_df = spark.read.csv("/FileStore/tables/ipl_ball_by_ball.csv", header="true", inferSchema="true")

# COMMAND ----------

# MAGIC %md ### Data Wrangling

# COMMAND ----------

ipl_matches_df1 = ipl_matches_df.replace("NA",None,['player_of_match','winner','result','result_margin','eliminator'])
ipl_matches_df2 = ipl_matches_df1.dropna()

# COMMAND ----------

ipl_ball_by_ball_df1 = ipl_ball_by_ball_df.replace("NA",None,"bowling_team")
ipl_ball_by_ball_df2 = ipl_ball_by_ball_df1.dropna()

# COMMAND ----------

# MAGIC %md ###Register tables so it is accessible via SQL Context

# COMMAND ----------

ipl_matches_df2.createOrReplaceTempView("ipl_matches_table")
ipl_venue_df.createOrReplaceTempView("ipl_venue_table")
ipl_ball_by_ball_df2.createOrReplaceTempView("ipl_ball_by_ball_table")

# COMMAND ----------

# MAGIC %md ###a.Write a query to find the highest extra runs given by a team in a match.

# COMMAND ----------

df1 = ipl_ball_by_ball_df2.groupBy('match_id','bowling_team','batting_team').agg({'extra_runs':'sum'})
df2 = df1.withColumnRenamed('sum(extra_runs)','sum_of_extra_runs')
df2.orderBy(df2.sum_of_extra_runs,ascending=False).show(1)

# COMMAND ----------

# MAGIC %md ### b.Write a query to find the Leading wicket taker in the IPL?
# MAGIC Note: A wicket won't be accounted for the bowler, if the dismissal is run out,
# MAGIC obstructing the field.

# COMMAND ----------

df1 = ipl_ball_by_ball_df2.groupby('bowler').agg({'is_wicket':'sum'})
df2 = df1.withColumnRenamed('sum(is_wicket)','sum_of_wickets')
df2.select('bowler').orderBy(df2.sum_of_wickets,ascending=False).show(1)

# COMMAND ----------

sqlContext.sql('''SELECT bowler FROM (SELECT sum(is_wicket) as total_wickets,bowler 
                    FROM ipl_ball_by_ball_table 
                    WHERE `dismissal_kind` != 'run out'
                    GROUP BY bowler) GROUP BY bowler ORDER BY max(total_wickets) DESC LIMIT 1;''').show(5)

# COMMAND ----------

# MAGIC %md ###c.Write a query to return a report for highest run scorer in matches which were affected by Duckworth-Lewis’s method (D/L method).

# COMMAND ----------

#df1 has all match_id where D/L was used.
df1 = ipl_matches_df2.filter(ipl_matches_df2.method=='D/L')
#Inner join between df1 and ipl_ball_by_ball where method is D/L
df2 = df1.join(ipl_ball_by_ball_df2,df1.match_id==ipl_ball_by_ball_df2.match_id).select('batsman','total_runs')
#group by batsmen with their total runs when method used was D/L
df3 = df2.groupBy('batsman').agg({'total_runs':'sum'})
df4 = df3.withColumnRenamed('sum(total_runs)','sum_total_runs')
df4.select('batsman','sum_total_runs').orderBy(df4.sum_total_runs,ascending=False).show(1)


# COMMAND ----------

# MAGIC %md ###d.Write a query to return a report for highest strike rate by a batsman in powerplay (1-6 overs)
# MAGIC Note: strike rate = (Total Runs scored/Total balls faced by player) *100, Make sure that balls faced by player should be legal delivery 

# COMMAND ----------

#df1 is filtered for powerplay
lst = [0,1,2,3,4,5]
df1 = ipl_ball_by_ball_df2.filter(ipl_ball_by_ball_df2.overs.isin(lst))
#df2 contains all legal deliveries
df2 = df1.where(df1.extras_type=="NA")
#df3 has batsmen with total runs and total balls faced in powerplay
df3 = df1.groupBy('batsman').agg({'total_runs':'sum','ball':'sum'})
#rename columns
df4 = df3.withColumnRenamed('sum(total_runs)','sum_total_runs').withColumnRenamed('sum(ball)','sum_total_balls')
#strike rate column added
df5 = df4.withColumn("strike_rate",(df4.sum_total_runs/df4.sum_total_balls)*100)
#highest strike rate batsmen
df5.select('batsman','strike_rate').orderBy(df5.strike_rate,ascending=False).toPandas()[:5]

# COMMAND ----------

# MAGIC %md ###e.Write a query to return a report for highest extra runs in a venue (stadium, city).

# COMMAND ----------

#left join ipl_ball_by_ball and ipl_matches dataframe 
df1 = ipl_ball_by_ball_df2.join(ipl_matches_df2,ipl_ball_by_ball_df2.match_id==ipl_matches_df2.match_id,"inner")
#total extra runs in each venue
df2 = df1.groupBy('venue_id').agg({"extra_runs":"sum"})
df3 = df2.withColumnRenamed('sum(extra_runs)','total_extra_runs')
#join venue dataframe
df4 = df3.join(ipl_venue_df,df3.venue_id==ipl_venue_df.venue_id,"inner")
df4.select('total_extra_runs','venue','city').orderBy(df4.total_extra_runs,ascending = False).toPandas()[:4]


# COMMAND ----------

# MAGIC %md ###f.Write a query to return a report for the cricketers with the most number of players of the match award in neutral venues.

# COMMAND ----------

#df with all neutral venues
df1 = ipl_matches_df2.where(ipl_matches_df2.neutral_venue==0)
#group by player of the match and the number of times player of the match
df2 = df1.groupBy('player_of_match').agg({'player_of_match':'count'})
df3 = df2.withColumnRenamed('count(player_of_match)','times_player_of_match')
df4 = df3.select('player_of_match','times_player_of_match').orderBy(df3.times_player_of_match,ascending=False).show(5)

# COMMAND ----------

# MAGIC %md ###g.Write a query to get a list of top 10 players with the highest batting average
# MAGIC Note: Batting average is the total number of runs scored divided by the
# MAGIC number of times they have been out (Make sure to include run outs (on non-
# MAGIC striker end) as valid out while calculating average).

# COMMAND ----------

df1 = ipl_ball_by_ball_df2.groupBy('batsman').agg({'batsman_runs':'sum'})
df2 = df1.withColumnRenamed('sum(batsman_runs)','total_runs')

df3 = ipl_ball_by_ball_df2.select('player_dismissed','is_wicket').where(ipl_ball_by_ball_df2.is_wicket=='1')
df4 = df3.groupBy('player_dismissed').agg({'is_wicket':'sum'})
df5 = df4.withColumnRenamed('player_dismissed','batsman').withColumnRenamed('sum(is_wicket)','out')

df6 = df2.join(df5,df5.batsman==df2.batsman,'left')
df7 = df6.drop(df5.batsman)
df8 = df7.withColumn('batting_average',df7.total_runs/df7.out)
df8.select('batsman','batting_average').orderBy(df8.batting_average,ascending=False).toPandas()[:10]

# COMMAND ----------

# MAGIC %md ###h.Write a query to find out who has officiated (as an umpire) the most number of matches in IPL.

# COMMAND ----------

#union two dataframes having umpire1 and umpire2 
df1 = ipl_matches_df2.select('umpire1')
df2 = df1.withColumnRenamed('umpire1','umpires')
df3 = ipl_matches_df2.select('umpire2')
df4 = df2.union(df3)
df4.groupBy('umpires').agg({'umpires':'count'}).withColumnRenamed('count(umpires)','umpiring').orderBy('umpiring',ascending=False).show(1)

# COMMAND ----------

# MAGIC %md ###i.Find venue details of the match where V Kohli scored his highest individual runs in IPL.

# COMMAND ----------

#filter where batsman is V Kohli
df1 = ipl_ball_by_ball_df2.filter(ipl_ball_by_ball_df2.batsman=='V Kohli').select('match_id','batsman','batsman_runs')
#get runs in every match
df2 = df1.groupBy('match_id').agg({'batsman_runs':'sum'})
df3 = df2.withColumnRenamed('sum(batsman_runs)','runs')
#get match_id with highest score
df4 = df3.orderBy('runs',ascending=False).toPandas()[:1]
match_id_of_max_runs = int(df4['match_id'][0])
df5 =ipl_matches_df2.where(ipl_matches_df2.match_id==match_id_of_max_runs)
#join runs column
df6 = df5.join(df3,df5.match_id==df3.match_id)
#join with venue dataframe
df7 = df6.join(ipl_venue_df,df6.venue_id==ipl_venue_df.venue_id)
df7.select('player_of_match','runs','date','venue','city').toPandas()
