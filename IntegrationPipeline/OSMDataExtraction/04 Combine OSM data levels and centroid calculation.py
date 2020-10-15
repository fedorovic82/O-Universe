# Databricks notebook source
# MAGIC %md ### Loading data, variables and dependencies

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql import types as t
import re

# COMMAND ----------

class CombineOSMDataLevels(object):
  def __init__(self, country, countryCode):
    self.country = country
    self.countryMod = re.sub("-", "_", country)
    self.countryCode = countryCode
    
  def __load_data(self):   
    dfN = spark.table('dev_sources_osm.raw_' + self.countryMod + "_node")     # Points on the map
    dfW = spark.table('dev_sources_osm.raw_' + self.countryMod + "_way")      # Ordered list of nodes
    dfR = spark.table('dev_sources_osm.raw_' + self.countryMod + "_relation") # Ordered list containing either nodes, ways or even other relations.
    return dfN, dfW, dfR
    
  def __expand_way(self, dfW):
    dfWExp = dfW \
      .select(
        'id', 
        'version', 
        f.explode('nodes').alias('node')) \
      .distinct() \
      .withColumn('nodeIndex', f.col('node')['index']) \
      .withColumn('nodeId', f.col('node')['nodeId'])
    return dfWExp
  
  def __expand_relation(self, dfR):
    dfRExp = dfR \
      .select(
        'id', 
        'version', 
        'timestamp', 
        'changeset', 
        'uid', 
        'user_sid',
        'tags',
        f.explode('members').alias('member')) \
      .distinct()
    return dfRExp
  
  def __centroid_way(self, dfN, dfW):
    dfWExp = self.__expand_way(dfW)
    # Calculate the centroid of the ways
    dfWCentroid = dfWExp \
      .join(dfN.withColumnRenamed('id', 'nodeId').select('nodeId', 'latitude', 'longitude'), on='nodeId' , how='left') \
      .groupBy('id') \
      .agg(f.mean('latitude').alias('latitude'), 
           f.mean('longitude').alias('longitude'))
    return dfWCentroid

  def __split_relation(self, dfR):
    dfRExp = self.__expand_relation(dfR)
    # Split the members of the relations
    dfRSplit = dfRExp \
      .withColumn('memberId', f.col('member')['id']) \
      .withColumn('memberRole', f.col('member')['role']) \
      .withColumn('memberType', f.col('member')['type']) \
      .withColumn('nodeId', f.when(f.col('memberType')=='Node', f.col('memberId')).otherwise(f.lit(None))) \
      .withColumn('wayId', f.when(f.col('memberType') == 'Way', f.col('memberId')).otherwise(f.lit(None))) \
      .withColumn('relationId', f.when(f.col('memberType') == 'Relation', f.col('memberId')).otherwise(f.lit(None))) \
      .where(f.col('relationId').isNull())
    return dfRSplit
  
  def __centroid_relation(self, dfR, dfW, dfN):
    # Add the centroids of the ways and the coordinates of the nodes, to the parent relation.
    # Next, take the average of these coordinates.
    # Note that relations consisting of relations (such as the European Union relation) are discarded by this step. We assume that businesses are not relations of relations.
    dfRSplit = self.__split_relation(dfR)  
    dfWCentroid = self.__centroid_way(dfN, dfW)
    
    dfRWayPoint = dfRSplit \
      .where(f.col('wayId').isNotNull()) \
      .join(dfWCentroid.withColumnRenamed('id', 'wayId'), on='wayId', how='left') \
      .drop('wayId')
    dfRNodePoint = dfRSplit \
      .where(f.col('nodeId').isNotNull()) \
      .join(dfN.select('id', 'latitude', 'longitude').withColumnRenamed('id', 'nodeId'), on='nodeId', how='left') \
      .drop('nodeId')
    dfRCentroid = dfRWayPoint \
      .union(dfRNodePoint) \
      .groupBy('id') \
      .agg(f.mean('latitude').alias('latitude'), f.mean('longitude').alias('longitude'))
    return dfRCentroid
  
  def merge_data_sources(self):
    dfN, dfW, dfR = self.__load_data()    
    # Create an all point dataframe
    dfWCentroid = self.__centroid_way(dfN, dfW)
    dfRCentroid = self.__centroid_relation(dfR, dfW, dfN)
    dfFull = dfR \
      .withColumn('type', f.lit('relation')) \
      .join(dfRCentroid, on='id', how='left') \
      .select('id', 'type', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'latitude', 'longitude') \
      .union(dfW \
        .withColumn('type', f.lit('way')) \
        .join(dfWCentroid, on='id', how='left') \
        .select('id', 'type', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'latitude', 'longitude')) \
      .union(dfN \
        .withColumn('type', f.lit('node')) \
        .select('id', 'type', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'latitude', 'longitude')) \
      .withColumn('countryCode', f.lit(countryCodeU)) \
      .withColumnRenamed('id', 'osmId') \
      .withColumn('id', f.sha1(f.concat(f.col('osmId'), f.col('type'))))
    return dfFull

# COMMAND ----------

country = getArgument("country").lower()
countryCodeU = getArgument("countryCode").upper()
# country = 'netherlands'
# countryCodeU = 'NL'

dfFull = CombineOSMDataLevels(country, countryCodeU).merge_data_sources()

# COMMAND ----------

from delta_utils.write import WriteDeltaHive

hiveTable = 'dev_sources_osm.cleaned_total_data_set'
deltaTable = '/mnt/datamodel/dev/sources/osm/cleaned/total_data_set'

WriteDeltaHive(spark, deltaTable, hiveTable, dfFull, False, ['countryCode'], ['countryCode'], [countryCodeU]).main()

# dfFull.write \
#   .format("delta") \
#   .mode("overwrite") \
#   .option("overwriteSchema", "true") \
#   .option("replaceWhere", "countryCode = '" + countryCodeU + "'") \
#   .partitionBy("countryCode") \
#   .save(deltaTable)

# sqlQuery1 = "drop table if exists " + hiveTable
# sqlQuery2 = "create table " + hiveTable + " using delta location '" + deltaTable + "'"
# spark.sql(sqlQuery1)
# spark.sql(sqlQuery2)

# COMMAND ----------

# dfRSplit1 = dfRSplit.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit2 = dfRSplit1.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit3 = dfRSplit2.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit4 = dfRSplit3.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit5 = dfRSplit4.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit6 = dfRSplit5.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit7 = dfRSplit6.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit8 = dfRSplit7.where(f.col('relationId').isNotNull()) \
#   .select('id', 'version', 'timestamp', 'changeset', 'uid', 'user_sid', 'tags', 'member', 'relationId') \
#   .join(dfRSplit.select('id', 'memberId', 'memberInfo', 'memberType', 'nodeId', 'wayId', 'relationId').withColumnRenamed('relationId', 'relationId2').withColumnRenamed('id', 'relationId'), on='relationId', how='left') \
#   .drop('relationId') \
#   .withColumnRenamed('relationId2', 'relationId')

# dfRSplit8.groupBy('id').count().show()
