# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta_utils.write import WriteDeltaHive
import re
 
class TransformOSMParquetToDelta(object):
  def __init__(self, country, dbfsFolder = '/mnt/datamodel/dev/sources/osm/raw/'):
    self.country = country
    self.dbfsFolder = dbfsFolder

  def change_data_types_osm(self, df, dataType):
    dfDecoded = df \
      .withColumn('tags', f.col('tags').cast(t.ArrayType(t.StructType([t.StructField('key', t.StringType()), t.StructField('value', t.StringType())])))) \
      .withColumn('user_sid', f.col('user_sid').cast(t.StringType()))  
    if dataType == 'relation':
      dfDecoded = dfDecoded.withColumn('members', f.col('members').cast(t.ArrayType(t.StructType([t.StructField('id', t.LongType()), t.StructField('role', t.StringType()), t.StructField('type', t.StringType())]))))
    return dfDecoded

  def parquet_to_delta_osm(self):
    for dataType in ['node', 'way', 'relation']:
      countryMod = re.sub('-', '_', self.country)

      hiveTable = 'dev_sources_osm.raw_' + countryMod + '_' + dataType
      deltaTable = self.dbfsFolder       + countryMod + '_' + dataType

      df = spark.read.parquet("dbfs:" + self.dbfsFolder + self.country + '.osm.pbf.' + dataType + ".parquet") \

      dfDecoded = self.change_data_types_osm(df, dataType)    
      WriteDeltaHive(spark, deltaTable, hiveTable, dfDecoded).main()

# COMMAND ----------

country = getArgument("country")

# country = 'france'
TransformOSMParquetToDelta(country).parquet_to_delta_osm()
