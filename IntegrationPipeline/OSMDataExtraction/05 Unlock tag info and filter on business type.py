# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t
from pyspark.sql.utils import AnalysisException
from delta_utils.write import WriteDeltaHive
import re

# Function to find specific (key, value) combinations (value is specified by user)
def obtain_tag(tagArray, keyTupleList):
  keyTupleListKeys = list(set([i[0] for i in keyTupleList]))
  keyTupleListValues = [i[1] for i in keyTupleList]
  values = []
  for tag in tagArray:
    if tag['key'] in keyTupleListKeys and tag['value'] in keyTupleListValues:
      values.append(tag['value'])
  if len(values) == 0:
    values = None
  return values

def obtain_tag_udf(keyTupleList):
  return f.udf(lambda c: obtain_tag(c, keyTupleList), t.ArrayType(t.StringType()))

# Function to get the values (can be any value) corresponding to a specific tag key
def obtain_tag_values(tagArray, key):
  value = None
  for tag in tagArray:
    if tag['key'] == key:
      value = tag['value']
      break
  return value

def obtain_tag_values_udf(key):
  return f.udf(lambda c: obtain_tag_values(c, key), t.StringType())

def clean_cuisine(s):
  if s != None:
    sSplit = re.split(';|,|&', s)
    sClean = [re.sub('\s+|^_|_$', '', i) for i in sSplit]
    sRes = sorted(sClean)
  else:
    sRes = None
  return sRes

clean_cuisine_udf = f.udf(lambda y: clean_cuisine(y), t.ArrayType(t.StringType()))


class UnlockTagInfoOSM(object):
  def __init__(self, df, tagList, tagValueList):
    self.df = df
    self.tagList = tagList
    self.tagValueList = tagValueList
 
  def __has_column(self, df, col):
    try:
        df[col]
        return True
    except AnalysisException:
        return False
      
  def main(self):
    dfExtracted = self.df \
      .withColumn('businessTypesList', obtain_tag_udf(businessTypes)(f.col('tags'))) \
      .where(f.col('businessTypesList').isNotNull())
    for (osmTag, tag) in self.tagValueList:
      dfExtracted = dfExtracted \
        .withColumn(tag, obtain_tag_values_udf(osmTag)(f.col('tags')))
    if self.__has_column(dfExtracted, 'cuisine'):
      dfExtracted = dfExtracted \
        .withColumn('cuisineCleanList', clean_cuisine_udf(f.col('cuisine')))
    return dfExtracted

# COMMAND ----------

# Business Types from OSM relevant for UFS, ordered alphabetically 
businessTypes = [
  ('shop', 'alcohol'),
  ('shop', 'bakery'),
  ('shop', 'beverages'),
  ('shop', 'butcher'),  
  ('shop', 'cheese'),
  ('shop', 'coffee'),
  ('shop', 'tea'),
  ('shop', 'convenience'),  
  ('shop', 'deli'),  
  ('shop', 'fruit'),
  ('shop', 'fruits'),
  ('shop', 'greengrocer'),
  ('shop', 'grocery'),  
  ('shop', 'ice_cream'),  
  ('shop', 'kiosk'),
  ('shop', 'seafood'),
  ('shop', 'supermarket'),
  ('shop', 'tobacco'),  
  ('shop', 'wine'),  
  ('amenity', 'bar'),
  ('amenity', 'biergarten'),
  ('amenity', 'cafe'),
  ('amenity', 'childcare'),
  ('amenity', 'community_centre'),
  ('amenity', 'fast_food'),
  ('amenity', 'food_court'),
  ('amenity', 'fuel'),
  ('amenity', 'clinic'),
  ('amenity', 'hospital'),  
  ('amenity', 'hospitals'),
  ('amenity', 'ice_cream'),
  ('amenity', 'nursing_home'),
  ('amenity', 'pub'),
  ('amenity', 'restaurant'),
  ('amenity', 'school'),
  ('amenity', 'social_facility'),
  ('amenity', 'university'),
  ('building', 'dormitory'),
  ('building', 'kiosk'),
  ('building', 'supermarket'),
  ('building', 'sport_hall'),
  ('building', 'stadium'),
  ('craft', 'bakery'),
  ('craft', 'caterer'),
  ('highway', 'services'),
  ('leisure', 'pitch'),
  ('leisure', 'summer_camp'),
  ('leisure', 'swimming_pool'),
  ('tourism', 'alpine_hut'),
  ('tourism', 'apartment'),
  ('tourism', 'camp_pitch'),
  ('tourism', 'camp_site'),
  ('tourism', 'caravan_site'),
  ('tourism', 'chalet'),
  ('tourism', 'hostel'),
  ('tourism', 'holiday_village'),  
  ('tourism', 'hotel'),
  ('tourism', 'motel'),
  ('tourism', 'resort'),
  ('tourism', 'wilderness_hut'),
  ('tourism', 'theme_park'),
  ('tourism', 'zoo')
]

tagValueList = [
  ('addr:country', 'country'),
  ('addr:state', 'state'),
  ('addr:suburb', 'suburb'),
  ('addr:province', 'province'),
  ('addr:place', 'place'),
  ('addr:city', 'city'),
  ('addr:postcode', 'postcode'),
  ('addr:street', 'street'),
  ('addr:full', 'address'),
  ('addr:housenumber', 'housenumber'),
  ('website', 'website'),
  ('phone', 'phone'),
  ('cuisine', 'cuisine'),
  ('name', 'name'),
  ('name:en', 'name:en'),
  ('name:ru', 'name:ru'),
  ('name:ar', 'name:ar'),
  ('name:ja', 'name:ja'),
  ('name:ko', 'name:ko'),
  ('name:fr', 'name:fr'),
  ('name:de', 'name:de'),
  ('name:uk', 'name:uk'),
  ('name:es', 'name:es'),
  ('name:uk', 'name:uk'),
  ('name:fi', 'name:fi'),
  ('name:be', 'name:be'),
  ('name:he', 'name:he'),
  ('name:br', 'name:br'),
  ('name:pl', 'name:pl'),
  ('name:sr', 'name:sr'),
  ('name:sv', 'name:sv'),
  ('name:ga', 'name:ga'),
  ('name:kn', 'name:kn'),
  ('name:el', 'name:el'),
  ('name:th', 'name:th'),
  ('name:it', 'name:it'),
  ('name:hu', 'name:hu'),
  ('name:ca', 'name:ca'),
  ('name:cs', 'name:cs'),
  ('name:lt', 'name:lt'),
  ('name:oc', 'name:oc'),
  ('name:ro', 'name:ro'),
  ('name:eu', 'name:eu'),
  ('name:ka', 'name:ka'),
  ('name:pt', 'name:pt'),
  ('name:my', 'name:my'),
  ('name:vi', 'name:vi'),
  ('name:cy', 'name:cy'),
  ('name:fa', 'name:fa'),
  ('name:hy', 'name:hy'),
  ('name:tr', 'name:tr'),
  ('name:hi', 'name:hi'),
  ('name:mk', 'name:mk'),
  ('name:bg', 'name:bg'),
  ('name:nl', 'name:nl'),
  ('name:ku', 'name:ku'),
  ('name:ar1', 'name:ar1'),
  ('name:en1', 'name:en1'),
  ('name:ko-Latn', 'name:ko-Latn'),
  ('name:sr-Latn', 'name:sr-Latn'),
  ('name:ja_rm', 'name:ja_rm'),
  ('name:ja-Hira', 'name:ja-Hira'),
  ('name:ja_kana', 'name:ja_kana'),
  ('name:ja-Latn', 'name:ja-Latn'),
  ('name:zh-Hant', 'name:zh-Hant')
]

dfFull = spark.table('dev_sources_osm.cleaned_total_data_set')

# COMMAND ----------

dfOperators = UnlockTagInfoOSM(dfFull, businessTypes, tagValueList).main()

# COMMAND ----------

localNameList = ['name:ru', 'name:ar', 'name:ja', 'name:ko', 'name:fr', 'name:de', 'name:uk', 'name:es', 'name:uk', 'name:fi', 'name:be', 'name:he', 'name:br', 'name:pl', 'name:sr', 'name:sv', 'name:ga', 'name:kn', 'name:el', 'name:th', 'name:it', 'name:hu', 'name:ca', 'name:cs', 'name:lt', 'name:oc', 'name:ro', 'name:eu', 'name:ka', 'name:pt', 'name:my', 'name:vi', 'name:cy', 'name:fa', 'name:hy', 'name:tr', 'name:hi', 'name:mk', 'name:bg', 'name:nl', 'name:ku', 'name:ar1', 'name:en1', 'name:ko-Latn', 'name:sr-Latn', 'name:ja_rm', 'name:ja-Hira', 'name:ja_kana', 'name:ja-Latn', 'name:zh-Hant']

columns = [f.col(i) for i in localNameList]

dfOperatorsNamesCombined = dfOperators \
  .withColumn("name:local", f.array_except(f.array(columns), f.array(f.lit(None)))[0]) \
  .withColumn("name", f.when(f.col('name').isNull(), f.when(f.col('name:en').isNull(), f.col('name:local')).otherwise(f.col('name:en'))).otherwise(f.col('name'))) \
  .drop(*localNameList)

# COMMAND ----------

hiveTable = 'dev_sources_osm.cleaned_total_operator_base'
deltaTable = '/mnt/datamodel/dev/sources/osm/cleaned/total_operator_base'

WriteDeltaHive(spark, deltaTable, hiveTable, dfOperatorsNamesCombined, partitionByColList=['countryCode']).main()
