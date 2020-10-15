# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t

# COMMAND ----------

countryCode = 'ch'

# COMMAND ----------

def castInteger(string):
  try:
    valInteger = int(float(string))
  except:
    print('error')
    valInteger = string
    
  return valInteger 
  
castIntegerUdf = f.udf(castInteger)

# COMMAND ----------

class unionOunOHub(object):
    
  def __init__(self,countryCode,fn_oun,fn_ohub,fn_matches):
    
    self.countryCode    = countryCode
    self.fn_oun         = fn_oun
    self.fn_ohub        = fn_ohub
    self.fn_matches     = fn_matches
    self.listColumns    = ['operatorID','osmId','placeIdGoogle','operatorOhubId',\
                            'name','address','postalCode','city','latitude','longitude','businessType','website','phone',\
                            'nameGoogle','addressGoogle','postalCodeGoogle','cityGoogle',\
                            'latitudeGoogle','longitudeGoogle','businessTypeGoogle','websiteGoogle',
                            'type','uid','user_sid','nameOSM','addressOSM','postalCodeOSM','cityOSM',\
                            'latitudeOSM','longitudeOSM','businessTypeOSM','websiteOSM','cuisineTypeOSM','phoneOSM',\
                            'nameOHUB','addressOHUB','zipcodeOHUB','cityOHUB','operatorConcatID','channelOHUB']
    
  def getUnion(self):
    print('input countryCode', self.countryCode)
    
    oun = self.getOun()
    ohub = self.getOhub()
    validatedMatches  = self.getMatches()
    
    print('Places in OUN: \n',
          oun.select('operatorID').distinct().count(), 
          '\n',
          'Places in OHUB: \n',
          ohub.select('operatorOhubId').distinct().count()
         )    
    
    # Filter oun on records that are not part of the validated matches
    oun = oun.join(validatedMatches, on = ['operatorID'], how = 'left_anti')
    
    # Filter ohub on records that are not part of the validated matches
    ohub = ohub.join(validatedMatches, on = ['operatorOhubId'], how = 'left_anti')

    columnNames = ['operatorID','osmId','placeIdGoogle','name','address','postalCode','city','latitude','longitude',\
                              'businessType','website','phone','nameGoogle','addressGoogle','postalCodeGoogle',\
                              'cityGoogle','latitudeGoogle','longitudeGoogle','businessTypeGoogle',\
                              'websiteGoogle','type','uid','user_sid','nameOSM','addressOSM','postalCodeOSM',\
                              'cityOSM','latitudeOSM','longitudeOSM','businessTypeOSM','websiteOSM','cuisineTypeOSM',\
                              'phoneOSM']
    
    oun = oun.select(self.listColumns)

    ohub = ohub.select(self.listColumns)

    validatedMatches = validatedMatches.select(self.listColumns)


    print('Places in ouniverse excluding matches: \n',
          oun.select('operatorID').distinct().count(),
          '\n',
          'Places in ohub excluding matches: \n',
          ohub.select('operatorOHUBId').distinct().count(),
          '\n',
          'ouniverse Places in Validated Matches: \n',
          validatedMatches.select('operatorID').distinct().count(),
          '\n',
          'ohub Places in Validated Matches: \n',
          validatedMatches.select('operatorOHUBid').distinct().count(),
          '\n',
         )
    
    # Filter oun on records that are not part of the validated matches
    universe  = oun.union(ohub).union(validatedMatches)
    universe  = universe.withColumn('operatorID', f.concat(f.when(f.col("osmId").isNotNull(),\
                                                                      f.col('osmId')).otherwise(f.lit('')),\
                                                               f.lit(' / '),\
                                                               f.when(f.col('placeIdGoogle').isNotNull(),\
                                                                      f.col('placeIdGoogle')).otherwise(f.lit('')),\
                                                               f.lit(' / '),\
                                                               f.when(f.col('operatorOhubID').isNotNull(),\
                                                                      f.col('operatorOhubID')).otherwise(f.lit(''))))
    print('Universe counts: ',
          universe.select('operatorID').distinct().count(),\
          universe.select('operatorOHUBid').distinct().count())

    universe  = universe.where(f.col('name').isNotNull())
    universe = universe.\
                    withColumn('osmid',f.when(f.col('osmid') !='',f.col('osmid')).otherwise(f.lit(None).cast('string'))).\
                    withColumn('placeIdGoogle',f.when(f.col('placeIdGoogle') !='',f.col('placeIdGoogle')).otherwise(f.lit(None).cast('string'))).\
                    withColumn('operatorOhubID',f.when(f.col('operatorOhubID') !='',f.col('operatorOhubID')).otherwise(f.lit(None).cast('string'))).\
                    withColumn('postalCode',f.when(f.col('postalCode') !='',f.col('postalCode')).otherwise(f.lit(None).cast('string')))

    print('Universe counts excluding nameless places: ',
          universe.select('operatorid').distinct().count(),\
          universe.select('operatorOHUBid').distinct().count())\
                            
                            
    return universe

  
  def getOun(self):

      oun = spark.table(self.fn_oun)\
                               .withColumn('postalcode',castIntegerUdf(f.regexp_replace(f.when(f.col('postalcode').isNotNull(),
                                                        castIntegerUdf(f.col('postalcode'))).otherwise(f.lit('')),'[^a-zA-Z0-9]','')))\
                               .withColumn('operatorOhubId',f.lit(None).cast('string'))\
                               .withColumn('nameOHUB',f.lit(None).cast('string'))\
                               .withColumn('addressOHUB',f.lit(None).cast('string'))\
                               .withColumn('zipCodeOHUB',f.lit(None).cast('string'))\
                               .withColumn('cityOHUB',f.lit(None).cast('string'))\
                               .withColumn('operatorConcatID',f.lit(None).cast('string'))\
                               .withColumn('channelOHUB',f.lit(None).cast('string'))
      
      return oun
                                                    
  def getOhub(self):
    
      ohub = spark.table(self.fn_ohub)
      
      ohub = ohub\
                .withColumn('operatorID',f.when(f.col("operatorOHUBId").isNotNull(), f.col('operatorOHUBId')).otherwise(f.lit('')))\
                .withColumn('osmId',f.lit(None).cast('string'))\
                .withColumn('placeIdGoogle',f.lit(None).cast('string'))\
                .withColumn('name',f.col('nameOHUB'))\
                .withColumn('address',f.col('AddressOHUB'))\
                .withColumn('postalCode',f.col('zipcodeOhub'))\
                .withColumn('city',f.col('cityOHUB'))\
                .withColumn('latitude',f.lit(None).cast('string'))\
                .withColumn('longitude',f.lit(None).cast('string'))\
                .withColumn('businessType',f.col('channelOHUB'))\
                .withColumn('website',f.lit(None).cast('string'))\
                .withColumn('phone',f.lit(None).cast('string'))\
                .withColumn('nameGoogle',f.lit(None).cast('string'))\
                .withColumn('addressGoogle',f.lit(None).cast('string'))\
                .withColumn('postalCodeGoogle',f.lit(None).cast('string'))\
                .withColumn('cityGoogle',f.lit(None).cast('string'))\
                .withColumn('latitudeGoogle',f.lit(None).cast('string'))\
                .withColumn('longitudeGoogle',f.lit(None).cast('string'))\
                .withColumn('businessTypeGoogle',f.lit(None).cast('string'))\
                .withColumn('websiteGoogle',f.lit(None).cast('string'))\
                .withColumn('phoneGoogle',f.lit(None).cast('string')) \
                .withColumn('type',f.lit(None).cast('string'))\
                .withColumn('uid',f.lit(None).cast('string'))\
                .withColumn('user_sid',f.lit(None).cast('string'))\
                .withColumn('nameOSM',f.lit(None).cast('string'))\
                .withColumn('addressOSM',f.lit(None).cast('string'))\
                .withColumn('postalCodeOSM',f.lit(None).cast('string'))\
                .withColumn('cityOSM',f.lit(None).cast('string'))\
                .withColumn('latitudeOSM',f.lit(None).cast('string'))\
                .withColumn('longitudeOSM',f.lit(None).cast('string'))\
                .withColumn('businessTypeOSM',f.lit(None).cast('string'))\
                .withColumn('websiteOSM',f.lit(None).cast('string'))\
                .withColumn('phoneOSM',f.lit(None).cast('string'))\
                .withColumn('cuisineTypeOSM',f.lit(None).cast('string'))

      return ohub
    
  def getMatches(self):
    
    inputOun    =  spark.table(self.fn_oun)\
                                           .withColumn('postalcode',castIntegerUdf(f.regexp_replace(f.when(f.col('postalcode').isNotNull(),
                                                        castIntegerUdf(f.col('postalcode'))).otherwise(f.lit('')),'[^a-zA-Z0-9]','')))\
    
    inputOhub    =  spark.table(self.fn_ohub).\
         withColumn('zipCodeOhub',castIntegerUdf(f.regexp_replace(f.when(f.col('zipCodeOhub').isNotNull(),
                                                        f.col('zipCodeOhub')).otherwise(f.lit('')),'[^a-zA-Z0-9]','')))
    
    # store in dev_sources_ouniverse
    inputMatches = spark.table(self.fn_matches).select('OperatorIDOUN', 'operatorOhubId').withColumnRenamed('OperatorIDOUN','operatorID')
    
    matchesOUN =  inputMatches.join(inputOun,on = ['operatorID'],how = 'left_outer')
    matchesOUNOHUB = matchesOUN.join(inputOhub,on = ['operatorOhubId'], how = 'left_outer')
    
    matches = matchesOUNOHUB\
                            .withColumn('name',f.when(f.col('name').isNotNull(),f.col('name')).otherwise(f.col('nameOhub')))\
                            .withColumn('address',f.when(f.col('address').isNotNull(),f.col('address')).otherwise(f.col('addressOhub')))\
                            .withColumn('postalcode',f.when(f.col('postalcode').isNotNull(),f.col('postalcode')).otherwise(f.col('zipcodeOHUB')))\
                            .withColumn('city',f.when(f.col('city').isNotNull(),f.col('city')).otherwise(f.col('cityOHUB')))\
                            .withColumn('businessType',f.when(f.col('businessType').isNotNull(),\
                                  f.col('businessType')).otherwise(f.col('channelOHUB')))
                                        
    return matches
  

# COMMAND ----------

fn_oun     =  'dev_derived_ouniverse.output_maps_universe_' + countryCode
fn_ohub    =  'dev_sources_ohub.cleaned_operators_ssol_' + countryCode
fn_matches =  'dev_derived_ouniverse.output_validated_matches_oun_ohub_' + countryCode

# COMMAND ----------

oun = unionOunOHub(countryCode,fn_oun,fn_ohub,fn_matches).getOun()
ohub = unionOunOHub(countryCode,fn_oun,fn_ohub,fn_matches).getOhub()
matches = unionOunOHub(countryCode,fn_oun,fn_ohub,fn_matches).getMatches()
universe = unionOunOHub(countryCode,fn_oun,fn_ohub,fn_matches).getUnion()

# COMMAND ----------

display(universe.where((f.col('operatorOhubId').isNotNull()) & (f.col('placeIdGoogle').isNotNull()) & (f.col('osmId').isNotNull())))

# COMMAND ----------

# MAGIC %md ### Create Delta Table

# COMMAND ----------

# Location where to save the Delta Table in the DBFS
deltaTable = "/mnt/datamodel/dev/derived/ouniverse/raw_total_universe_"+str(countryCode)

# Location where to put the table in the Databricks database menu
hiveTable = "dev_derived_ouniverse.raw_total_universe_"+str(countryCode)

# COMMAND ----------

# Write the data to a Delta Table
universe.write \
  .format("delta") \
  .mode("overwrite") \
  .option("overwriteSchema", "true") \
  .save(deltaTable)


# COMMAND ----------

# Load the Delta Table into the Data interface within Databricks.
sqlQuery1 = "drop table if exists " + hiveTable
sqlQuery2 = "create table " + hiveTable + " using delta location " + "'" + deltaTable + "'"
spark.sql(sqlQuery1)
spark.sql(sqlQuery2)

# COMMAND ----------

# The data in a Delta Table can be reordered to make it faster to work with the data (you might have a lot of small files). To improve the speed of read queries, you can use OPTIMIZE to collapse small files into larger ones. 
sqlQuery3 = "optimize " + hiveTable
spark.sql(sqlQuery3)


# COMMAND ----------

display(spark.table('dev_derived_ouniverse.'))
