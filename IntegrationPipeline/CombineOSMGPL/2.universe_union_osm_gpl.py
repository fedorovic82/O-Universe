# Databricks notebook source
# MAGIC %md ### Setup configuration

# COMMAND ----------

countryCode = 'gr'
countryName = 'greece'

# COMMAND ----------

# MAGIC %md ### Load packages

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

# MAGIC %md ### Create Union Object

# COMMAND ----------

# DBTITLE 0,unionClass
class unionOSMGPL(object):
    
  def __init__(self,countryName,countryCode,fn_gpl,fn_osm):
    
    self.countryName    = countryName
    self.countryCode    = countryCode
    self.fn_gpl         = fn_gpl
    self.fn_osm         = fn_osm
 
  def getUnion(self):
    print('input countryCode', self.countryCode)
    osm = self.getOSM()
    gpl = self.getGPL()
    
    validatedMatches  = self.getMatches()
    
    print('Places in OSM: \n',
          osm.select('osmId').distinct().count(), 
          '\n',
          'Places in GPL: \n',
          gpl.select('placeIdGoogle').distinct().count()
         )    
    
    # Filter osm on records that are not part of the validated matches
    osm = osm.join(validatedMatches, on = ['osmId'], how = 'left_anti')
    
    # Filter gpl on records that are not part of the validated matches
    gpl = gpl.join(validatedMatches, on = ['placeIdGoogle'], how = 'left_anti')

    columnNames = ['operatorID','osmId','placeIdGoogle','name','address','postalCode','city','latitude','longitude',\
                              'businessType','website','phone','nameGoogle','addressGoogle','postalCodeGoogle',\
                              'cityGoogle','latitudeGoogle','longitudeGoogle','businessTypeGoogle',\
                              'websiteGoogle','type','uid','user_sid','nameOSM','addressOSM','postalCodeOSM',\
                              'cityOSM','latitudeOSM','longitudeOSM','businessTypeOSM','websiteOSM','cuisineTypeOSM',\
                              'phoneOSM']
    
    gpl = gpl.select(columnNames)

    osm = osm.select(columnNames)

    validatedMatches = validatedMatches.select(columnNames)


    
    print('Places in OSM excluding matches: \n',
          osm.select('osmId').distinct().count(),
          '\n',
          'Places in GPL excluding matches: \n',
          gpl.select('placeIdGoogle').distinct().count(),
          '\n',
          'OSM Places in Validated Matches: \n',
          validatedMatches.select('osmId').distinct().count(),
          '\n',
          'GPL Places in Validated Matches: \n',
          validatedMatches.select('placeIdGoogle').distinct().count(),
          '\n',
         )
    
    # Filter osm on records that are not part of the validated matches
    ouniverse  = osm.union(gpl).union(validatedMatches)

    print('Universe counts: ',
          ouniverse.select('osmId').distinct().count(),\
          ouniverse.select('placeIdGoogle').distinct().count())

    ouniverse  = ouniverse.where(f.col('name').isNotNull())

    print('Universe counts excluding nameless places: ',
          ouniverse.select('operatorid').distinct().count(),\
          ouniverse.select('osmId').distinct().count(),\
          ouniverse.select('placeIdGoogle').distinct().count())
    
    
    return ouniverse
  
  def getOSM(self):
    osm = spark.table(self.fn_osm).where(f.lower(f.col('countryCode'))==self.countryCode) \
                          .where(f.col('street').isNotNull() | (f.col('street') != '')) \
                          .where(f.col('name').isNotNull() | (f.col('name') !='')) \
                          .withColumn('operatorID', f.when(f.col("osmId").isNotNull(), f.col('osmId')).otherwise(f.lit(""))) \
                          .withColumn('osmId',f.col('osmid').cast('string')) \
                          .withColumn('placeIdGoogle',f.lit('')) \
                          .withColumn('name',f.col('name')) \
                          .withColumn('address',(f.concat(f.when(f.col('street').isNotNull(),f.col('street')).otherwise(f.lit('')),f.lit(" "),f.when(f.col('housenumber').isNotNull(),f.col('housenumber')).otherwise(f.lit(''))))) \
                          .withColumn('postalCode',f.regexp_replace(f.when(f.col('postcode').isNotNull(),f.col('postcode')).otherwise(f.lit('')),'[^a-zA-Z0-9]','').cast('string')) \
                          .withColumn('city',f.col('city')) \
                          .withColumn('latitude',f.col('latitude').cast('float')) \
                          .withColumn('longitude',f.col('longitude').cast('float')) \
                          .withColumn('businessType',f.col('BusinessTypesList').cast('string')) \
                          .withColumn('website',f.col('website')) \
                          .withColumn('phone',f.col('phone')) \
                          .withColumn('nameGoogle',f.lit('')) \
                          .withColumn('addressGoogle',f.lit('')) \
                          .withColumn('postalCodeGoogle',f.lit('')) \
                          .withColumn('cityGoogle',f.lit('')) \
                          .withColumn('latitudeGoogle',f.lit('')) \
                          .withColumn('longitudeGoogle',f.lit('')) \
                          .withColumn('businessTypeGoogle',f.lit('')) \
                          .withColumn('websiteGoogle',f.lit('')) \
                          .withColumn('phoneGoogle',f.lit('')) \
                          .select('operatorID',
                                  'osmId',
                                  'placeIdGoogle',
                                  'name',
                                  'address',
                                  'postalCode',
                                  'city',
                                  'latitude',
                                  'longitude',
                                  'businessType',
                                  'website',
                                  'phone',
                                  'nameGoogle',
                                  'addressGoogle',
                                  'postalCodeGoogle',
                                  'cityGoogle',
                                  'latitudeGoogle',
                                  'longitudeGoogle',
                                  'businessTypeGoogle',
                                  'websiteGoogle',
                                  'type',                                                            
                                  'uid',
                                  'user_sid',
                                  f.col('name').alias('nameOSM'),
                                  f.col('address').alias('addressOSM'),
                                  f.col('postalCode').alias('postalCodeOSM'),
                                  f.col('city').alias('cityOSM'),
                                  f.col('latitude').alias('latitudeOSM'),
                                  f.col('longitude').alias('longitudeOSM'),
                                  f.col('businessType').alias('businessTypeOSM'),
                                  f.col('website').alias('websiteOSM'),
                                  f.col('cuisineCleanList').cast('string').alias('cuisineTypeOSM'),
                                  f.col('phone').alias('phoneOSM'))
    return osm
                                                    
  def getGPL(self):
      gpl = spark.table(self.fn_gpl) \
                          .where(f.lower(f.col('countryGoogle')) == self.countryName) \
                          .withColumn('operatorID', f.when(f.col("PlaceIDGoogle").isNotNull(), f.col('PlaceIDGoogle')).otherwise(f.lit(''))) \
                          .withColumn('osmId',f.lit('')) \
                          .withColumn('placeIdGoogle',f.col('PlaceIDGoogle')) \
                          .withColumn('name',f.col('nameGoogle')) \
                          .withColumn('address',f.col('AddressGoogle')) \
                          .withColumn('postalCode',f.col('PostalCodeGoogle')) \
                          .withColumn('city',f.col('CityGoogle')) \
                          .withColumn('latitude',f.col('latitudeGoogle')) \
                          .withColumn('longitude',f.col('longitudeGoogle')) \
                          .withColumn('businessType',f.col('businessTypeGoogle')) \
                          .withColumn('website',f.col('WebsiteGoogle')) \
      .withColumn('phone',f.when(f.col('PhoneNumberGoogle').isNotNull(),f.col('PhoneNumberGoogle')).otherwise(f.col('InternationalPhoneNumberGoogle'))) \
                          .withColumn('type',f.lit('')) \
                          .withColumn('uid',f.lit('')) \
                          .withColumn('user_sid',f.lit('')) \
                          .withColumn('nameOSM',f.lit('')) \
                          .withColumn('addressOSM',f.lit('')) \
                          .withColumn('postalCodeOSM',f.lit('')) \
                          .withColumn('cityOSM',f.lit('')) \
                          .withColumn('latitudeOSM',f.lit('')) \
                          .withColumn('longitudeOSM',f.lit('')) \
                          .withColumn('businessTypeOSM',f.lit('')) \
                          .withColumn('websiteOSM',f.lit('')) \
                          .withColumn('phoneOSM',f.lit('')) \
                          .withColumn('cuisineTypeOSM',f.lit('')) \
                          .select('operatorID',
                                  'osmId',
                                  'placeIdGoogle',
                                  'name',
                                  'address',
                                  'postalCode',
                                  'city',
                                  'latitude',
                                  'longitude',
                                  'businessType',
                                  'website',
                                  'phone',
                                  'nameGoogle',
                                  'addressGoogle',
                                  'postalCodeGoogle',
                                  'cityGoogle',
                                  'latitudeGoogle',
                                  'longitudeGoogle',
                                  'businessTypeGoogle',
                                  'websiteGoogle',
                                  'type',                                                            
                                  'uid',
                                  'user_sid',
                                  'nameOSM',
                                  'addressOSM',
                                  'postalCodeOSM',
                                  'cityOSM',
                                  'latitudeOSM',
                                  'longitudeOSM',
                                  'businessTypeOSM',
                                  'websiteOSM',
                                  'phoneOSM',
                                  'cuisineTypeOSM')
                                  
      return gpl
    
  def getMatches(self):
    
    inputGpl    =  spark.table(self.fn_gpl).where(f.lower(f.col('countryGoogle'))==self.countryName) 
    
    inputOSM    =  spark.table(self.fn_osm).where(f.lower(f.col('countryCode'))==self.countryCode) 
    
    # store in dev_sources_ouniverse
    inputMatches = spark.table('dev_derived_ouniverse.output_validated_matches_osm_gpl_' + countryCode).select('placeIdGoogle', 'osmId')
    
    matchesGPL =  inputMatches.join(inputGpl,on = ['placeIdGoogle'],how = 'left_outer')
    matchesGPLOSM = matchesGPL.join(inputOSM,on = ['osmid'], how = 'left_outer')
    
    matches = matchesGPLOSM.withColumn('operatorID', f.concat(f.when(f.col("osmId").isNotNull(), f.col('osmId')).otherwise(''),f.lit('/'),f.when(f.col('placeIdGoogle').isNotNull(),f.col('placeIdGoogle')).otherwise(f.lit('')))) \
                        .withColumn('nameOSM',f.col('name')) \
                        .withColumn('name',f.when(f.col('nameOSM').isNotNull(),f.col('nameOSM')).otherwise(f.col('nameGoogle'))) \
                        .withColumn('addressOSM',(f.concat(f.when(f.col('street').isNotNull(),f.col('street')).otherwise(f.lit('')),f.lit(" "),f.when(f.col('housenumber').isNotNull(),f.col('housenumber')).otherwise(f.lit(''))))) \
                        .withColumn('address',f.when(f.col('addressOSM').isNotNull(),f.col('addressOSM')).otherwise(f.col('addressGoogle'))) \
                        .withColumn('postalCodeOSM',f.regexp_replace(f.when(f.col('postcode').isNotNull(),f.col('postcode')).otherwise(f.lit('')),'[^a-zA-Z0-9]','').cast('int')) \
                       .withColumn('postalcode',f.when(f.col('postalCodeOSM').isNotNull(),f.col('postalCodeOSM')).otherwise(f.col('postalCodeGoogle'))) \
                       .withColumn('cityOSM',f.col('city')) \
                        .withColumn('city',f.when(f.col('cityOSM').isNotNull(),f.col('cityOSM')).otherwise(f.col('cityGoogle'))) \
                        .withColumn('latitudeOSM',f.col('latitude')) \
                        .withColumn('latitude',f.when(f.col('latitudeOSM').isNotNull(),f.col('latitudeOSM')).otherwise(f.col('latitudeGoogle'))) \
                        .withColumn('longitudeOSM',f.col('longitude')) \
                        .withColumn('longitude',f.when(f.col('longitudeOSM').isNotNull(),f.col('longitudeOSM')).otherwise(f.col('longitudeGoogle'))) \
                        .withColumn('businessTypeOSM',f.col('BusinessTypesList').cast('string')) \
        .withColumn('businessType',f.when(f.col('businessTypeOSM').isNotNull(),f.col('businessTypeOSM').cast('string')).otherwise(f.col('businessTypeGoogle'))) \
                          .withColumn('websiteOSM',f.col('website')) \
                          .withColumn('website',f.when(f.col('websiteOSM').isNotNull(),f.col('websiteOSM')).otherwise(f.col('websiteGoogle'))) \
                          .withColumn('phoneOSM',f.col('phone')) \
 .withColumn('phone',f.when(f.col('phoneOSM').isNotNull(),f.col('phoneOSM')).when(f.col('PhoneNumberGoogle').isNotNull(),f.col('PhoneNumberGoogle')).otherwise(f.col('internationalPhoneNumberGoogle'))) \
                          .select('operatorID',
                                  'osmId',
                                  'placeIdGoogle',
                                  'name',
                                  'address',
                                  'postalCode',
                                  'city',
                                  'latitude',
                                  'longitude',
                                  'businessType',
                                  'website',
                                  'phone',
                                  'nameGoogle',
                                  'addressGoogle',
                                  'postalCodeGoogle',
                                  'cityGoogle',
                                  'latitudeGoogle',
                                  'longitudeGoogle',
                                  'businessTypeGoogle',
                                  'websiteGoogle',
                                  'type',                                                            
                                  'uid',
                                  'user_sid',
                                  'nameOSM',
                                  'addressOSM',
                                 'postalCodeOSM',
                                  'cityOSM',
                                  'latitudeOSM',
                                  'longitudeOSM',
                                  'businessTypeOSM',
                                  'websiteOSM',
                                  f.col('cuisineCleanList').cast('string').alias('cuisineTypeOSM'),
                                  'phoneOSM')
    
    
    return matches
  


# COMMAND ----------

# MAGIC %md ### Run Union Object and functions

# COMMAND ----------

fn_gpl = 'dev_sources_gpl.cleaned_total_operator_base'
fn_osm = 'dev_sources_osm.cleaned_total_operator_base'
gpl = unionOSMGPL(countryName,countryCode,fn_gpl,fn_osm).getGPL()
osm = unionOSMGPL(countryName,countryCode,fn_gpl,fn_osm).getOSM()
matches = unionOSMGPL(countryName,countryCode,fn_gpl,fn_osm).getMatches()
ouniverse = unionOSMGPL(countryName,countryCode,fn_gpl,fn_osm).getUnion()

# COMMAND ----------

display(gpl)

# COMMAND ----------

display(osm)

# COMMAND ----------

display(matches)

# COMMAND ----------

# MAGIC %md ### Check main counts

# COMMAND ----------

print(ouniverse.count(),\
      ouniverse.select('operatorid').distinct().count(), '\n',\
      ouniverse.select('osmID').distinct().count(), '\n',\
      osm.select('osmID').distinct().count(),'\n', \
      matches.select('osmID').distinct().count(),'\n', \
      ouniverse.select('placeIDGoogle').distinct().count(),'\n', \
      gpl.select('placeIDGoogle').distinct().count(),'\n',
      matches.select('placeIDGoogle').distinct().count()
)


# COMMAND ----------

# MAGIC %md ### Create Delta Table

# COMMAND ----------

# Location where to save the Delta Table in the DBFS
deltaTable = "/mnt/datamodel/dev/derived/ouniverse/raw_maps_universe_"+str(countryCode)

# Location where to put the table in the Databricks database menu
hiveTable = "dev_derived_ouniverse.raw_maps_universe_"+str(countryCode)

# COMMAND ----------

# Write the data to a Delta Table
ouniverse.write \
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

# %sql

# refresh table data_user_tim.pt_osm_gpl_matches_validated;

# drop table if exists data_cleaning.pt_osm_gpl_mapping;
# create table data_cleaning.pt_osm_gpl_mapping
# as
# Select *
# From
# (
# Select *, row_number() over(partition by (case When osmID is not Null Then osmID else '' END), (case When placeIdGoogle is not Null Then placeIdGoogle else '' END) order by (case when nameDistance is not Null Then nameDistance else '' END) desc,(case when nameOSM is not Null Then nameOSM else '' END),(case when nameGPL is not Null Then nameGPL else '' END)) as rank
# From
# (
# Select s.osmID, t.*
# From data_user_tim.pt_osm_gpl_matches_validated t
#         left outer join
#      (select * from dev_sources_osm.cleaned_total_operator_base where countryCode = 'PT') s on t.id = s.id and t.type = s.type
# )
# )
# Where 1=1
# And rank = 1
# ;


# COMMAND ----------

# %sql

# Select count(*), count(distinct id), count(distinct osmID), count(distinct placeIdGoogle), count(distinct (case when osmID is not Null then osmID else '' end) || ' / ' || placeIdGoogle)
# From
# (Select distinct *
# from data_cleaning.pt_osm_gpl_mapping
# )
# ;

