# Databricks notebook source
countryCode = 'gr'

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Deduplication:
# MAGIC * Every name only once per zipcode
# MAGIC * Every name only once per operatorid
# MAGIC * DeDuplication for non-informative records on business name x city level

# COMMAND ----------

def castInteger(string):
  try:
    string = string.replace(',','.')
    valInteger = int(float(string))
  except:
    print('error')
    valInteger = string
    
  return valInteger
  
castIntegerUdf = f.udf(castInteger)

# COMMAND ----------

class deduplication(object):
  
  def __init__(self,fn_universe):
    self.fn_universe = fn_universe
  
  
  def dedupAll(self):
    universeNZ = self.dedupNameZipcode()
    print('count after NameZipcode', universeNZ.count())
    universeId = self.dedupOperatorId(universeNZ)
    print('count after id', universeId.count())

    universeFinal = self.dedupNameCity(universeId)
    print('count after nameCity', universeFinal.count())
    
    universeFinal = universeFinal.where(f.col('name').isNotNull()).where(f.col('name') != '')
    universeFinal = universeFinal.where((f.col('postalCode').isNotNull()) | (f.col('address').isNotNull())).where((f.col('postalCode') != '') | (f.col('address') != ''))
    
    print('Count total universe records: \n',
          universeFinal.count(),'\n',
          "Count total universe id's: \n",
          universeFinal.select('operatorid').distinct().count()
         )
    
    universeFinal = universeFinal.where(f.col('rank_row') == 1).distinct()
    
    print('Count total universe records after deduplication on name x zipcode: \n',
          universeFinal.count(),'\n',
          "Count total universe id's  after deduplication on name x zipcode: \n",
          universeFinal.select('operatorid').distinct().count()
         )
    
    universeFinal = universeFinal.where(f.col('rank_row_operatorId')==1).distinct()

    print('Count total universe records after deduplication on operatorid: \n',
          universeFinal.count(),'\n',
          "Count total universe id's  after deduplication  on operatorid: \n",
          universeFinal.select('operatorid').distinct().count()
         )

    
    
    universeFinal = universeFinal.where(f.col('rank_row_city') == 1).distinct()

    print('Count total universe records after deduplication on city level: \n',
          universeFinal.count(),'\n',
          "Count total universe id's  after deduplication  on city level: \n",
          universeFinal.select('operatorid').distinct().count()
         )

#     universeFinal.write.mode('overwrite').saveAsTable("data_user_hien.test_de_dedup_step")
    
    return universeFinal
    
  def dedupNameCity(self,universeId):
    
    # Partition by name x city
    # Order by Existence of zipcode. Records with zipcode are prioritized over records with no zipcode.
    window = Window.partitionBy(f.concat(f.when(f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]', '').isNotNull(),\
                                                f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                         otherwise(f.lit('')),\
                                         f.lit('/'),\
                                         f.when(f.col('city').isNotNull(),\
                                                f.col('city')).\
                                         otherwise(f.lit('')))\
                               ).orderBy(f.when(f.col('postalCode').isNotNull(),\
                                                f.lit(1)).\
                                         otherwise(f.lit(0)).desc())
      
    universeNC = universeId.withColumn('rank_row_city',f.dense_rank().over(window))
      
    return universeNC
    
  def dedupOperatorId(self, universeNZ):

    # Partition by operatorid
    # Order by operator name
    window = Window.partitionBy(f.when(f.col('operatorId').isNotNull(),\
                                       f.col('operatorId')).\
                                otherwise(f.lit(''))\
                               ).orderBy(f.when(f.col('name').isNotNull(),\
                                                f.col('name')).\
                                         otherwise(f.lit('')).asc())
    
    universeId = universeNZ.withColumn('rank_row_operatorId',f.dense_rank().over(window))
    
    return universeId
  
  def dedupNameZipcode(self):
    
    universeNZ = self.fn_universe
    
#     universeNZ = universeNZ.withColumn('postalCode', castIntegerUdf(f.col('postalCode')))
    
    # Partition by name x zipcode
    # Order by osmId and PlaceIdGoogle
    window = Window.partitionBy(f.concat(f.when(f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]', '').isNotNull(),\
                                                f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                         otherwise(f.lit('')),\
                                         f.lit('/'),\
                                         f.when(f.regexp_replace(castIntegerUdf(f.col('postalCode')),'[^a-zA-Z0-9]','').isNotNull(),\
                                                f.regexp_replace(castIntegerUdf(f.col('postalCode')),'[^a-zA-Z0-9]','')).\
                                         otherwise(f.lit('')))).\
    orderBy(f.when(f.col('osmId').isNotNull(),\
                   f.col('osmId')).\
            otherwise(f.lit('')).desc(),\
            f.when(f.col('placeIdGoogle').isNotNull(),\
                   f.col('placeIdGoogle')).\
            otherwise(f.lit('')).desc(),\
            f.when(f.col('address').isNotNull(),\
                   1).\
            otherwise(f.lit(0)).desc()            
           )
    
    universeNZ = universeNZ.withColumn('rank_row',f.dense_rank().over(window))
      
    return universeNZ
  
  def testNameZipcode(self,universeFinal):
    w = Window.partitionBy(f.concat(f.when(f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]', '').isNotNull(),\
                                           f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                    otherwise(f.col('')),f.lit('/'),\
                                    f.when(f.regexp_replace(f.col('postalCode'),'[^a-zA-Z0-9]',''),\
                                           f.regexp_replace(f.col('postalCode'),'[^a-zA-Z0-9]','')).\
                                    otherwise(f.lit(''))))
    
    testNameZipcode = universeFinal.withColumn('dupeCount', \
                                               f.count(f.concat(f.when(f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]', '').isNotNull(),\
                                                                       f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                                                otherwise(f.lit('')),\
                                                                f.lit('/'),\
                                                                f.when(f.regexp_replace(f.col('postalCode'),'[^a-zA-Z0-9]','').isNotNull(),\
                                                                       f.regexp_replace(f.col('postalCode'),'[^a-zA-Z0-9]','')))).\
                                               over(w)).\
    where(f.col('dupeCount') > 1).\
    drop('dupeCount')

    return testNameZipcode
  
  def testNameCity(self,universeFinal):
     
    w = Window.partitionBy(f.concat(f.when(f.regexp_replace(f.col('name'),\
                                                            '[^a-zA-Z0-9]', '').isNotNull(),\
                                           f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                    otherwise(f.col('')),\
                                    f.lit('/'),\
                                    f.when(f.col('city').isNotNull(),\
                                           f.col('city')).\
                                    otherwise(f.lit('')))\
                          )
    
    testNameCity = universeFinal.withColumn('dupeCount', \
                                            f.count(f.concat(f.when(f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]', '').isNotNull(),\
                                                                    f.regexp_replace(f.col('name'),'[^a-zA-Z0-9]','')).\
                                                             otherwise(f.lit('')),\
                                                             f.lit('/'),\
                                                             f.when(f.col('city').isNotNull(),f.col('city')).\
                                                             otherwise(f.lit('')))).over(w)).\
    where(f.col('dupeCount') > 1).\
    drop('dupeCount')
    
    return testNameCity
    

# COMMAND ----------

universe = spark.table( "dev_derived_ouniverse.raw_maps_universe_"+str(countryCode)).withColumn('postalCode',f.when(castIntegerUdf(f.col('postalCode')) !='',f.col('postalCode')).otherwise(f.lit(None).cast('string')))
universeDedup = deduplication(universe).dedupAll()

# COMMAND ----------

# MAGIC %md ### Validate deduplication

# COMMAND ----------

nameOccurencePlus1 = universeDedup.groupBy('name', 'city').agg(f.count(f.lit(1)).alias('cntRecords')).where(f.col('cntRecords')>1).select('name', 'city').distinct()
universeDedupValidate =  universeDedup.join(nameOccurencePlus1, on = ['name', 'city'], how = 'left_semi').orderBy(f.col('city').asc(), f.col('name').asc(), f.col('postalCode').asc())

display(universeDedupValidate.select('name', 'postalCode', 'address', 'city'))

# COMMAND ----------

nameOccurencePlus1 = universeDedup.groupBy('name', 'postalCode').agg(f.count(f.lit(1)).alias('cntRecords')).where(f.col('cntRecords')>1).select('name', 'postalCode').distinct()
universeDedupValidate =  universeDedup.join(nameOccurencePlus1, on = ['name', 'postalCode'], how = 'left_semi').orderBy(f.col('city').asc(), f.col('name').asc(), f.col('postalCode').asc())

display(universeDedupValidate)

# COMMAND ----------

print(universeDedupValidate.select('name', 'postalCode', 'address', 'city').count(),\
      universeDedupValidate.select('name', 'postalCode', 'address', 'city').select('name').distinct().count())

# COMMAND ----------

# MAGIC %md ### Write to Delta Table

# COMMAND ----------

# Location where to save the Delta Table in the DBFS
deltaTable = "/mnt/datamodel/dev/derived/ouniverse/output_maps_universe_"+str(countryCode)

# Location where to put the table in the Databricks database menu
hiveTable = "dev_derived_ouniverse.output_maps_universe_"+str(countryCode)

# COMMAND ----------

# Write the data to a Delta Table
universeDedup.write \
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

