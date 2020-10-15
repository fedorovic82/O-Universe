# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t
from textMine.fuzzyMatch.jaroWinkler import jaroWinklerAlphanum
from textMine.fuzzyMatch.removeTags import removeTags
from textMine.fuzzyMatch.rankMatch import rankMatch
from textMine.basicFunctions.alphaNumString import alphaNumString
from geoCode.geoHash import geoHashSpark as geoHashSpark
import re
import unicodedata
from unidecode import unidecode
import uuid


# COMMAND ----------

# MAGIC %md
# MAGIC #### Set up Configuration

# COMMAND ----------

fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
precision = 5
minNameScore = 0.8
minAddressScore = 0.5

# COMMAND ----------

def castInteger(string):
  try:
    valInteger = int(float(string))
  except:
    print('error')
    valInteger = string
    
  return valInteger 
  
castIntegerUdf = f.udf(castInteger)

def cleanPostalCode(string,countDigits,regexPostalCode):
  if string =='':
    return string
  else:
    try:
      if bool(re.search("^[0-9]*$",string)) == True:
        print('onlyDigits')
        stringN = str(string).zfill(countDigits)
      else:
        stringN = re.sub(regexPostalCode, "", string).lower()
    except:
        stringN = string
  return stringN

cleanPostalCodeUdf = f.udf(lambda x,y,z: cleanPostalCode(x,y,z))

# COMMAND ----------

print(cleanPostalCode('555 00',5,'\s+'))

# COMMAND ----------

def alphaNumString(string):

    string = (string)
#     string = str(string).lower().strip()
#     string = re.sub('\s+','',string)
    string = re.sub('[^a-zA-Z0-9(,)\s]','',string)
    return string
  
def alphaNumStringSpark(df,inCol,outCol):
  alphaNumStringUDF = f.udf(alphaNumString)
  df = df.withColumn(outCol,alphaNumStringUDF(f.col(inCol)))
  return df

def addUuid(operatorid):
    newUuid = str(uuid.uuid4())
    return newUuid

udfAddUuid = f.udf(addUuid)

# COMMAND ----------

# MAGIC %md
# MAGIC #### fuzzy matching class

# COMMAND ----------

class fuzzyOSMGPL(object):

  def __init__(self,countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore):
    
    self.countryName = countryName
    self.countryCode = countryCode
    self.fn_OSM = fn_OSM
    self.fn_GPL = fn_GPL
    self.tagList = tagList
    self.precision = precision
    self.minNameScore = minNameScore
    self.minAddressScore = minAddressScore 
    self.regexPostalCode = regexPostalCode
    self.digitsPostalCode = digitsPostalCode
    self.fn_Matches = fn_Matches

    
  def fuzzyOSMGPL(self):
    print('loading countryCode:', self.countryCode)
    osm = self.getOSM()
    gpl = self.getGPL()
    print('counts original files: ','\n',
          'osm',osm.select('osmId').distinct().count(),'\n',
          'gpl',gpl.select('placeIdGoogle').distinct().count(),'\n')
    
        
    dfTotal = self.getJoin(osm,gpl)
    
    print('counts total match files before fuzzy matching: ',dfTotal.count(),'\n',
      'osm',dfTotal.select('osmid').distinct().count(),'\n',
      'gpl',dfTotal.select('placeIdGoogle').distinct().count(),'\n',
      'osm/gpl',dfTotal.select('osmid','placeIdGoogle').distinct().count(),'\n')
    
    dfTotalF = self.fuzzyMatchingJaro(dfTotal)
    
    print('counts total match files after fuzzy matching: ',dfTotalF.count(),'\n',
          'osm',dfTotalF.select('osmid').distinct().count(),'\n',
          'gpl',dfTotalF.select('placeIdGoogle').distinct().count(),'\n',
          'osm/gpl',dfTotalF.select('osmid','placeIdGoogle').distinct().count(),'\n')
      
    dfFinal = self.rankMatch(dfTotalF)
    
    print('counts total match files after deduplication on both ID sets: ',dfFinal.count(),'\n',
          'osm',dfFinal.select('osmId').distinct().count(),'\n',
          'gpl',dfFinal.select('placeIdGoogle').distinct().count(),'\n',
          'osm/gpl',dfFinal.select('osmId','placeIdGoogle').distinct().count(),'\n')

    dfFinal = dfFinal.withColumn('validation',f.lit(''))
    dfFinal = dfFinal.withColumn('nameDistance',f.col('nameDistance').cast('float'))\
                     .withColumn('addressDistance',f.col('addressDistance').cast('float'))
    
    dfFinal = dfFinal.where(f.col('nameDistance') >= self.minNameScore).where(f.col('addressDistance') >= self.minAddressScore)
    dfFinal = dfFinal.select('osmId','placeIDGoogle',\
                             'nameOSM','nameGoogle',\
                             'addressOSM','addressGoogle',\
                             'validation','nameDistance','addressDistance','zipcode','geoHash')
    
    
    print('number of matches after removing the scores below the threshold and rank:', dfFinal.count())
    
    dfFinal = dfFinal.withColumn('randomizer',udfAddUuid(f.monotonically_increasing_id()))
    dfFinal.\
    sort(['randomizer'], ascending = False).\
    write.mode('overwrite').saveAsTable(self.fn_Matches)
    
    return dfFinal
  
  def removeTagsInName(self,dfInput,tagList,inCol,outCol):
    """
    remove the common words in name to increase the matching accuracy 
    """
    dfInput = dfInput.withColumn(outCol,removeTags(tagList)(f.col(inCol)))
    return dfInput
  
  def getOSM(self):
    """
    obtain the osm table from osm database and proceed the several cleaning step
    - select relevant columns
    - clean zipcode data and check whether the zipcode is in right format
    - geohash location of business (input parameter: precision) 
    """
    dfOSM = spark.table(self.fn_OSM).where(f.lower(f.col('countryCode'))==self.countryCode).\
                     where(f.col('name').isNotNull()).\
                     select('osmId',
                           'id', 
                           'type', 
                           'uid', 
                           f.col('name').alias('nameOSM'), 
                           f.concat(f.when(f.col('street').isNotNull(),f.col('street')).otherwise(f.lit('')),f.lit(" "),f.when(f.col('housenumber').isNotNull(),f.col('housenumber')).otherwise(f.lit('')),f.lit(", "),f.when(f.col('city').isNotNull(),f.col('city')).otherwise(f.lit(''))).alias('addressOSM'), 
                           f.col('housenumber').alias('housenumberOSM'), 
                           f.col('postcode').alias('zipcodeOSM'),  
                           f.col('city').alias('cityOSM'), 
                           f.col('BusinessTypesList').alias('businessTypeOSM'), 
                           f.col('website').alias('websiteOSM'), 
                           f.col('cuisineCleanList').alias('cuisineType'), 
                           f.col('latitude').alias('latitudeOSM'), 
                           f.col('longitude').alias('longitudeOSM')).\
                           where(f.col('nameOSM').isNotNull()).\
                           where((f.col('addressOSM').isNotNull()))
    
    print('number of OSM operators before cleaning',dfOSM.count())

    #clean Name
    dfOSM = self.removeTagsInName(dfOSM,tagList,inCol ='nameOSM',outCol = 'nameOSMTag')
    dfOSM = alphaNumStringSpark(dfOSM,'nameOSMTag','nameOSMClean')

    #clean address and postalcode
    dfOSM = alphaNumStringSpark(dfOSM,'addressOSM','addressOSMClean')
    dfOSM = dfOSM.withColumn('zipcodeOSMInt',castIntegerUdf(f.col('zipcodeOSM')))
    dfOSM = dfOSM.withColumn('zipcodeOSMClean',cleanPostalCodeUdf(f.col('zipcodeOSMInt'),f.lit(digitsPostalCode),f.lit(regexPostalCode)))
    
    #filter out coordinates is null
    dfOSM = dfOSM.where((f.col('latitudeOSM').isNotNull()) & (f.col('longitudeOSM').isNotNull())).\
                  where((f.col('latitudeOSM').cast('string') != '') &(f.col('longitudeOSM').cast('string') != ''))
                             
    dfOSM = geoHashSpark(dfOSM,'latitudeOSM','longitudeOSM',self.precision)
    print('number of OSM operators after cleaning',dfOSM.count())

    
    return dfOSM

  def getGPL(self):
    """
    obtain the osm table from GPL database and proceed the several cleaning steps
    - select relevant columns
    - clean zipcode data and check whether the zipcode is in right format
    - geohash location of business (input parameter: precision) 
    """

    dfGPL = spark.table(self.fn_GPL).\
                            where(f.lower(f.col('countryGoogle'))==(self.countryName)).\
                            select('PlaceIDGoogle', 
                                   'nameGoogle', 
                                   'addressGoogle', 
                                   'postalCodeGoogle', 
                                   'cityGoogle', 
                                   'countryGoogle',
                                   'businessTypeGoogle',
                                   'websiteGoogle',
                                   'latitudeGoogle',
                                   'longitudeGoogle')
    
    print('number of GPL operators before cleaning',dfGPL.count())


    #clean Name
    dfGPL = self.removeTagsInName(dfGPL,tagList,inCol ='nameGoogle',outCol = 'nameGoogleTag')
    dfGPL = alphaNumStringSpark(dfGPL,'nameGoogleTag','nameGoogleClean')
    
    #clean address and postalcode
    dfGPL = self.removeTagsInName(dfGPL,[self.countryName],inCol ='addressGoogle',outCol = 'addressGoogleTag')
    dfGPL = alphaNumStringSpark(dfGPL,'addressGoogleTag','addressGoogleClean')
    
    dfGPL = dfGPL.withColumn('postalCodeGoogleInt',castIntegerUdf(f.col('postalCodeGoogle')))
    dfGPL = dfGPL.withColumn('PostalCodeGoogleClean',cleanPostalCodeUdf(f.col('postalCodeGoogleInt'),f.lit(digitsPostalCode),f.lit(regexPostalCode)))
    
    

    #filter coordinates null
    dfGPL = dfGPL.where((f.col('latitudeGoogle').isNotNull()) & (f.col('longitudeGoogle').isNotNull())).\
                    where((f.col('latitudeGoogle').cast('string') != '') &(f.col('longitudeGoogle').cast('string') != ''))
    
    #geohash
    dfGPL = geoHashSpark(dfGPL,'latitudeGoogle','longitudeGoogle',self.precision)
    print('number of GPL operators after cleaning',dfGPL.count())
    return dfGPL
  
  def getJoin(self,dfOSM,dfGPL):
    """
    - dataframe1: join by zipcode
    - dataframe2: join by geohash
    """
    dfTotalGeohash = dfOSM.join(dfGPL, on='geoHash', how='left_outer')
    dfTotalGeohash = dfTotalGeohash.withColumn('zipcode',\
                                               f.when(f.col('zipcodeOSMClean').isNotNull(),f.col('zipcodeOSMClean'))\
                                               .otherwise(f.col('PostalCodeGoogleClean')))\
                                               .drop(f.col('zipcodeOSMClean')).drop(f.col('postalCodeGoogleClean'))
    
    dfTotalGeohash = dfTotalGeohash.select('osmId','id', 'type', 'uid', 'nameOSM', 'addressOSM', 'housenumberOSM', 'cityOSM', 'businessTypeOSM', 'websiteOSM', 'cuisineType', 'latitudeOSM', 'longitudeOSM', 'nameOSMClean', 'PlaceIDGoogle', 'nameGoogle', 'addressGoogle', 'cityGoogle', 'websiteGoogle', 'latitudeGoogle', 'longitudeGoogle', 'nameGoogleClean', 'zipcode','geoHash')
    
    dfOSM = dfOSM.withColumnRenamed('zipcodeOSMClean', 'zipcode').withColumnRenamed('geohash', 'geohashOSM')
    dfGPL = dfGPL.withColumnRenamed('postalCodeGoogleClean', 'zipcode').withColumnRenamed('geohash', 'geohashGPL')
    
    dfTotalZipcode = dfOSM.join(dfGPL, on='zipcode', how='left_outer')\
                          .withColumn('geohash', f.when(f.col('geohashOSM').isNotNull(),f.col('geohashOSM'))\
                                      .otherwise(f.col('geohashGPL'))).drop(f.col('geohashOSM')).drop(f.col('geohashGPL'))
    
    dfTotalZipcode = dfTotalZipcode.select('osmId','id', 'type', 'uid', 'nameOSM', 'addressOSM', 'housenumberOSM', 'cityOSM', 'businessTypeOSM', 'websiteOSM', 'cuisineType', 'latitudeOSM', 'longitudeOSM', 'nameOSMClean', 'PlaceIDGoogle', 'nameGoogle', 'addressGoogle', 'cityGoogle', 'websiteGoogle', 'latitudeGoogle', 'longitudeGoogle', 'nameGoogleClean', 'zipcode','geoHash')
    
    dfTotal = dfTotalGeohash.union(dfTotalZipcode).distinct()
#     display(dfTotal)
    return dfTotal
  
  def fuzzyMatchingJaro(self,dfTotal):
    """
    measure the similarity between two names and addresses
    """
    dfTotal = dfTotal.where(f.col('nameOSMClean').isNotNull()).where(f.col('nameGoogleClean').isNotNull()).where(f.regexp_replace(f.col('nameOSMClean'),'[^0-9a-zA-Z]','') != '').where(f.regexp_replace(f.col('nameGoogleClean'),'[^0-9a-zA-Z]','') != '')
    dfTotal = jaroWinklerAlphanum(dfTotal, 'nameOSMClean', 'nameGoogleClean', 'nameDistance')
    
    dfTotal = alphaNumStringSpark(dfTotal,'addressGoogle','addressGoogleClean')
    dfTotal = alphaNumStringSpark(dfTotal,'addressOSM','addressOSMClean')
    dfTotal = dfTotal.where(f.col('addressOSM').isNotNull()).where(f.col('addressGoogle').isNotNull()).where(f.regexp_replace(f.col('addressOSM'),'[^0-9a-zA-Z]','')  != '').where(f.regexp_replace(f.col('addressGoogle'),'[^0-9a-zA-Z]','') != '' )
    dfTotal = jaroWinklerAlphanum(dfTotal,'addressOSMClean', 'addressGoogleClean', 'addressDistance')
    dfTotal = dfTotal.withColumn('addressDistance',f.col('addressDistance').cast(t.DecimalType(16,8)))\
                     .withColumn('nameDistance',f.col('nameDistance').cast(t.DecimalType(16,8)))
    return dfTotal

  def rankMatch(self,dfTotal):
    
    """
    select the record with highest scores
    
    """
    dfFinal = rankMatch(dfTotal, 'zipcode', 'osmId','nameDistance', 'addressDistance', 'PlaceIDGoogle')
    return dfFinal


# COMMAND ----------

# MAGIC %md
# MAGIC #### input parameters

# COMMAND ----------

# MAGIC %md
# MAGIC #### union two sources and display the valid matches

# COMMAND ----------

'''
Taglists:
DE:https://westeurope.azuredatabricks.net/?o=5110279409247704#notebook/354388259246008/command/3625018647350816
tagList = ['restaurant', 'kneipe', 'bar', 'cafe', 'hôtel', 'motel', 'Bäckerei', 'Metzger', 'Campingplatz', 'camping', 'Tennisplatz', 
           'Bistrot', 'gasthaus','gästehaus','gasthof','landgasthof','hotelbetrieb','pension','hotelpension','herberge','jugendherberge','hostel','jugendheim','jugendhaus', 
           'Italienisch', 'pizzeria', 'Kantine', 'Fußballstadion', 'Krankenhaus', 'Altenheim','Klinik','Pflegeheim',
           'bäckerei','bakery','bakeries','backshop','bäcker','backhaus','brotladen','bäckerladen','backstube',
           'grundschule', 'weiterführendeschule', 'mittelschule', 'realschule', 'gymnasium', 'hauptschule',
          'polytechnikum','lehre','ausbildung', 'fachhochschule', 'universität','altenpflege','pflegepension', 'pflegezentrum', 
           'altenheim', 'altenwohnheim','sanatorium', 'seniorenheim',  'diakonie', 'altersheim','senioren wohnsitz', 'seniorenhaus', 'seniorencentrum',
          'seniorenzentrum', 'sozialer arbeitsplatz','kinderheim', 'jugendheim', 'jugenddorf']

ES:
tagList = ['colegio publico', 'colegio de educacion', 'instituto de educacion secundaria', 'colexio de educacion', 'centro de educacion', 'facultad de ciencias',
'escuela de música', 'escuela infantil e primaria', 'educacion infantil e primaria', 'taberna', 'bar', 'cafetería', 'cervecería', 'barra',
'pastilla', 'mostrador', 'barrera', 'restaurante', 'restoran', 'hostal', 'casa de huéspedes', 'albergue',
'hotel', 'hotelero', 'hoteleria', 'posada', 'campo de futbol', 'cancha de tenis', 'piscina municipal', 'cancha de padel', 'cancha de baloncesto',
'bolera', 'residencia de mayores', 'hogar del pensionista', 'pizzería', 'panaderia', 'cantina', 'camping', 'carnicero', 'verdugo', 'sanguinario', 'comedor']

IT:
tagList = ['ristorante', 'pub', 'bar', 'hotel', 'motel', 'forno', 'macellaio', 'campeggio', 'campo da tennis', 
           'Bistrot', 'scuola elementare', 'scuola media', 'scuola terziaria', 'Università','politecnico','licei',
           'locanda', 'pizzeria', 'mensa', 'stadio di calcio', 'ospedale', 'Casa di riposo','clinica','Assistenza domiciliare']

PT:
tagList = ['restaurante', 'barra', 'cafeteria', 'hotel', 'motel', 'padaria', 'açougueiro', 'acampamento', 'quadra de tênis', 
           'bistrot', 'politécnico', 'grade', 'escola', 'escola primaria', 'ensino médio','universidade','escola pública', 'escola particular',
           'pousada', 'italiano','pasta', 'pizzeria', 'cantina', 'restaurante da estrada', 'estádio de futebol', 'hospital', 'lar de idosos','casas para idosos', 'consultório']

FR:
tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis', 
           'Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle',
           'Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins']
           
TR:
taglist = ['Restaurant', 'Restoran', 'Restoranı', 'Lokanta', 'Lokantası', 'Cafe', 'Kafe', 'Hotel', 'Hoteli', 'Otel', 'Oteli', 'Kebap', 'Kebapçısı', 'Kebap Salonu', 'Kebab Salonu', 'Pide Salonu', 'Pide', 'Dönercisi', 'Döner', 'Büfe', 'Çiğ Köfte', 'Çiğköfte', 'Kokoreççi', 'Balıkçı', 'Balık', 'Pastanesi', 'Pastane', 'Börek', 'Börekçisi', 'Fırını', 'Fırın', 'Kahvaltı Salonu']


BE = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis','Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle','Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins', 'boulangerie', 'pizzeria', 'traiteur', 'college', 'cantine', 'logis', 'charcuterie', 'camping', 'bistrot', 'brasserie', 'ziekenhuis', 'medisch centrum', 'verpleegtehuis', 'verzorgingstehuis', 'kliniek']

SK = ['zmrzlina', 'bar','športové miesto','reštaurácia', 'kaviareň', 'nemocnica','stredná škola', 'sociálne pracovisko', 'pekáreň'','Diaľková chata','Motel','Školský internát','Chaty a chalupy','Iné'', 'Diaľničná', 'Vysoká škola','Hostel','Hotel','Pub','Zábavný','Karavan','Zoo','Štadión','Kino',''občerstvením','Food court','Primárna škola','bowlingová dráha','Byty a bungalovy','Pláviareň',' Pivná záhrada','Dom dôchodcov','Detská staroslivosť','miesto na kempovanie','restaurant', 'pub', 'bar', 'cafe', 'motel','camping','pizzeria', 'traiteur', 'college', 'cantine']

CZ = ["Zmrzlina", "bar", "Sportovní umístění", "Kavárna", "Nemocnice", "Střední škola", "Sociální pracoviště", "Pekařství","Vzdálená chata","Motel","Chaty a chalupy","Jiné","Restaurace","Dálniční","Terciární škola","Hostel","Hotel","Hospoda","Themeparks","Karavan","Zoo","Stadion","Kino","Food Court","Základní škola","Bazén","Biergarte","Kemp",'Ice cream','Bar', 'Sports location', 'Cafe', 'Hospital', 'Secondary school', 'Social workplace', 'Bakery', 'Remote hut', 'Motel', 'Restaurant', 'Highway restaurant', 'Tertiary school', 'Nursing home', 'Hostel', 'Hotel', 'Pub', 'Themeparks', 'Caravan site', "Zoo's", 'Stadium', 'Movie theater', 'Fast food restaurant', 'Food court', 'Primary school', 'Bowling alley', 'Swimming pool', 'Biergarten', 'Elderly home', 'Childcare', 'Camp site']
'''  

# COMMAND ----------

gpl = fuzzyOSMGPL(countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore).getGPL()
osm = fuzzyOSMGPL(countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore).getOSM()
total = fuzzyOSMGPL(countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore).getJoin(osm,gpl)
totalWithScore = fuzzyOSMGPL(countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore).fuzzyMatchingJaro(total)
totalWithScoreFilter = fuzzyOSMGPL(countryName,countryCode,fn_OSM,fn_GPL,fn_Matches,tagList,regexPostalCode,digitsPostalCode,precision,minNameScore,minAddressScore).fuzzyOSMGPL()

# COMMAND ----------

# DBTITLE 1,DK
countryCode = 'dk'
countryName = 'denmark'
fn_Matches = 'dev_derived_ouniverse.output_non_validated_matches_osm_gpl_' + countryCode
tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hotel', 'motel','cantine', 'college', 'cantine', 'camping', 'bakery', 'school']
regexPostalCode = ''
digitsPostalCode = ''

# COMMAND ----------

display(gpl)

# COMMAND ----------

display(osm)

# COMMAND ----------

# DBTITLE 1,TR - done
# countryCode = 'tr'
# countryName = 'turkey'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['Restaurant', 'Restoran', 'Restoranı', 'Lokanta', 'Lokantası', 'Cafe', 'Kafe', 'Hotel', 'Hoteli', 'Otel', 'Oteli', 'Kebap', 'Kebapçısı', 'Kebap Salonu', 'Kebab Salonu', 'Pide Salonu', 'Pide', 'Dönercisi', 'Döner', 'Büfe', 'Çiğ Köfte', 'Çiğköfte', 'Kokoreççi', 'Balıkçı', 'Balık', 'Pastanesi', 'Pastane', 'Börek', 'Börekçisi', 'Fırını', 'Fırın', 'Kahvaltı Salonu']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,FR - done
# countryCode = 'fr'
# countryName = 'france'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis', 
#            'Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle',
#            'Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,IT - done
# countryCode = 'it'
# countryName = 'italy'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['ristorante', 'pub', 'bar', 'hotel', 'motel', 'forno', 'macellaio', 'campeggio', 'campo da tennis', 
#            'Bistrot', 'scuola elementare', 'scuola media', 'scuola terziaria', 'Università','politecnico','licei',
#            'locanda', 'pizzeria', 'mensa', 'stadio di calcio', 'ospedale', 'Casa di riposo','clinica','Assistenza domiciliare']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,CH - done
# countryCode = 'ch'
# countryName = 'switzerland'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['restaurant', 'kneipe', 'bar', 'cafe', 'hôtel', 'motel', 'Bäckerei', 'Metzger', 'Campingplatz', 'camping', 'Tennisplatz', 
#            'Bistrot', 'gasthaus','gästehaus','gasthof','landgasthof','hotelbetrieb','pension','hotelpension','herberge','jugendherberge','hostel','jugendheim','jugendhaus', 
#            'Italienisch', 'pizzeria', 'Kantine', 'Fußballstadion', 'Krankenhaus', 'Altenheim','Klinik','Pflegeheim',
#            'bäckerei','bakery','bakeries','backshop','bäcker','backhaus','brotladen','bäckerladen','backstube',
#            'grundschule', 'weiterführendeschule', 'mittelschule', 'realschule', 'gymnasium', 'hauptschule',
#           'polytechnikum','lehre','ausbildung', 'fachhochschule', 'universität','altenpflege','pflegepension', 'pflegezentrum', 
#            'altenheim', 'altenwohnheim','sanatorium', 'seniorenheim',  'diakonie', 'altersheim','senioren wohnsitz', 'seniorenhaus', 'seniorencentrum',
#           'seniorenzentrum', 'sozialer arbeitsplatz','kinderheim', 'jugendheim', 'jugenddorf']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,GR - done
# countryCode = 'gr'
# countryName = 'greece'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['restaurant','hotel','motel', 'Ξενοδοχείο', 'Εστιατόριο', 'Ταχυφαγείο', 'Καφετέρια', 'Ζαχαροπλαστείο', 'Ενοικιαζόμενα', 'δωμάτια', 'Ταβέρνα', 'Μπαρ', 'Φούρνος', 'Ξενώνας', 'Μεζεδοπωλείο', 'Σουβλατζίδικο', 'Αρτοποιείο', 'Ουζερί', 'Πιτσαρία', 'Αρτοζαχαροπλαστείο', 'Τσιπουράδικο', 'Κρεπερί', 'Κυλικείο', 'Ψητοπωλείο', 'Μπεργκεράδικο', 'Catering']
# regexPostalCode = '\s+'
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,BE - done
# countryCode = 'be'
# countryName = 'belgium'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis','Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle','Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins', 'boulangerie', 'pizzeria', 'traiteur', 'college', 'cantine', 'logis', 'charcuterie', 'camping', 'bistrot', 'brasserie', 'ziekenhuis', 'medisch centrum', 'verpleegtehuis', 'verzorgingstehuis', 'kliniek']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# DBTITLE 1,AR - done 
# countryCode = 'ar'
# countryName = 'argentina'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['colegio publico', 'colegio de educacion', 'instituto de educacion secundaria', 'colexio de educacion', 'centro de educacion', 'facultad de ciencias','escuela de música', 'escuela infantil e primaria', 'educacion infantil e primaria', 'taberna', 'bar', 'cafetería', 'cervecería', 'barra','pastilla', 'mostrador', 'barrera', 'restaurante', 'restoran', 'hostal', 'casa de huéspedes', 'albergue',
# 'hotel', 'hotelero', 'hoteleria', 'posada', 'campo de futbol', 'cancha de tenis', 'piscina municipal', 'cancha de padel', 'cancha de baloncesto',
# 'bolera', 'residencia de mayores', 'hogar del pensionista', 'pizzería', 'panaderia', 'cantina', 'camping', 'carnicero', 'verdugo', 'sanguinario', 'comedor']
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# countryCode = 'sk'
# countryName = 'slovakia'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = ['zmrzlina', 'bar','športové miesto','reštaurácia', 'kaviareň', 'nemocnica','stredná škola', 'sociálne pracovisko', 'pekáreň','Diaľková chata','Motel','Školský internát','Chaty a chalupy','Iné', 'Diaľničná', 'Vysoká škola','Hostel','Hotel','Pub','Zábavný','Karavan','Zoo','Štadión','Kino','občerstvením','Food court','Primárna škola','bowlingová dráha','Byty a bungalovy','Pláviareň',' Pivná záhrada','Dom dôchodcov','Detská staroslivosť','miesto na kempovanie','restaurant', 'pub', 'bar', 'cafe', 'motel','camping','pizzeria', 'traiteur', 'college', 'cantine']
# regexPostalCode = '\s+'
# digitsPostalCode = 5
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

# countryCode = 'ph'
# countryName = 'philippines'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'motel','camping','pizzeria', 'traiteur', 'college', 'cantine']
# regexPostalCode = ''
# digitsPostalCode = ''

# COMMAND ----------

# MAGIC %md 
# MAGIC ## ISR not done yet

# COMMAND ----------

# countryCode = 'ir'
# countryName = 'israel'
# tagList = ['מזון', 'סטייקהאוס','פיצרייה','קפה','המברוגר','חוף','שיפודיה','מקסיקני','דגים','אסייאתי','קונדיטוריה','אולמות','בשר','קייטרינג','בתי מלון','איטלקי','סיטונאות','סופר','פאב','מוסד','חומוסיה',
# 'סנדוויץ בר','מסעדה','מסעדת','אירועים','מכללת','מלון','אירוח','הארחה','אורחן','מלונית','אולם','בר','פיצה','שניצל','דייג','שף','מטבח','אוכל','בית חולים','אוניברסיטה','תיכון','בית ספר','ישיבה','ישיבת','קיבוץ','מאפה','טבון','סלט','רטבים','רוטב','מעדני','מעדניה','שוקולד','מתוק','טעים','hotel','restaurant','streetfood']
# regexPostalCode = ''

# COMMAND ----------

# MAGIC %md
# MAGIC #### NORDICS

# COMMAND ----------

# countryCode = 'no'
# countryName = 'norway'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hotel', 'motel','cantine', 'college', 'cantine', 'camping', 'bakery', 'school']
# regexPostalCode = ''
# digitsPostalCode = 4

# COMMAND ----------

# countryCode = 'fi'
# countryName = 'finland'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hotel', 'motel','cantine', 'college', 'cantine', 'camping', 'bakery', 'school']
# regexPostalCode = ''
# digitsPostalCode = 5

# COMMAND ----------

# countryCode = 'se'
# countryName = 'sweden'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hotel', 'motel','cantine', 'college', 'cantine', 'camping', 'bakery', 'school']
# regexPostalCode = '\s+'
# digitsPostalCode = 5

# COMMAND ----------

# countryCode = 'ar'
# countryName = 'argentina'
# fn_OSM  = 'dev_sources_osm.cleaned_total_operator_base'
# fn_GPL  = 'dev_sources_gpl.cleaned_total_operator_base'
# tagList = []
# regexPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

  display(osm)

# COMMAND ----------

display(gpl)
