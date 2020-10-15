# Databricks notebook source
from pyspark.sql import functions as f
from pyspark.sql import types as t
from textMine.fuzzyMatch.jaroWinkler import jaroWinklerAlphanum
from textMine.fuzzyMatch.removeTags import removeTags
from textMine.fuzzyMatch.rankMatch import rankMatch
from geoCode.geoHash import geoHashSpark as geoHashSpark
import re
import unicodedata
from unidecode import unidecode
import uuid

# COMMAND ----------

# Safer solution :)

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

def alphaNumString(string):

    string = unidecode(string)
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

class fuzzyOunOhub(object):
  
  def __init__(self,countryCode,fn_oun,fn_ohub,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore):
    
    self.countryCode = countryCode
    self.fn_oun = fn_oun
    self.fn_ohub = fn_ohub
    self.tagList = tagList
    self.minNameScore = minNameScore
    self.minAddressScore = minAddressScore 
    self.regexPostalCode = regexPostalCode
    self.digitsPostalCode = digitsPostalCode
    
  def fuzzyOunOhub(self):
    
    print('loading countryCode:', self.countryCode)
    oun = self.getOun()
    ohub = self.getOhub()

    print('counts original files: ','\n',
          oun.select('operatorIdOUN').distinct().count(),'\n',
          ohub.select('operatorOhubId').distinct().count(),'\n')
    
    
    dfTotal = self.getJoin(oun,ohub)
    
    print('counts total match files before fuzzy matching: ','\n',
          dfTotal.count(),'\n',
          'oun', dfTotal.select('operatorIdOUN').distinct().count(),'\n',
          'ohub',dfTotal.select('operatorOhubId').distinct().count(),'\n',
          'oun/ohub',dfTotal.select('operatorIdOUN','operatorOhubId').distinct().count(),'\n',
          'counts total match files before filtering for clean names: ','\n',
          dfTotal.where(f.col('nameOUNClean').isNotNull()).select('operatorIdOUN').distinct().count(),'\n',
          dfTotal.where(f.col('nameOUNClean').isNotNull()).select('operatorOhubId').distinct().count(),'\n',
          dfTotal.where(f.col('nameOHubClean').isNotNull()).select('operatorIdOUN').distinct().count(),'\n',
          dfTotal.where(f.col('nameOHubClean').isNotNull()).select('operatorOhubId').distinct().count(),'\n',
          dfTotal.where(f.col('nameOUNClean').isNotNull()).where(f.col('nameOHubClean').isNotNull()).select('operatorIdOUN').distinct().count(),'\n',
          dfTotal.where(f.col('nameOHubClean').isNotNull()).where(f.col('nameOUNClean').isNotNull()).select('operatorOhubId').distinct().count(),'\n')
    
    dfTotalF = self.fuzzyMatchingJaro(dfTotal)    

    print('counts total match files after fuzzy matching: ','\n',
          dfTotalF.count(),'\n',
          'oun',dfTotalF.select('operatorIdOUN').distinct().count(),'\n',
          'ohub',dfTotalF.select('operatorOhubId').distinct().count(),'\n',
          'oun/ohub',dfTotalF.select('operatorIdOUN','operatorOhubId').distinct().count(),'\n')

    
    dfFinal  = self.rankMatch(dfTotalF)

    print('counts total match files after deduplication on both ID sets: ','\n',
          dfFinal.count(),'\n',
          'oun',dfFinal.select('operatorIdOUN').distinct().count(),'\n',
          'ohub',dfFinal.select('operatorOhubId').distinct().count(),'\n',
          'oun/ohub',dfFinal.select('operatorIdOUN','operatorOhubId').distinct().count(),'\n')

    
    dfFinal  = dfFinal.withColumn('validation',f.lit(''))
    dfFinal  = dfFinal.withColumn('nameDistance',f.col('nameDistance').cast('float'))\
                     .withColumn('addressDistance',f.col('addressDistance').cast('float'))

    print('counts total match files with valid name and address: ','\n',
          dfFinal.count(),'\n',
          'oun',dfFinal.select('operatorIdOUN').distinct().count(),'\n',
          'ohub',dfFinal.select('operatorOhubId').distinct().count(),'\n',
          'oun/ohub',dfFinal.select('operatorIdOUN','operatorOhubId').distinct().count(),'\n')
    
    
    dfFinal = dfFinal.where(f.col('nameDistance') >= self.minNameScore).where(f.col('addressDistance') >= self.minAddressScore)
    dfFinal = dfFinal.select('operatorOhubId','OperatorIDOUN','osmIdOUN','PlaceIDGoogleOUN',\
                             'nameOUN','nameOhub',\
                             'addressOUN','addressOHUB',\
                             'validation','nameDistance','addressDistance','zipcode')
    
    print('counts total match files after filtering out the score below threshold: ','\n',
      dfFinal.count(),'\n',
      'oun',dfFinal.select('operatorIdOUN').distinct().count(),'\n',
      'ohub',dfFinal.select('operatorOhubId').distinct().count(),'\n',
      'oun/ohub',dfFinal.select('operatorIdOUN','operatorOhubId').distinct().count(),'\n')
    
    
    dfFinal = dfFinal.withColumn('randomizer',udfAddUuid(f.monotonically_increasing_id()))
    dfFinal.\
    sort(['randomizer'], ascending = False).\
    write.mode('overwrite').saveAsTable('dev_derived_ouniverse.output_non_validated_matches_oun_ohub_' + self.countryCode)
    
    return dfFinal
  
  def removeTagsInName(self,dfInput,tagList,inCol,outCol):
    """
    remove the common words in name to increase the matching accuracy 
    """
    dfInput = dfInput.withColumn(outCol,removeTags(tagList)(f.col(inCol)))
    
    return dfInput
  
  def getOun(self):
    """
    """
    dfOun = spark.table(self.fn_oun).\
                     select(f.col('OperatorID').alias('OperatorIDOUN'),
                           f.col('osmId').alias('osmIdOUN'), 
                           f.col('PlaceIDGoogle').alias('PlaceIDGoogleOUN'), 
                           f.col('name').alias('nameOUN'), 
                           f.col('address').alias('addressOUN'),
                           f.col('postalcode').alias('postalCodeOUN'),
                           f.col('city').alias('cityOUN'),
                           f.col('Website').alias('websiteOUN'),
                           f.col('businessType').alias('businessTypeOUN'),
                           f.col('cuisineTypeOSM').alias('cuisineTypeOUN'),        
                           f.col('Latitude').alias('latitudeOUN'),
                           f.col('Longitude').alias('longitudeOUN'))
    
    dfOun = dfOun.where(f.col('nameOUN').isNotNull())\
                   .where(f.col('addressOUN').isNotNull())
    #clean Name
    dfOun = self.removeTagsInName(dfOun,tagList,inCol ='nameOUN',outCol = 'nameOUNTag')
    dfOun = alphaNumStringSpark(dfOun,'nameOUNTag','nameOUNClean')
    

    #clean address and postalcode
    dfOun = dfOun.withColumn('postalCodeOUNInt',castIntegerUdf(f.col('postalCodeOUN')))
    dfOun = dfOun.withColumn('postalCodeOUNClean',cleanPostalCodeUdf(f.col('postalCodeOUNInt'),f.lit(digitsPostalCode),f.lit(regexPostalCode)))
    
    dfOun = alphaNumStringSpark(dfOun,'addressOUN','addressOUNClean')


    
    return dfOun

  def getOhub(self):
    """
    """
    dfOhub = spark.table(self.fn_ohub)
    #clean Name
    dfOhub = dfOhub.where(f.col('nameOhub').isNotNull())\
                   .where(f.col('addressOhub').isNotNull())
    dfOhub = self.removeTagsInName(dfOhub,tagList,inCol ='nameOhub',outCol = 'nameOhubTag')
    dfOhub = alphaNumStringSpark(dfOhub,'nameOhubTag','nameOhubClean')
    
    #clean address and postalcode
    dfOhub = alphaNumStringSpark(dfOhub,'addressOhub','addressOhubClean')

    dfOhub = dfOhub.withColumn('zipCodeOhubInt',castIntegerUdf(f.col('zipCodeOhub')))
    
    dfOhub = dfOhub.withColumn('zipCodeOhubClean',cleanPostalCodeUdf(f.col('zipCodeOhubInt'),f.lit(digitsPostalCode),f.lit(regexPostalCode)))

    return dfOhub
  
  def getJoin(self,dfOun,dfOhub):
    """
    join by zipcode
    """

    dfOun = dfOun.withColumnRenamed('postalCodeOUNClean', 'zipcode')
    dfOhub = dfOhub.withColumnRenamed('zipCodeOhubClean', 'zipcode')
    
    dfTotal = dfOun.join(dfOhub, on='zipcode', how='left_outer')
    dfTotal = dfTotal.select('OperatorIDOUN','osmIdOUN', 'PlaceIDGoogleOUN', 'nameOUN','addressOUN',\
                                    'cityOUN', 'cityOUN', 'websiteOUN', 'businessTypeOUN', 'cuisineTypeOUN', 'latitudeOUN',\
                                    'longitudeOUN','nameOUNClean','addressOUNClean','operatorOhubId','operatorConcatID',\
                                    'nameOhub','addressOhub','zipcode','cityOhub','channelOhub','nameOhubClean','addressOhubClean')
  
    return dfTotal
  
  def fuzzyMatchingJaro(self,dfTotal):
    """
    measure the similarity between two names and addresses
    """
    dfTotal = dfTotal.where(f.col('nameOUNClean').isNotNull())\
                     .where(f.col('nameOhubClean').isNotNull())\
                     .where(f.regexp_replace(f.col('nameOUNClean'),'[^0-9a-zA-Z]','') != '')\
                     .where(f.regexp_replace(f.col('nameOhubClean'),'[^0-9a-zA-Z]','') != '')\

    dfTotal = jaroWinklerAlphanum(dfTotal,'nameOUNClean', 'nameOhubClean', 'nameDistance')
    dfTotal = dfTotal.where(f.col('addressOUNClean').isNotNull())\
                     .where(f.col('addressOhubClean').isNotNull())\
                     .where(f.regexp_replace(f.col('addressOUNClean'),'[^0-9a-zA-Z]','')  != '')\
                     .where(f.regexp_replace(f.col('addressOhubClean'),'[^0-9a-zA-Z]','') != '' )\
      
    
    dfTotal = jaroWinklerAlphanum(dfTotal,'addressOUNClean', 'addressOhubClean', 'addressDistance')
    dfTotal = dfTotal.withColumn('addressDistance',f.col('addressDistance').cast(t.DecimalType(16,8)))\
                     .withColumn('nameDistance',f.col('nameDistance').cast(t.DecimalType(16,8)))

    return dfTotal
                                 
  def rankMatch(self,dfTotal):
    
    """
    select the record with highest score
    
    """
    dfFinal = rankMatch(dfTotal, 'zipcode', 'operatorOhubId','nameDistance', 'addressDistance', 'operatorIDOUN')
    return dfFinal
  


# COMMAND ----------

display(spark.table(fn_oun))

# COMMAND ----------

countryCode = 'gr'
countryName = 'greece'
fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
fn_ohub_ssol      =   'dev_sources_ohub.cleaned_operators_ssol_' + countryCode
tagList = ['restaurant','hotel','motel', 'Ξενοδοχείο', 'Εστιατόριο', 'Ταχυφαγείο', 'Καφετέρια', 'Ζαχαροπλαστείο', 'Ενοικιαζόμενα', 'δωμάτια', 'Ταβέρνα', 'Μπαρ', 'Φούρνος', 'Ξενώνας', 'Μεζεδοπωλείο', 'Σουβλατζίδικο', 'Αρτοποιείο', 'Ουζερί', 'Πιτσαρία', 'Αρτοζαχαροπλαστείο', 'Τσιπουράδικο', 'Κρεπερί', 'Κυλικείο', 'Ψητοπωλείο', 'Μπεργκεράδικο', 'Catering']
regexPostalCode = '\s+'
digitsPostalCode = 5
precision = 5
minNameScore = 0.8
minAddressScore = 0.5

# COMMAND ----------

ohub = fuzzyOunOhub(countryCode,fn_oun,fn_ohub_ssol,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore).getOhub()
oun = fuzzyOunOhub(countryCode,fn_oun,fn_ohub_ssol,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore).getOun()
# total = fuzzyOunOhub(countryCode,fn_oun,fn_ohub_ssol,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore).getJoin(oun,ohub)
# totalWithScore = fuzzyOunOhub(countryCode,fn_oun,fn_ohub_ssol,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore).fuzzyMatchingJaro(total)
# totalWithScoreFilter = fuzzyOunOhub(countryCode,fn_oun,fn_ohub_ssol,tagList,regexPostalCode,digitsPostalCode,minNameScore,minAddressScore).fuzzyOunOhub()


# COMMAND ----------

display(ohub)

# COMMAND ----------

display(ohub)

# COMMAND ----------

# countryCode = 'it'
# countryName = 'italy'
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   "dev_sources_ohub.cleaned_operators_ssol_" + str(countryCode)
# tagList = ['ristorante', 'pub', 'bar', 'hotel', 'motel', 'forno', 'macellaio', 'campeggio', 'campo da tennis', 
#            'Bistrot', 'scuola elementare', 'scuola media', 'scuola terziaria', 'Università','politecnico','licei',
#            'locanda', 'pizzeria', 'mensa', 'stadio di calcio', 'ospedale', 'Casa di riposo','clinica','Assistenza domiciliare']
# regexPostalCode = ''
# digitsPostalCode = ''
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5

# COMMAND ----------

#postalCode = 3000-3030
# countryCode = 'ch'
# tagList = ['restaurant', 'kneipe', 'bar', 'cafe', 'hôtel', 'motel', 'Bäckerei', 'Metzger', 'Campingplatz', 'camping', 'Tennisplatz',          'Bistrot','gasthaus','gästehaus','gasthof','landgasthof','hotelbetrieb','pension','hotelpension','herberge','jugendherberge','hostel','jugendheim','jugendhaus', 'Italienisch', 'pizzeria', 'Kantine', 'Fußballstadion','Krankenhaus','Altenheim','Klinik','Pflegeheim','bäckerei','bakery','bakeries','backshop','bäcker','backhaus','brotladen','bäckerladen','backstube','grundschule', 'weiterführendeschule', 'mittelschule', 'realschule', 'gymnasium', 'hauptschule','polytechnikum','lehre','ausbildung', 'fachhochschule', 'universität','altenpflege','pflegepension', 'pflegezentrum', 
#  'altenheim', 'altenwohnheim','sanatorium', 'seniorenheim',  'diakonie', 'altersheim','senioren wohnsitz', 'seniorenhaus', 'seniorencentrum',
#           'seniorenzentrum', 'sozialer arbeitsplatz','kinderheim', 'jugendheim', 'jugenddorf']
# digitsPostalCode = 4
# regexPostalCode = ''
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_' + countryCode

# COMMAND ----------

# countryCode = 'pt'
# tagList = ['restaurante', 'barra', 'cafeteria', 'hotel', 'motel', 'padaria', 'açougueiro', 'acampamento', 'quadra de tênis', 
#            'bistrot', 'politécnico', 'grade', 'escola', 'escola primaria', 'ensino médio','universidade','escola pública', 'escola particular',
#            'pousada', 'italiano','pasta', 'pizzeria', 'cantina', 'restaurante da estrada', 'estádio de futebol', 'hospital', 'lar de idosos','casas para idosos', 'consultório']
# regexPostalCode = '\-'
# digitsPostalCode = 7
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_pt'

# COMMAND ----------

# countryCode = 'es'
# tagList = ['colegio publico', 'colegio de educacion', 'instituto de educacion secundaria', 'colexio de educacion', 'centro de educacion', 'facultad de ciencias',
# 'escuela de música', 'escuela infantil e primaria', 'educacion infantil e primaria', 'taberna', 'bar', 'cafetería', 'cervecería', 'barra',
# 'pastilla', 'mostrador', 'barrera', 'restaurante', 'restoran', 'hostal', 'casa de huéspedes', 'albergue',
# 'hotel', 'hotelero', 'hoteleria', 'posada', 'campo de futbol', 'cancha de tenis', 'piscina municipal', 'cancha de padel', 'cancha de baloncesto',
# 'bolera', 'residencia de mayores', 'hogar del pensionista', 'pizzería', 'panaderia', 'cantina', 'camping', 'carnicero', 'verdugo', 'sanguinario', 'comedor']
# regexPostalCode = ''
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_es'

# COMMAND ----------

# countryCode = 'at'
# tagList = ['restaurant', 'kneipe', 'bar', 'cafe', 'hôtel', 'motel', 'Bäckerei', 'Metzger', 'Campingplatz', 'camping', 'Tennisplatz',          'Bistrot','gasthaus','gästehaus','gasthof','landgasthof','hotelbetrieb','pension','hotelpension','herberge','jugendherberge','hostel','jugendheim','jugendhaus', 'Italienisch', 'pizzeria', 'Kantine', 'Fußballstadion','Krankenhaus','Altenheim','Klinik','Pflegeheim','bäckerei','bakery','bakeries','backshop','bäcker','backhaus','brotladen','bäckerladen','backstube','grundschule', 'weiterführendeschule', 'mittelschule', 'realschule', 'gymnasium', 'hauptschule','polytechnikum','lehre','ausbildung', 'fachhochschule', 'universität','altenpflege','pflegepension', 'pflegezentrum', 
#  'altenheim', 'altenwohnheim','sanatorium', 'seniorenheim',  'diakonie', 'altersheim','senioren wohnsitz', 'seniorenhaus', 'seniorencentrum',
#           'seniorenzentrum', 'sozialer arbeitsplatz','kinderheim', 'jugendheim', 'jugenddorf']
# regexPostalCode = ''
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_at'

# COMMAND ----------

# countryCode = 'fr'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis', 
#            'Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle',
#            'Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins']
# regexPostalCode = ''
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_fr'

# COMMAND ----------

# countryCode = 'gb'
# tagList = ['Ice cream', "children's and other type of holiday home", 'Bar', 'Sports location', 'Food delivery restaurant', 'Cafe', 'Hospital', 'Secondary school', 'Social workplace', 'Bakery', 'Remote hut', 'Motel', 'Student / School dormitory', 'Cottages & Cabins', 'Other', 'An tent or caravan pitch location within a campsite or caravan site', 'Restaurant', 'Highway restaurant', 'Tertiary school', 'Nursing home', 'Hostel', 'Hotel', 'Pub', 'Themeparks', 'Caravan site', "Zoo's", 'Stadium', 'Movie theater', 'Fast food restaurant', 'Food court', 'Primary school', 'Bowling alley', 'Visitor Flats and bungalows', 'Swimming pool', 'Biergarten', 'Elderly home', 'Childcare', 'Camp site']
# regexPostalCode = '\\s+'
# digitsPostalCode = 0
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_gb'


# COMMAND ----------

# countryCode = 'be'
# tagList = ['restaurant', 'pub', 'bar', 'cafe', 'hôtel', 'motel', 'boulangerie', 'bouchère', 'site de camp', 'camping', 'court de tennis','Bistrot', 'Camping Municipal','Lycée professionnel', 'Grill', 'Lycée général', 'École primaire privée', 'École primaire publique','École élémentaire publique','École maternelle','Auberge', 'italien', 'pizzeria', 'cantine', 'aire de restauration', 'stade de football', 'hôpital', 'maison de retraite','clinique','maison de soins', 'boulangerie', 'pizzeria', 'traiteur', 'college', 'cantine', 'logis', 'charcuterie', 'camping', 'bistrot', 'brasserie', 'ziekenhuis', 'medisch centrum', 'verpleegtehuis', 'verzorgingstehuis', 'kliniek']
# regexPostalCode = ''
# digitsPostalCode = 4
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_' + str(countryCode)



# COMMAND ----------

# countryCode = 'tr'
# tagList = ['Restaurant', 'Restoran', 'Restoranı', 'Lokanta', 'Lokantası', 'Cafe', 'Kafe', 'Hotel', 'Hoteli', 'Otel', 'Oteli', 'Kebap', 'Kebapçısı', 'Kebap Salonu', 'Kebab Salonu', 'Pide Salonu', 'Pide', 'Dönercisi', 'Döner', 'Büfe', 'Çiğ Köfte', 'Çiğköfte', 'Kokoreççi', 'Balıkçı', 'Balık', 'Pastanesi', 'Pastane', 'Börek', 'Börekçisi', 'Fırını', 'Fırın', 'Kahvaltı Salonu']
# regexPostalCode = ''
# digitsPostalCode = 5
# minNameScore      = 0.8
# minAddressScore   = 0.5
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_' + str(countryCode)



# COMMAND ----------

# countryCode = 'za'
# countryName = 'South Africa'
# fn_oun            =   "dev_derived_ouniverse.output_maps_universe_"+ str(countryCode)
# fn_ohub_ssol      =   'data_user_tim.operators_ssol_' + str(countryCode)
# regexPostalCode = ''
# digitsPostalCode = 4
# precision = 5
# minNameScore = 0.8
# minAddressScore = 0.5
# tagList = ['Ice cream', "children's and other type of holiday home", 'Bar', 'Sports location', 'Food delivery restaurant', 'Cafe', 'Hospital', 'Secondary school', 'Social workplace', 'Bakery', 'Remote hut', 'Motel', 'Student / School dormitory', 'Cottages & Cabins', 'Other', 'An tent or caravan pitch location within a campsite or caravan site', 'Restaurant', 'Highway restaurant', 'Tertiary school', 'Nursing home', 'Hostel', 'Hotel', 'Pub', 'Themeparks', 'Caravan site', "Zoo's", 'Stadium', 'Movie theater', 'Fast food restaurant', 'Food court', 'Primary school', 'Bowling alley', 'Visitor Flats and bungalows', 'Swimming pool', 'Biergarten', 'Elderly home', 'Childcare', 'Camp site']

# COMMAND ----------

display(ohub)

# COMMAND ----------

display(oun)

# COMMAND ----------

display(spark.table('dev_derived_ouniverse.output_non_validated_matches_oun_ohub_gr'))

# COMMAND ----------

display(spark.table('dev_derived_ouniverse.output_non_validated_matches_oun_ohub_it'))
