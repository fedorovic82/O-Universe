# Databricks notebook source
# https://download.geofabrik.de/

# continent, country, countryCode (ISO-2 code)

listContinentCountry = [
  ('africa', 'egypt', 'EG'),
  ('asia', 'israel-and-palestine','IL'),
  ('africa', 'south-africa', 'ZA'),
  ('asia', 'gcc-states', 'GCC'),
  ('asia', 'indonesia', 'ID'),
  ('asia', 'israel-and-palestine', 'ILPS'),
  ('asia', 'jordan', 'JO'),
  ('asia', 'lebanon', 'LB'),
  ('asia', 'malaysia-singapore-brunei', 'MYSGBN'),
  ('asia', 'maldives', 'MV'),
  ('asia', 'pakistan', 'PK'),
  ('asia', 'philippines', 'PH'),
  ('asia', 'sri-lanka', 'LK'),
  ('asia', 'taiwan', 'TW'),
  ('asia', 'china', 'CN'),
  ('australia-oceania', 'australia', 'AU'),
  ('europe', 'austria', 'AT'),
  ('europe', 'belgium', 'BE'),
  ('europe','czech-republic','CZ'),
  ('europe','denmark','DK'),
  ('europe', 'france', 'FR'),
  ('europe', 'finland', 'FI'),
  ('europe', 'germany', 'DE'),
  ('europe', 'great-britain', 'GB'),
  ('europe', 'greece', 'GR'),
  ('europe', 'ireland-and-northern-ireland', 'IEGBNIR'),
  ('europe', 'italy', 'IT'),
  ('europe', 'netherlands', 'NL'),
  ('europe', 'norway', 'NO'),
  ('europe', 'poland', 'PL'),
  ('europe', 'portugal', 'PT'),
  ('europe', 'spain', 'ES'),
  ('europe','slovakia','SK'),
  ('europe', 'sweden', 'SE'),
  ('europe', 'switzerland', 'CH'),
  ('europe', 'turkey', 'TR'),
  ('south-america', 'argentina', 'AR'),
  ('south-america', 'brazil', 'BR')
]

# COMMAND ----------

for continent, country, countryCode in listContinentCountry:
  dbutils.notebook.run("00 Remove OSM old data", 12000, {"country": country})

# COMMAND ----------

# Refresh the mounted folder such that the mount caches are update with the information that the old osm data is not there anymore.
dbutils.fs.refreshMounts()

# COMMAND ----------

for continent, country, countryCode in listContinentCountry:
  dbutils.notebook.run("01 Download OSM Data from Geofabric", 12000, {"continent": continent, "country": country})

# COMMAND ----------

for continent, country, countryCode in listContinentCountry:
  dbutils.notebook.run("02 Transform OSM pbf to parquet", 12000, {"country": country})

# COMMAND ----------

for continent, country, countryCode in listContinentCountry:
  dbutils.notebook.run("03 Transform OSM parquet to delta table", 12000, {"country": country})

# COMMAND ----------

for continent, country, countryCode in listContinentCountry:
  dbutils.notebook.run("04 Combine OSM data levels and centroid calculation", 12000, {"country": country, "countryCode": countryCode})

# COMMAND ----------

dbutils.notebook.run("05 Unlock tag info and filter on business type", 12000)
