// Databricks notebook source
val country = getArgument("country", "error")
println(country)

// COMMAND ----------

import sys.process._

// Download script to parquetize the osm data
"wget https://github.com/adrianulbona/osm-parquetizer/releases/download/v1.0.0/osm-parquetizer-1.0.0.jar -P /tmp/osm" !!

// COMMAND ----------

// Wait for a few seconds to lead the file load properly
Thread.sleep(5000)

// COMMAND ----------

import sys.process._

// Process the pbf format to parquet
"java -jar /tmp/osm/osm-parquetizer-1.0.0.jar /dbfs/mnt/datamodel/dev/sources/osm/raw/" + country + ".osm.pbf" !!
