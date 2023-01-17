from pyspark.sql import SparkSession
from datetime import date
import uuid
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-hbase-tutorial') \
  .getOrCreate()

spark.sql("USE mmop_tesla_project")
df_stock = spark.sql("select COUNT(*) as count,MIN(p) as min_price, MAX(p) as max_price, AVG(p) as avg_price, current_date() as timestamp from stock where cast(ingestion_date as TIMESTAMP) > cast(current_timestamp as TIMESTAMP) + INTERVAL -1 days")

df_stock= df_stock.withColumn('key', F.expr("uuid()"))

# Define the schema for catalog
catalog = ''.join("""{
    "table":{"namespace":"default", "name":"stocks"},
    "rowkey":"key",
    "columns":{
        "key":{"cf":"rowkey", "col":"key", "type":"String"},
        "count":{"cf":"statistics", "col":"count", "type":"Integer"},
        "min_price":{"cf":"statistics", "col":"min_price", "type":"Float"},
        "max_price":{"cf":"statistics", "col":"max_price", "type":"Float"},
        "avg_price":{"cf":"statistics", "col":"avg_price", "type":"Float"},
        "timestamp":{"cf":"statistics", "col":"timestamp", "type":"Date"}
    }
}""".split())

# Write to HBase
df_stock.write.format('org.apache.hadoop.hbase.spark').options(catalog=catalog).option("hbase.spark.use.hbasecontext", "false").mode("append").save()

# Read from HBase
# result = spark.read.format('org.apache.hadoop.hbase.spark').options(catalog=catalog).option("hbase.spark.use.hbasecontext", "false").load()
# result.show()