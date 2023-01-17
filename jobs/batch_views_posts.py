from pyspark.sql import SparkSession
from textblob import TextBlob
from datetime import date, timedelta
import uuid

import pandas as pd

from pyspark.sql import functions as F

import numpy as np
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import FloatType

from tqdm.notebook import tqdm
tqdm.pandas()

#Functions 
def calculate_sentiment(sentence):
    blob_text = TextBlob(sentence)
    sentiment = blob_text.sentiment.polarity
    return sentiment

def apply_sentiment(df):
    df["sentiment"] = df.progress_apply(lambda x: calculate_sentiment(x["text"]) , axis = 1)

    df= df.drop_duplicates()
    X = df[["id", "created_at", "sentiment"]]
    return X

def features_extraction(df):
    
    nr_neg = len(df[df["sentiment"] < -0.2])
    nr_pos = len(df[df["sentiment"] > 0.2])
    nr_neutral = len(df[(df["sentiment"] >= -0.2) & (df["sentiment"] <= 0.2)])
    mean_neg = np.mean(df[df["sentiment"] < -0.2]["sentiment"])
    mean_pos = np.mean(df[df["sentiment"] > 0.2]["sentiment"])
    mean_neutral = np.mean(df[(df["sentiment"] >= -0.2) & (df["sentiment"] <= 0.2)]["sentiment"])
    res_dict = {
        "nr_neg" : nr_neg,
        "nr_pos" : nr_pos,
        "nr_neutral" : nr_neutral, 
        "mean_neg" : mean_neg,
        "mean_pos" : mean_pos,
        "mean_neutral" : mean_neutral
    }
    return res_dict

# Initialize Spark Session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-hbase-tutorial') \
  .getOrCreate()

basePath = 'hdfs://cluster-a0d6-m/user/mmop/posts/'
paths = ['hdfs://cluster-a0d6-m/user/mmop/posts/*']
df_posts = spark.read.option("basePath", basePath).parquet(*paths)
df_posts = df_posts.filter(to_date(col("created_at")) > lit(date.today() - timedelta(days=1)))
count_posts = df_posts.count()

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

if(count_posts == 0):
    schema = StructType([StructField("key", StringType(), True),
                    StructField("count", IntegerType(), True),
                    StructField("nr_neg", IntegerType(), True),
                    StructField("nr_pos", IntegerType(), True),
                    StructField("nr_neutral", IntegerType(), True),
                    StructField("mean_neg", FloatType(), True),
                    StructField("mean_pos", FloatType(), True),
                    StructField("mean_neutral", FloatType(), True),
                    StructField("timestamp", DateType(), True)])
    
    hive_table_posts = spark.createDataFrame(
        [
            (str(uuid.uuid4()), count_posts, 0, 0, 0, None, None, None, date.today())
        ],
        schema=schema
    )    
else:
    df_posts = df_posts.toPandas()
    df_posts = apply_sentiment(df_posts)

    res = features_extraction(df_posts)

    to_save = pd.DataFrame(columns = ["nr_neg", "nr_pos", "nr_neutral", "mean_neg", "mean_pos", "mean_neutral", "timestamp"])
    to_save = to_save.append(res, ignore_index = True)
    to_save["count"] = count_posts
    to_save["timestamp"] = date.today()
    to_save["key"] = str(uuid.uuid4())
    hive_table_posts = spark.createDataFrame(to_save)

# Define the schema for catalog
catalog = ''.join("""{
    "table":{"namespace":"default", "name":"posts"},
    "rowkey":"key",
    "columns":{
        "key":{"cf":"rowkey", "col":"key", "type":"String"},
        "count":{"cf":"statistics", "col":"count", "type":"Integer"},
        "nr_neg":{"cf":"statistics", "col":"nr_neg", "type":"Integer"},
        "nr_neutral":{"cf":"statistics", "col":"nr_neutral", "type":"Integer"},
        "nr_pos":{"cf":"statistics", "col":"nr_pos", "type":"Integer"},
        "mean_neg":{"cf":"statistics", "col":"mean_neg", "type":"Float"},
        "mean_neutral":{"cf":"statistics", "col":"mean_neutral", "type":"Float"},
        "mean_pos":{"cf":"statistics", "col":"mean_pos", "type":"Float"},
        "timestamp":{"cf":"statistics", "col":"timestamp", "type":"Date"}
    }
}""".split())

# Write to HBase
hive_table_posts.write.format('org.apache.hadoop.hbase.spark').options(catalog=catalog).option("hbase.spark.use.hbasecontext", "false").mode("append").save()

# Read from HBase
# result = spark.read.format('org.apache.hadoop.hbase.spark').options(catalog=catalog).option("hbase.spark.use.hbasecontext", "false").load()
# result.show()