from pyspark.sql.functions import udf,col, lower
from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.mllib.tree import RandomForestModel, RandomForest
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import monotonically_increasing_id, lit
from pyspark.sql.functions import col, greatest
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, StructField, StringType
from operator import itemgetter
from pyspark.sql.types import StructType
from textblob import TextBlob
import argparse
import typing
from typing import Optional, Callable
from google.cloud import pubsub_v1
from pyspark.sql.types import StructType, StructField, FloatType
import numpy as np
from google import pubsub_v1 as psv1
import pandas as pd
from io import StringIO
from datetime import datetime


def pull_all_posts(subscription_name):
    '''
        Pulls all posts in a buffer and deletes them from the buffer.
    '''
    client = psv1.SubscriberClient()
    pull_response = _pull_request(client, subscription_name)
    ack_ids, posts = _extract_posts(pull_response)
    if len(ack_ids) > 1:
        _ack_request(client, subscription_name, ack_ids)
    return posts

def _pull_request(client, subscription_name):
    request = psv1.PullRequest(
        subscription=subscription_name,
        max_messages=1277,
    )
    response = client.pull(request=request)
    return response

def _ack_request(client, subscription_name, ack_ids):
    request = psv1.AcknowledgeRequest(
        subscription=subscription_name,
        ack_ids=ack_ids)
    client.acknowledge(request=request)
    
def _get_post_pd_df(posts):
    buffer = '\n'.join(posts)
    csvStringIO = StringIO(buffer)
    df = pd.read_csv(csvStringIO, sep=",", header=None)
    df.columns = ['id', 'text', 'timestamp', 'source']
    return df
    
def _extract_posts(pull_response):
    ack_ids = [m.ack_id for m in pull_response.received_messages]
    posts = [m.message.data.decode('UTF-8') for m in pull_response.received_messages]
    posts_df = _get_post_pd_df(posts)
    return ack_ids, posts_df 


from pyspark.sql import SparkSession


def get_catalog():

    return ''.join("""{
        "table":{"namespace":"default", "name":"labels1"},
        "rowkey":"key",
        "columns":{
            "key":{"cf":"rowkey", "col":"key", "type":"string"},
            "label":{"cf":"labels", "col":"label", "type":"string"},
            "stock":{"cf":"stock", "col":"stock", "type":"string"},
            "posts":{"cf":"posts", "col":"posts", "type":"string"}
        }
    }""".split())



def write_to_hbase(df, catalog):
    df.write.format('org.apache.hadoop.hbase.spark').options(catalog=catalog).option("hbase.spark.use.hbasecontext", "false").mode("overwrite").save()

    
    
def _parse_stock_dataset(stock_pyspark_df):
    # Load and parse the data file, converting it to a DataFrame.
    columns = stock_pyspark_df.columns

    vectorAssembler = VectorAssembler(inputCols = columns, outputCol = 'features')
    data = vectorAssembler.transform(stock_pyspark_df)
    return data.select(['features'])

def _predict_stock(dataStock):
    
    ml_models_path = "gs://dataproc-staging-europe-west4-375495060785-ncrgfyir/notebooks/jupyter/ml_models/rf_stock_model"
    rf2 = RandomForestClassificationModel.load(ml_models_path)

    predictions_stock = rf2.transform(dataStock)
    return predictions_stock.select('probability')


def _calculate_sentiment(sentence):
    blob_text = TextBlob(sentence)
    sentiment = blob_text.sentiment.polarity
    return sentiment

 
def _apply_sentiment(df):
    df["sentiment"] = df.apply(lambda x: _calculate_sentiment(x["text"]) , axis = 1)

    df= df.drop_duplicates()
    X = df[["sentiment"]]
    return X

def _features_extraction(df):
    
    nr_neg = len(df[df["sentiment"] < -0.2])
    nr_pos = len(df[df["sentiment"] > 0.2])
    nr_neutral = len(df[(df["sentiment"] >= -0.2) & (df["sentiment"] <= 0.2)])
       
    if nr_pos == 0:
        mean_pos = 0.4  
    else:
        mean_pos = np.mean(df[df["sentiment"] > 0.2]["sentiment"])
        
    if nr_neg == 0:
        mean_neg = -0.4
    else:
        mean_neg = np.mean(df[df["sentiment"] < -0.2]["sentiment"])
        
    if nr_neutral == 0:
        mean_neutral = 0.0
    else:
        mean_neutral = np.mean(df[(df["sentiment"] >= -0.2) & (df["sentiment"] <= 0.2)]["sentiment"])
        
    
    res_dict = {        
        "nr_neg" : nr_neg,
        "nr_pos" : nr_pos,
        "nr_neutral" : nr_neutral, 
        "mean_neg" : mean_neg,
        "mean_pos" : mean_pos,
        "mean_neutal" : mean_neutral
    }
        
    return res_dict


def _parse_post_dataset(posts_pd_df):
    
    posts_pd_df_sent = _apply_sentiment(posts_pd_df)
    df_post = pd.DataFrame(columns = ["nr_neg", "nr_pos", "nr_neutral", "mean_neg", "mean_pos", "mean_neutal"])
    fe = _features_extraction(posts_pd_df_sent)
    posts_pd_df_features = df_post.append(fe, ignore_index = True)  
    posts_spark_df_features = spark.createDataFrame(posts_pd_df_features)
    
    vectorAssembler = VectorAssembler(inputCols = ["nr_neg", "nr_pos", "nr_neutral", "mean_neg", "mean_pos", "mean_neutal"], outputCol = 'features')
    data = vectorAssembler.transform(posts_spark_df_features)
    return data.select(['features'])
    

    
def _predict_posts(dataPosts):
    
    posts_model = RandomForestClassificationModel.load("gs://dataproc-staging-europe-west4-375495060785-ncrgfyir/notebooks/jupyter/ml_models/rf_posts_model")
    predictions_posts = posts_model.transform(dataPosts)
    return predictions_posts.select('probability')




def _predict_label(probab_stock, probab_posts):
    proba_stock = probab_stock.withColumn("prob_label_s", vector_to_array("probability")).select([col("prob_label_s")[i] for i in range(3)])
    proba_posts = probab_posts.withColumn("prob_label_p", vector_to_array("probability")).select([col("prob_label_p")[i] for i in range(3)])
    proba_stock = proba_stock.select("*").withColumn("id", monotonically_increasing_id())
    proba_stock.createOrReplaceTempView('proba_stock')
    proba_stock = spark.sql('select row_number() over (order by "id") as num, * from proba_stock')
    proba_posts = proba_posts.select("*").withColumn("id", monotonically_increasing_id())
    proba_posts.createOrReplaceTempView('proba_posts')
    proba_posts = spark.sql('select row_number() over (order by "id") as num, * from proba_posts')
#     return proba_posts, proba_stock
    proba_all = proba_stock.join(proba_posts,proba_stock["num"] == proba_posts["num"])
#     return proba_all
    proba_all = proba_all.withColumn("0", col("prob_label_s[0]") +  col("prob_label_p[0]"))
    proba_all = proba_all.withColumn("1", col("prob_label_s[1]") +  col("prob_label_p[1]"))
    proba_all = proba_all.withColumn("2", col("prob_label_s[2]") +  col("prob_label_p[2]"))
    proba_all_labels = proba_all.select("0", "1", "2")
#     return proba_all_labels
    schema=StructType([StructField('maxval',FloatType()),StructField('label',StringType())])

    maxcol = F.udf(lambda row: max(row,key=itemgetter(0)), schema)
    maxDF = proba_all_labels.withColumn('maxfield', maxcol(F.struct([F.struct(proba_all_labels[x],F.lit(x)) for x in proba_all_labels.columns]))).\
    select(proba_all_labels.columns+['maxfield.maxval','maxfield.label'])

    return maxDF.select('label')


post_sub_id = "projects/central-lock-371520/subscriptions/post-sub"


def _handle_message(message):
    data = message.data.decode('UTF-8') 
    csvStringIO = StringIO(data)
    df = pd.read_csv(csvStringIO, sep=",", header=None).sort_values(by=2)[1]
    
    # create schema
    fields = [StructField(f'{i}', FloatType(), True) for i in range(0, len(df.values))]
    schema = StructType(fields)
    # load data
    data = [[float(x) for x in df.values]]
    
    return spark.createDataFrame(data, schema)


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print('##START: Handling new batch of data...\n')
    try:
        
        # ----- POSTS -----
        posts_pd_df = pull_all_posts(post_sub_id)  
        dataPosts = _parse_post_dataset(posts_pd_df)        
        probab_posts = _predict_posts(dataPosts)  
           
        # ----- STOCK -----
        stock_pyspark_df = _handle_message(message)
        print("Number of stock records: ", len(stock_pyspark_df.columns))
        
        dataStock = _parse_stock_dataset(stock_pyspark_df)
        print(dataStock.show())
        probab_stock = _predict_stock(dataStock)  

        print(dataPosts.show())
        label_df = _predict_label(probab_stock, probab_posts)
#         time_stamp = datetime.now()
#         df = label_df.withColumn('key', lit(time_stamp))
        df = label_df.withColumn('key', lit("key"))
        
        df = df.withColumn('stock', lit(str(dataStock.toPandas()["features"][0])))
        df = df.withColumn('posts', lit(str(dataPosts.toPandas()["features"][0])))
        
        print(df.show())   

        # save in HBase
        catalog = get_catalog()
        write_to_hbase(df, catalog)
        print('## Row inserted to HBase\n')
    except Exception as e:
        print(str(e))
    finally:
        message.ack()
    print('### END')


def streaming_pull(
    project_id: str, subscription_id: str,
    callback: Callable[[pubsub_v1.subscriber.message.Message], None],
    timeout: Optional[float] = None
) -> None:
    """Receives messages from a pull subscription."""

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When timeout is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=timeout)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.


# Initialize Spark Session
spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('tesla_app') \
  .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

project_id = "central-lock-371520"
stock_sub_id = "stock-sub"
streaming_pull(project_id, stock_sub_id, callback)