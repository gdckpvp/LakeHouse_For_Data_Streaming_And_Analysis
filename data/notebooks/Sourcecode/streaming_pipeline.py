### Initialize Spark Session
import logging
import pyspark
from pyspark.sql import SparkSession
from delta.tables import *

from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.udf import get_last_price_previous_day
from spark.etl import ingest_to_bronze, process_fact_stream
from spark.delta_manager import optimize_delta, vacuum_delta
from tbl_paths import BUCKET_PATH, TABLE_PATHS

from AutomateTable import create_spark_session_for_streaming
import time

spark = create_spark_session_for_streaming()
btc_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_btc')}"
eth_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_eth')}"
dimtime_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_time')}" 
dimdate_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_date')}"
dimcoin_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_coin')}"
dimcoin_stg_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dimcoin_stg')}"
fact_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('fact')}"

def optimize_vacuum_storage():
    optimize_delta(spark,btc_path)
    optimize_delta(spark,eth_path)
    optimize_delta(spark,fact_path)
    vacuum_delta(spark,btc_path)
    vacuum_delta(spark,eth_path)
    vacuum_delta(spark,fact_path)

def start_streaming():
    df_coin = spark.read.format('delta').load(dimcoin_stg_path)
    df_coin_broadcast = broadcast(df_coin)

    bitcoin_batch_df = spark.read.format('delta').load(btc_path)
    ethereum_batch_df = spark.read.format('delta').load(eth_path)

    last_ethereum = get_last_price_previous_day(ethereum_batch_df)
    last_bitcoin = get_last_price_previous_day(bitcoin_batch_df)

    bitcoin_df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'broker:29092').option("startingOffsets", "latest").option('subscribe', 'bitcoin').load()
    ethereum_df = spark.readStream.format('kafka').option('kafka.bootstrap.servers', 'broker:29092').option("startingOffsets", "latest").option('subscribe', 'ethereum').load()

    bronze_btc = ingest_to_bronze(bitcoin_df, 'bitcoin')
    bronze_eth = ingest_to_bronze(ethereum_df, 'ethereum')

    bitcoin_bronze_df = spark.readStream.format('delta').option("startingOffsets", "latest").load(btc_path)
    ethereum_bronze_df = spark.readStream.format('delta').option("startingOffsets", "latest").load(eth_path)

    fact_btc = process_fact_stream(bitcoin_bronze_df, 'bitcoin', last_bitcoin, df_coin_broadcast,fact_path)
    fact_eth = process_fact_stream(ethereum_bronze_df, 'ethereum', last_ethereum, df_coin_broadcast,fact_path)

    bronze_btc.awaitTermination()
    bronze_eth.awaitTermination()
    fact_btc.awaitTermination()
    fact_eth.awaitTermination()

if __name__ == "__main__":
    #optimize_vacuum_storage()
    start_streaming()