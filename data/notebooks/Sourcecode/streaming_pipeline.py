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
import threading
import signal

spark = create_spark_session_for_streaming()
btc_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_btc')}"
eth_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_eth')}"
dimtime_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_time')}" 
dimdate_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_date')}"
dimcoin_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_coin')}"
dimcoin_stg_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dimcoin_stg')}"
fact_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('fact')}"


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

    bitcoin_bronze_df = spark.readStream.format('delta').option("withEventTimeOrder", "true").load(btc_path).withWatermark("timestamp","2 minutes")
    ethereum_bronze_df = spark.readStream.format('delta').option("withEventTimeOrder", "true").load(eth_path).withWatermark("timestamp","2 minutes")
    
    fact_btc = process_fact_stream(bitcoin_bronze_df, 'bitcoin', last_bitcoin, df_coin_broadcast,fact_path)
    fact_eth = process_fact_stream(ethereum_bronze_df, 'ethereum', last_ethereum, df_coin_broadcast,fact_path)    
    # fact_btc = process_fact_stream(bitcoin_bronze_df, 'bitcoin', last_bitcoin,fact_path)
    # fact_eth = process_fact_stream(ethereum_bronze_df, 'ethereum', last_ethereum,fact_path)
    def run_stream(stream):
        stream.awaitTermination()

    threading.Thread(target=run_stream, args=(fact_btc,), daemon=True).start()
    threading.Thread(target=run_stream, args=(fact_eth,), daemon=True).start()
    threading.Thread(target=run_stream, args=(bronze_btc,), daemon=True).start()
    threading.Thread(target=run_stream, args=(bronze_eth,), daemon=True).start()

    
    stop_event = threading.Event()

    def on_exit(signum, frame):
        print("Received exit signal")
        stop_event.set()

    
    signal.signal(signal.SIGINT, on_exit)
    signal.signal(signal.SIGTERM, on_exit)

    print("Main thread is running. Press Ctrl+C to exit.")
    stop_event.wait()

    print("Main thread ends")

if __name__ == "__main__":
    start_streaming()