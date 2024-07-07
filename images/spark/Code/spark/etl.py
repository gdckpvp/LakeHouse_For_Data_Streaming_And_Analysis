from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
from spark.udf import append_to_delta, get_date_id
import sys
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("Spark-Streaming")

def transform_bronze_tables(df: DataFrame, coin: str):
    try:
        schema = StructType([
        StructField(coin, StringType()),
        StructField('timestamp', StringType()) 
        ])
        transformed_df = df.selectExpr("CAST(decode(value, 'UTF-8') AS STRING) AS decoded_value") \
            .select(from_json(col("decoded_value"), schema).alias("data")) \
            .select("data.*") \
            .withColumn(coin, col(coin).cast("double")) \
            .withColumn('timestamp', from_unixtime(col('timestamp').cast('double'))) \
            .withColumn('timestamp', expr("from_utc_timestamp(timestamp, 'Asia/Ho_Chi_Minh')"))
        return transformed_df
    except Exception as e:
        print(f'{e}')

def ingest_to_bronze(df, coin_name):
    try:
        path = f's3a://test/bronze/{coin_name}_stg'
        #transformed_df = transform_bronze_tables (df, coin_name)
        average_df = df.transform(lambda df : transform_bronze_tables(df, coin_name)) \
                            .withWatermark("timestamp", "1 minute") \
                            .groupBy(window("timestamp", "1 minute")) \
                            .agg(
                                  avg(coin_name).alias("average_1minute"),
                                  last(coin_name).alias(coin_name),
                                  last("timestamp").alias("timestamp")
                              )
        # Start the streaming query to continuously update results
        query = average_df.select(col(coin_name), col("timestamp"), col('average_1minute'))\
            .writeStream.outputMode("update")\
            .foreachBatch(lambda microBatchDf, batchId: append_to_delta(microBatchDf, batchId, path))\
            .start()
        logger.info(f'Start streaming {coin_name} to bronze')
        return query
    except Exception as e:
        logger.info(f'{e}')


def process_fact_stream(df_bronze, coin_name, last_price, df_broadcast, fact_path):
    try:
        fact = df_bronze\
            .withWatermark("timestamp", "1 minute")\
            .withColumn('date_id', get_date_id(df_bronze['timestamp']))\
            .withColumn('time_id', date_format(col('timestamp'), 'HHmmss').cast(IntegerType()))\
            .join(df_broadcast, df_bronze.columns[0] == df_broadcast['name'], 'left')\
            .withColumn('market_cap', col(coin_name) * col('supply'))\
            .withColumn('change_percent_last_day', (col(coin_name) - last_price) / last_price)\
            .withColumn('created_at', date_format(current_timestamp(), 'HH:mm:ss'))\
            .select(
                col('coin_id').alias('coin_id'),
                col('date_id'),
                col('time_id'),
                col(coin_name).alias('price'),
                col('market_cap'),
                col('change_percent_last_day'),
                col('average_1minute'),
                col('created_at')
            ).coalesce(2)
        
        logger.info(f'Start process {coin_name} stream')
        
        return fact.writeStream.format('delta')\
            .outputMode('append')\
            .partitionBy('coin_id')\
            .option('checkpointLocation', f'./delta3/eventFact_{coin_name}/checkpoints_')\
            .trigger(processingTime='1 second')\
            .start(fact_path)
    except Exception as e:
        logger.error(f"Process streams failed. Error: {e}")