import logging
from pyspark.sql.window import Window
from utils import get_coins_from_api
from delta.tables import *
from pyspark.sql.functions import row_number, col, lit, sha2, concat_ws, to_date, current_timestamp

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("Spark-Streaming")

def populate_time_dimension(spark, dimtime_path):
    try:
        spark.sql(f"select explode(sequence(to_timestamp('1900-01-01 00:00:00'), to_timestamp('1900-01-01 23:59:59'), interval 1 second)) as t") \
            .createOrReplaceTempView('time')

        dim_time = spark.sql("""
            SELECT
                CAST(date_format(t, 'HHmmss') AS INT) AS Id,
                date_format(t, 'hh:mm:ss a') AS Time,
                date_format(t, 'hh') AS Hour,
                date_format(t, 'mm') AS Minute,
                date_format(t, 'ss') AS Second,
                date_format(t, 'HH:mm:ss') AS Time24,
                date_format(t, 'kk') AS Hour24,
                date_format(t, 'a') AS AmPm
            FROM time
            """)
        dim_time.write.format("delta").mode("overwrite").save(dimtime_path)
        logger.info("Created dimtime table successfully")
    except Exception as e:
        logger.error(f"Failed to create dimtime table: {e}")

def populate_date_dimension(spark, dimdate_path):
    try:
        begin_date = '2020-01-01'
        end_date = '2030-12-31'
        spark.sql(f"select explode(sequence(to_date('{begin_date}'), to_date('{end_date}'), interval 1 day)) as calendarDate") \
            .createOrReplaceTempView('dates')

        dim_date = spark.sql("""
            SELECT
                year(calendarDate) * 10000 + month(calendarDate) * 100 + day(calendarDate) AS id,
                CalendarDate,
                year(calendarDate) AS CalendarYear,
                date_format(calendarDate, 'MMMM') AS CalendarMonth,
                month(calendarDate) AS MonthOfYear,
                date_format(calendarDate, 'EEEE') AS CalendarDay,
                dayofweek(calendarDate) AS DayOfWeek,
                dayofmonth(calendarDate) AS DayOfMonth,
                dayofyear(calendarDate) AS DayOfYear,
                weekofyear(calendarDate) AS WeekOfYearIso
            FROM dates
            ORDER BY CalendarDate
            """)

        dim_date.write.mode("overwrite").format("delta").save(dimdate_path)
        logger.info("Created dimdate table successfully")
    except Exception as e:
        logger.error(f"Failed to create dimdate table: {e}")

def initialize_dimcoin_staging(spark, dimcoin_stg_path):
    try:
        dimcoin_stg = spark.read.format('delta').load(dimcoin_stg_path) #dataframe with exsiting ID
        coins_api_res = get_coins_from_api()
        coin_df = spark.createDataFrame(coins_api_res)
        columns = [ "coin_id","symbol","name","supply","maxSupply","volume24h"]
        max_existing_id = dimcoin_stg.toDF(*columns).agg({"coin_id":"max"}).collect()[0][0]

        #coin_df = coin_df.withColumn("coin_id", lit(0)) # coin_id column used for merge with existing staging table
        # Merge the updates into the existing Delta table
        DeltaTable.forPath(spark, dimcoin_stg_path).alias("dimcoin") \
            .merge(coin_df.alias("updates"), condition = "dimcoin.name = updates.name AND dimcoin.symbol = updates.symbol" ) \
            .whenMatchedUpdate(set={
                "supply": "updates.supply",
                "maxSupply": "updates.maxSupply",
                "volume24h":"updates.volume24h"
            })\
            .execute()
        logger.info("Upsert existing Coin successfully")
        
        #capture new coming coins
        condition = ['name','symbol']
        coin_df = coin_df.drop("coin_id") #dont need column coin_id here 
        rows_to_update = coin_df \
            .alias("source") \
            .join(dimcoin_stg.alias("target"), condition, 'leftanti') \
            .select('source.name', 'source.symbol', 'source.supply', 'source.maxSupply', 'source.volume24h') \
            .orderBy(col('source.name'))
        
        #assign IDs for new coming Coin that got from API 
        window_spec = Window.orderBy("name")
        new_df_with_row_num = rows_to_update.withColumn("row_number",row_number().over(window_spec) )
        new_df_with_incremented_id = new_df_with_row_num.withColumn("coin_id", col("row_number") + max_existing_id)
        new_df_with_incremented_id = new_df_with_incremented_id.drop("row_number").select("coin_id","symbol","name","supply","maxSupply","volume24h")
        new_df_with_incremented_id.write.format('delta').mode('append').save(dimcoin_stg_path)
        logger.info("Insert new upcoming coins completed successfully!")
    except Exception as e:
        logger.error("An error occurred during the upsert process: %s", e)
        logger.info("Create DimCoin staging")
        coins_api_res = get_coins_from_api()
        coin_df = spark.createDataFrame(coins_api_res)
        coin_df_sorted = coin_df.orderBy("name")
        # Define a window specification
        window_spec = Window.orderBy("name")
        # Add 'ID' column with consecutive values
        df_coin_with_id = coin_df_sorted.withColumn("coin_id", row_number().over(window_spec))
        df_coin_with_id = df_coin_with_id.select("coin_id","symbol","name","supply","maxSupply","volume24h")
        df_coin_with_id.write.format('delta').mode('overwrite').save("s3a://test/bronze/dimcoin_stg")
        logger.info("Created successfully")


def create_dimcoin(spark,dimcoin_stg_path):
    df = spark.read.format('delta').load(dimcoin_stg_path)
    #add hash column that supports for generate unique surrogate keys
    hash_cols = ['supply', 'maxSupply', 'volume24h']
    df = df.withColumn("hash", lit(sha2(concat_ws("~", *hash_cols), 256)))
    
    keys = ['coin_id','hash']
    # Build the dimension surrogate key
    w = Window().orderBy(*keys)
    df = df.withColumn("surrogate_key", row_number().over(w))
    df = df.withColumn("surrogate_key",col("surrogate_key").cast('long'))\
            .withColumn("supply", col("supply").cast("double"))\
            .withColumn("maxSupply", col("maxSupply").cast("double"))\
            .withColumn("volume24h", col("volume24h").cast("double"))\
            .withColumn("start_date", lit(to_date(current_timestamp())))\
            .withColumn("end_date", lit(to_date(lit("9999-12-31"))))\
            .withColumn("is_current", lit("Y"))
    return df.select("surrogate_key", "coin_id","symbol","name","supply","maxSupply","volume24h","hash","start_date","end_date","is_current")

def upsert_scd_dimcoin(spark,dimcoin_path,dimcoin_stg_path):
    try:
        columns = ["surrogate_key", "coin_id","symbol","name","supply","maxSupply","volume24h","hash","start_date","end_date","is_current"]
        dimcoin_silver = spark.read.format('delta').load(dimcoin_path) #existing data in dimcoin
        dimcoin_stg_surrogatekey = create_dimcoin(spark,dimcoin_stg_path) #new upcomming coins data
        
        if dimcoin_silver.head(1): #check if delta table has rows 
            condition = ['coin_id', 'hash', 'is_current']
            rows_to_update = dimcoin_stg_surrogatekey \
                .alias("source") \
                .where("is_current = 'Y'") \
                .join(dimcoin_silver.alias("target"),condition,'leftanti') \
                .select(*columns) \
                .orderBy(col('source.coin_id')) 

            # Retrieve maximum surrogate key in dim coin 
            max_table_key = dimcoin_silver.toDF(*columns).agg({"surrogate_key":"max"}).collect()[0][0]

            # Increment surrogate key in stage table by maxTableKey
            rows_to_update = rows_to_update.withColumn("surrogate_key", col("surrogate_key") + max_table_key)

            # Merge statement to expire old records
            DeltaTable.forPath(spark, dimcoin_path).alias("original").merge(
                source = rows_to_update
                .alias("updates"),
                condition = 'original.coin_id = updates.coin_id'
            ).whenMatchedUpdate(
                condition = "original.is_current = 'Y'  AND original.hash <> updates.hash",
                set = {                                      
                "is_current": "'N'",
                "end_date": lit(to_date(current_timestamp()))
                }
            ).execute()
            logger.info("Upsert DimCoin successfully")
            rows_to_update.select(*columns).write.mode("append").format("delta").save(dimcoin_path)
            logger.info("Load new upcoming coin data successfully")
        else:
            dimcoin_stg_surrogatekey.write.format('delta').mode('overwrite').save(dimcoin_path)
            logger.info("Load data to DimCoin successfully")
    except Exception as e:
        logger.error(f"Fall back to upsert because of {e}")


