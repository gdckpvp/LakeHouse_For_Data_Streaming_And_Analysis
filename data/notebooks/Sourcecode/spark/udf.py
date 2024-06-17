from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
from pyspark.sql.utils import AnalysisException
import sys

def get_last_price_previous_day(df):
    """
    Function to get the last price of the previous day from a DataFrame.

    Parameters:
    df (DataFrame): The Spark DataFrame with 'bitcoin' and 'timestamp' columns.

    Returns:
    float or None: The last price of Bitcoin from the previous day, or None if no data is available.
    """
    # Convert 'timestamp' to 'date'
    df = df.withColumn("date", to_date(col("timestamp")))

    # Define the window specification
    windowSpec = Window.partitionBy("date").orderBy(col("timestamp").desc())

    # Add a row number for each row within each date partition
    df = df.withColumn("row_num", row_number().over(windowSpec))

    # Filter for the previous day and the last row of the day (row_num = 1)
    previous_day = (datetime.now() - timedelta(1)).date()
    df_filtered = df.filter((col("date") == lit(previous_day)) & (col("row_num") == 1))

    # Get the last price if available
    last_price_rows = df_filtered.select(df.columns[0]).collect()
    if last_price_rows:
        last_price_previous_day = last_price_rows[0][0]
    else:
        last_price_previous_day = 0

    return last_price_previous_day


def get_date_id(ts):
    return year(ts)*10000 + month(ts) * 100 + day(ts)
    
def append_to_delta(microBatchDf: DataFrame, batchId: int,  path: str):
    """
    Appends a micro-batch DataFrame to a Delta table based on the coin type.

    Parameters:
    microBatchDf (DataFrame): The DataFrame to append.
    batchId (int): The batch ID of the streaming query.
    coin_type (str): The type of cryptocurrency, 'bitcoin' or 'ethereum'.

    Returns:
    None
    """
    if microBatchDf:       
        try:
            microBatchDf.write.format('delta').mode('append').save(path)
            pass
        except AnalysisException as e:
            print(f'AnalysisException: {e}')
        except Exception as e:
            print(f'Unexpected exception: {e}')


def append_to_fact(spark, microBatchDf: DataFrame, batchId: int,  path: str):
    try:
        microBatchDf.persist()
        if batchId % 60 == 0:
            optimize_delta(spark,path)
        if batchIid % 101 == 0:
            zorder_delta(spark,path)
        microBatchDf.write.format('delta').mode('append').save(path)
    except Exception as e:
        raise(e)
            
        