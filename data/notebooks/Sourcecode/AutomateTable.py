## libraries for creating spark session
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *

## libraries for connecting to functions in spark folder
from spark.delta_manager import optimize_delta, vacuum_delta
from utils import get_coins_from_api
from tbl_paths import BUCKET_PATH, TABLE_PATHS
from spark.populate_dim import upsert_scd_dimcoin,initialize_dimcoin_staging
from spark.meta_data import check_minio_bucket, create_hive_table, check_existing_timedata
import logging # Use for returning log information

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("Spark-Streaming")

btc_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_btc')}"
eth_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('bronze_eth')}"
dimtime_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_time')}" 
dimdate_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_date')}"
dimcoin_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_coin')}"
dimcoin_stg_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dimcoin_stg')}"
fact_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('fact')}"
#################### Create Spark Session ####################

def base_spark_config(is_streaming=False):
    # S3 configuration
    S3_ENDPOINT = "minio:9000"
    S3_ACCESS_KEY = "minioadmin"
    S3_SECRET_KEY = "minioadmin"

    conf = pyspark.SparkConf().setMaster("local[2]")
    conf.set("spark.jars.packages", 'org.apache.hadoop:hadoop-aws:3.3.1,io.delta:delta-spark_2.12:3.1.0')\
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')\
    .set('spark.hadoop.fs.s3a.endpoint', S3_ENDPOINT)\
    .set('spark.hadoop.fs.s3a.access.key', S3_ACCESS_KEY)\
    .set('spark.hadoop.fs.s3a.secret.key', S3_SECRET_KEY)\
    .set('spark.hadoop.fs.s3a.path.style.access', "true")\
    .set('hive.metastore.uris', "thrift://metastore:9083")\
    .set("spark.executor.memory", "2g")\
    .set("spark.executor.cores", "2")\
    .set("spark.cores.max", "10")\
    .set("spark.driver.memory", "2g")\

    if is_streaming:
        conf.set("spark.jars.packages",conf.get("spark.jars.packages") + ",org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")\
        .set("spark.sql.shuffle.partitions", 5)\
        .set("spark.default.parallelism", 2)\
        .set("spark.scheduler.allocation.file", "./fairscheduler.xml")\
        .set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')\
        .set("spark.databricks.delta.optimizeWrite.enabled", 'true')\
        .set("spark.scheduler.mode","FAIR")\
        
    return conf

def create_spark_session(conf, app_name):
    try:
        # This cell may take some time to run the first time, as it must download the necessary spark jars
        spark = pyspark.sql.SparkSession.builder\
        .appName(app_name)\
        .config(conf=conf)\
        .enableHiveSupport()\
        .getOrCreate()
        spark.sparkContext.setLocalProperty("spark.scheduler.pool", "mypool")
        spark.sparkContext.setLogLevel("ERROR")
        logging.info('Spark session initialized successfully')
        return spark
    except Exception as e:
        logging.error(f"Spark session initialization failed. Error: {e}")

def create_spark_session_for_streaming() -> SparkSession:
    conf = base_spark_config(is_streaming=True)
    spark = create_spark_session(conf, "Spark Streaming")
    return spark

def create_spark_session_for_automate_table():
    conf = base_spark_config()
    spark = create_spark_session(conf, "Spark Automate Table")
    return spark

def optimize_vacuum_storage():
    optimize_delta(spark,btc_path)
    optimize_delta(spark,eth_path)
    optimize_delta(spark,fact_path)
    vacuum_delta(spark,btc_path)
    vacuum_delta(spark,eth_path)
    vacuum_delta(spark,fact_path)

if __name__ == "__main__":
    check_minio_bucket()
    create_hive_table()
    spark = create_spark_session_for_automate_table()
    check_existing_timedata(spark)
    initialize_dimcoin_staging(spark, dimcoin_stg_path)
    upsert_scd_dimcoin(spark, dimcoin_path, dimcoin_stg_path)
    optimize_vacuum_storage()