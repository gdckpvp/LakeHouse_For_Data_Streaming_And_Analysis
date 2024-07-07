## libraries for connecting to Minio and Trino
import sys
from minio import Minio
from trino.dbapi import connect, Error
import time
from spark.populate_dim import populate_time_dimension,populate_date_dimension
from tbl_paths import BUCKET_PATH, TABLE_PATHS
import logging # Use for returning log information
# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')
logger = logging.getLogger("Spark-Streaming")

dimtime_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_time')}" 
dimdate_path = f"{BUCKET_PATH}/{TABLE_PATHS.get('dim_date')}"

#################### Create Minio test bucket ####################

def create_bucket(minioClient, bucket_name):
    try:
        minioClient.make_bucket(bucket_name)
        create_hive_table()
    except Exception as e:
        logger.error(f"Error creating bucket: {e}")
        sys.exit(1)


def check_minio_bucket():
    bucket_name = 'test'
    minio_client = Minio('minio:9000',
                        access_key='minioadmin',
                        secret_key = 'minioadmin', secure=False)

    if minio_client.bucket_exists(bucket_name):
        logger.info("Bucket {} already exists".format(bucket_name))
        create_hive_table()
    else:
        create_bucket(minio_client, bucket_name)
        logger.info("Bucket {} created successfully".format(bucket_name))

###################### Query Trino ######################
def create_hive_table():
    # time.sleep(70)
    conn = connect(host="trino",
                    port=8080,
                    user="airflow")
    cur = conn.cursor()
    
    query = []
    # get line by line from file ./query.sql and append to query list
    with open('./query.sql', 'r') as file:
        for line in file:
            # remove newline character
            line = line.replace('\n', '')
            query.append(line)
    
    #Query to create tables    
    while True:
        try:
            for line in query:
                cur.execute(line)
            logger.info("Create hive table completed")
            break
        except Exception as e:
            logger.error(f"Error creating hive table: {e}, system will retry in 10 seconds")
            time.sleep(10)  # Wait for 60 seconds before retrying

############### Check if there is no data ################
def check_existing_timedata(spark):
    try:
        conn = connect(
            host="trino",
            port=8080,
            user="airflow",
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM delta.silver.dimtime")
        result = cur.fetchall()
        if result[0][0] == 0:
            populate_time_dimension(spark, dimtime_path)
            populate_date_dimension(spark, dimdate_path)
        else:
            logger.info("Data already exists")
    except Error as trino_error:
        logger.error(f"Trino database error: {trino_error}")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")