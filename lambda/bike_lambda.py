import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    DB_CONFIG = {
        'dbname': os.environ['DB_NAME'].strip(),
        'user': os.environ['DB_USER'].strip(),
        'password': os.environ['DB_PASSWORD'],
        'host': os.environ['DB_HOST'].strip(),
        'port': os.environ['DB_PORT'].strip()
    }
    API_URL = "https://gbfs.prod.sharedmobility.ch/free_bike_status.json"
    
    conn = None
    cur = None
    
    try:
        logger.info(f"Attempting to connect to database at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        start_time = time.time()
        conn = psycopg2.connect(**DB_CONFIG)
        connect_time = time.time() - start_time
        logger.info(f"Database connection established in {connect_time:.2f} seconds")

        cur = conn.cursor()
        logger.info("Cursor created")

        # Create tables if they don't exist
        logger.info("Creating tables if they don't exist")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bike_status (
                bike_id VARCHAR(100),
                provider_id VARCHAR(100),
                lat FLOAT,
                lon FLOAT,
                is_reserved BOOLEAN,
                is_disabled BOOLEAN,
                timestamp TIMESTAMP,
                PRIMARY KEY (bike_id, timestamp)
            );
        """)
        conn.commit()
        logger.info("Tables created/verified")

        logger.info("Fetching data from GBFS API")
        start_time = time.time()
        response = requests.get(API_URL, timeout=10)
        api_time = time.time() - start_time
        logger.info(f"API request took {api_time:.2f} seconds")
        response.raise_for_status()
        bikes = response.json().get('data', {}).get('bikes', [])
        logger.info(f"Fetched data for {len(bikes)} bikes")

        # Prepare data for insertion
        current_time = datetime.now()
        bike_data = [(
            bike['bike_id'],
            bike['provider_id'],
            bike['lat'],
            bike['lon'],
            bike['is_reserved'],
            bike['is_disabled'],
            current_time
        ) for bike in bikes]

        # Insert data using execute_batch
        logger.info("Inserting data into database")
        execute_batch(cur, """
            INSERT INTO bike_status (
                bike_id, provider_id, lat, lon,
                is_reserved, is_disabled, timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, bike_data)
        
        conn.commit()
        logger.info(f"Successfully inserted {len(bikes)} records")

    except requests.RequestException as e:
        logger.error(f"API request error: {e}")
        raise
    except psycopg2.OperationalError as e:
        logger.error(f"Unable to connect to the database: {e}")
        raise
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logger.info("Database connection closed")

    total_time = time.time() - context.get_remaining_time_in_millis() / 1000
    logger.info(f"Lambda function executed in {total_time:.2f} seconds")

    return {
        'statusCode': 200,
        'body': f"Successfully processed {len(bikes)} bike records"
    }