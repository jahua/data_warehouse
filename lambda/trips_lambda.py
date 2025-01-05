import os
import psycopg2
import psycopg2.extras
import logging
from datetime import datetime, timedelta
import pytz
from contextlib import contextmanager
from typing import Optional, Dict
from psycopg2.extensions import connection

# Configure logging for AWS Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Source database configuration from environment variables
SOURCE_DB_CONFIG = {
    'dbname': os.environ['SOURCE_DB_NAME'].strip(),
    'user': os.environ['SOURCE_DB_USER'].strip(),
    'password': os.environ['SOURCE_DB_PASSWORD'],
    'host': os.environ['SOURCE_DB_HOST'].strip(),
    'port': os.environ['SOURCE_DB_PORT'].strip(),
    'connect_timeout': 300
}

# Destination database configuration from environment variables
DEST_DB_CONFIG = {
    'dbname': os.environ['DEST_DB_NAME'].strip(),
    'user': os.environ['DEST_DB_USER'].strip(),
    'password': os.environ['DEST_DB_PASSWORD'],
    'host': os.environ['DEST_DB_HOST'].strip(),
    'port': os.environ['DEST_DB_PORT'].strip(),
    'connect_timeout': 300
}

@contextmanager
def database_connection(config: Dict, conn: Optional[connection] = None):
    should_close = conn is None
    try:
        if conn is None:
            conn = psycopg2.connect(**config)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        if should_close and conn:
            conn.close()

def create_destination_table(dest_conn):
    """Create the destination table if it doesn't exist"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS all_trips (
        trip_id integer NOT NULL DEFAULT nextval('all_trips_trip_id_seq'::regclass),
        bike_id character varying(255),
        provider_id character varying(255),
        trip_start timestamp without time zone,
        trip_end timestamp without time zone,
        start_lat double precision,
        start_lon double precision,
        end_lat double precision,
        end_lon double precision,
        total_duration double precision,
        total_distance double precision,
        segment_count integer,
        municipality character varying(255),
        canton character varying(255),
        vehicle_type character varying(50),
        city character varying(255),
        temperature double precision,
        humidity double precision,
        aqi double precision,
        pm25 double precision,
        CONSTRAINT all_trips_pkey PRIMARY KEY (trip_id),
        CONSTRAINT unique_trip UNIQUE (bike_id, trip_start)
    );

    -- Create sequence if it doesn't exist
    CREATE SEQUENCE IF NOT EXISTS all_trips_trip_id_seq;

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_all_trips_bike_id ON all_trips (bike_id);
    CREATE INDEX IF NOT EXISTS idx_all_trips_provider_id ON all_trips (provider_id);
    CREATE INDEX IF NOT EXISTS idx_all_trips_trip_start ON all_trips (trip_start);
    CREATE INDEX IF NOT EXISTS idx_all_trips_municipality ON all_trips (municipality);
    """
    with dest_conn.cursor() as cursor:
        cursor.execute(create_table_query)
    dest_conn.commit()
    logger.info("Destination table created or verified successfully")

def extract_and_load_data(source_conn, dest_conn, start_date: datetime, end_date: datetime):
    """Extract data from source and load into destination"""
    
    extract_query = """
    WITH trip_data AS (
        SELECT 
            bike_id,
            provider_id,
            timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Zurich' as local_time,
            lat,
            lon,
            LAG(timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Zurich') OVER (PARTITION BY bike_id ORDER BY timestamp) as prev_time,
            LAG(lat) OVER (PARTITION BY bike_id ORDER BY timestamp) as prev_lat,
            LAG(lon) OVER (PARTITION BY bike_id ORDER BY timestamp) as prev_lon
        FROM bike_status
        WHERE timestamp >= %s::timestamp AND timestamp < %s::timestamp
    ),
    trip_segments AS (
        SELECT 
            bike_id,
            provider_id,
            local_time as end_time,
            prev_time as start_time,
            prev_lat as start_lat,
            prev_lon as start_lon,
            lat as end_lat,
            lon as end_lon,
            EXTRACT(EPOCH FROM (local_time - prev_time)) / 60.0 as duration_minutes,
            6371 * 2 * asin(
                sqrt(
                    power(sin(radians(lat - prev_lat) / 2), 2) +
                    cos(radians(prev_lat)) * cos(radians(lat)) *
                    power(sin(radians(lon - prev_lon) / 2), 2)
                )
            ) as distance_km
        FROM trip_data
        WHERE prev_time IS NOT NULL
            AND EXTRACT(EPOCH FROM (local_time - prev_time)) BETWEEN 60 AND 3600
            AND (prev_lat != lat OR prev_lon != lon)
    ),
    trip_aggregates AS (
        SELECT 
            bike_id,
            provider_id,
            MIN(start_time) as trip_start,
            MAX(end_time) as trip_end,
            MIN(start_lat) as start_lat,
            MIN(start_lon) as start_lon,
            MAX(end_lat) as end_lat,
            MAX(end_lon) as end_lon,
            SUM(duration_minutes) as total_duration,
            SUM(distance_km) as total_distance,
            COUNT(*) as segment_count
        FROM trip_segments
        GROUP BY bike_id, provider_id
    )
    SELECT 
        bike_id,
        provider_id,
        trip_start,
        trip_end,
        start_lat,
        start_lon,
        end_lat,
        end_lon,
        total_duration,
        total_distance,
        segment_count
    FROM trip_aggregates
    WHERE total_duration >= 1 AND total_duration <= 60
        AND total_distance > 0
        AND segment_count >= 2
    ORDER BY trip_start;
    """
    
    try:
        # Extract data
        with source_conn.cursor() as source_cursor:
            source_cursor.execute(extract_query, (start_date, end_date))
            records = source_cursor.fetchall()
            columns = [desc[0] for desc in source_cursor.description]
            
        if not records:
            logger.info("No new data to process")
            return
            
        logger.info(f"Extracted {len(records)} records from source database")
            
        # Load data
        insert_query = """
        INSERT INTO all_trips (
            bike_id, provider_id, trip_start, trip_end,
            start_lat, start_lon, end_lat, end_lon,
            total_duration, total_distance, segment_count
        ) VALUES %s
        ON CONFLICT (bike_id, trip_start) 
        DO UPDATE SET
            provider_id = EXCLUDED.provider_id,
            trip_end = EXCLUDED.trip_end,
            start_lat = EXCLUDED.start_lat,
            start_lon = EXCLUDED.start_lon,
            end_lat = EXCLUDED.end_lat,
            end_lon = EXCLUDED.end_lon,
            total_duration = EXCLUDED.total_duration,
            total_distance = EXCLUDED.total_distance,
            segment_count = EXCLUDED.segment_count
        """
        
        template = ','.join(['%s'] * len(columns))
        with dest_conn.cursor() as dest_cursor:
            psycopg2.extras.execute_values(
                dest_cursor,
                insert_query,
                records,
                template=f"({template})"
            )
        
        dest_conn.commit()
        logger.info(f"Successfully loaded {len(records)} records into destination database")
        
    except Exception as e:
        logger.error(f"Error in extract_and_load_data: {str(e)}")
        raise

def lambda_handler(event, context):
    """AWS Lambda handler function"""
    try:
        # Calculate time range for data extraction (last 24 hours)
        zurich_tz = pytz.timezone('Europe/Zurich')
        end_date = zurich_tz.localize(datetime.now())
        start_date = end_date - timedelta(days=1)
        
        logger.info(f"Processing data from {start_date} to {end_date}")
        
        # Connect to source and destination databases
        with database_connection(SOURCE_DB_CONFIG) as source_conn, \
             database_connection(DEST_DB_CONFIG) as dest_conn:
            
            # Ensure destination table exists
            create_destination_table(dest_conn)
            
            # Extract and load data
            extract_and_load_data(source_conn, dest_conn, start_date, end_date)
        
        return {
            'statusCode': 200,
            'body': 'Data warehouse update completed successfully'
        }
        
    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

# For local testing
if __name__ == "__main__":
    lambda_handler(None, None)