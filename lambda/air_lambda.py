import requests
import psycopg2
from datetime import datetime
import logging
import json
import os
import time
import concurrent.futures

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get configuration from environment variables with defaults
API_TOKEN = os.environ.get('WAQI_API_TOKEN')
DB_CONFIG = {
    'dbname': os.environ.get('DB_NAME'),
    'user': os.environ.get('DB_USER'),
    'password': os.environ.get('DB_PASSWORD'),
    'host': os.environ.get('DB_HOST'),
    'port': os.environ.get('DB_PORT')
}

# Swiss cities
SWISS_CITIES = {
    "zurich": "Zurich", 
    "lucerne": "Lucerne",
    "geneva": "Geneva",
    "basel": "Basel",
    "bern": "Bern",
    "lausanne": "Lausanne"
}

def get_air_quality_data(city):
    """Fetch air quality data for a city"""
    try:
        response = requests.get(
            f"https://api.waqi.info/feed/{city}/?token={API_TOKEN}",
            timeout=2  # Further reduced timeout
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed for {city}: {str(e)}")
        raise

def fetch_city_data(city_tuple):
    """Helper function to fetch data for a single city"""
    city_key, city_name = city_tuple
    try:
        logger.info(f"Fetching data for {city_name}")
        data = get_air_quality_data(city_name)
        result = data['data']
        
        return (
            city_key,
            result['aqi'],
            result['iaqi'].get('t', {}).get('v'),
            result['iaqi'].get('h', {}).get('v'),
            result['iaqi'].get('pm25', {}).get('v')
        )
    except Exception as e:
        logger.error(f"Error processing {city_name}: {str(e)}")
        return None

def lambda_handler(event, context):
    """Main Lambda handler function"""
    conn = None
    cur = None
    results = []
    
    try:
        # Connect to database
        logger.info("Connecting to database")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Use ThreadPoolExecutor for parallel API requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all cities to the thread pool
            future_to_city = {executor.submit(fetch_city_data, city_item): city_item 
                            for city_item in SWISS_CITIES.items()}
            
            batch_results = []
            for future in concurrent.futures.as_completed(future_to_city):
                result = future.result()
                if result:
                    batch_results.append(result)
                
                # Insert in smaller batches to avoid long DB transactions
                if len(batch_results) >= 2:
                    cur.executemany("""
                        INSERT INTO air_quality 
                            (city, aqi, temperature, humidity, pm25)
                        VALUES 
                            (%s, %s, %s, %s, %s)
                    """, batch_results)
                    conn.commit()
                    
                    # Add to results
                    for data in batch_results:
                        results.append({
                            'city': data[0],
                            'aqi': data[1],
                            'temperature': data[2],
                            'humidity': data[3],
                            'pm25': data[4]
                        })
                    batch_results = []
            
            # Insert any remaining results
            if batch_results:
                cur.executemany("""
                    INSERT INTO air_quality 
                        (city, aqi, temperature, humidity, pm25)
                    VALUES 
                        (%s, %s, %s, %s, %s)
                """, batch_results)
                conn.commit()
                
                for data in batch_results:
                    results.append({
                        'city': data[0],
                        'aqi': data[1],
                        'temperature': data[2],
                        'humidity': data[3],
                        'pm25': data[4]
                    })
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Successfully recorded air quality data for {len(results)} cities',
                'cities_processed': results
            })
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        if conn:
            conn.rollback()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
