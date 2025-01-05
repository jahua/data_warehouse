import json
import requests
import datetime
import psycopg2
import os
import logging
from typing import Dict

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
CITIES = {
    "Zurich": {"lat": "47.3769", "lon": "8.5417"},
    "Lucerne": {"lat": "47.0502", "lon": "8.3093"},
    "Geneva": {"lat": "46.2044", "lon": "6.1432"},
    "Basel": {"lat": "47.5596", "lon": "7.5886"},
    "Bern": {"lat": "46.9480", "lon": "7.4474"},
    "Lausanne": {"lat": "46.5197", "lon": "6.6323"}
}
API_KEY = "bf2154a741c8ecbd57ff922c25bc92c8"

def get_source_connection():
    """Get connection to source database"""
    return psycopg2.connect(
        dbname=os.environ.get('SOURCE_DB_NAME'),
        user=os.environ.get('SOURCE_DB_USER'),
        password=os.environ.get('SOURCE_DB_PASSWORD'),
        host=os.environ.get('SOURCE_DB_HOST'),
        port=os.environ.get('SOURCE_DB_PORT')
    )

def create_table_if_not_exists(cur):
    """Create weather_data table if it doesn't exist"""
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(50),
                aqi INTEGER,
                temperature DOUBLE PRECISION,
                humidity INTEGER,
                pm25 INTEGER,
                timestamp TIMESTAMP
            );
            CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp 
            ON weather_data(city, timestamp);
        """)
        logger.info("Table checked/created successfully")
    except psycopg2.Error as e:
        logger.error(f"Error creating table: {str(e)}")
        raise

def fetch_current_weather(city: str, lat: str, lon: str) -> Dict:
    """Fetch current weather data from OpenWeather API"""
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&units=metric&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching weather data for {city}: {str(e)}")
        return None

def fetch_air_quality(city: str, lat: str, lon: str) -> Dict:
    """Fetch air quality data from OpenWeather API"""
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Error fetching air quality data for {city}: {str(e)}")
        return None

def insert_weather_data(cur, city: str, weather_data: Dict, air_quality_data: Dict):
    """Insert weather and air quality data into database"""
    try:
        cur.execute("""
            INSERT INTO public.weather_data (
                city,
                aqi,
                temperature,
                humidity,
                pm25,
                timestamp
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            city,
            air_quality_data.get('list', [{}])[0].get('main', {}).get('aqi') if air_quality_data else None,
            weather_data.get('main', {}).get('temp'),
            weather_data.get('main', {}).get('humidity'),
            air_quality_data.get('list', [{}])[0].get('components', {}).get('pm2_5') if air_quality_data else None,
            datetime.datetime.now()
        ))
        logger.info(f"Weather data inserted successfully for {city}")
    except Exception as e:
        logger.error(f"Error inserting weather data for {city}: {str(e)}")
        raise

def lambda_handler(event, context):
    """Main Lambda handler function"""
    successful_cities = []
    failed_cities = []

    try:
        # Check if it's around 11:50
        current_time = datetime.datetime.now()
        if current_time.hour != 11 or current_time.minute < 45 or current_time.minute > 55:
            logger.info("Not within execution window (11:45-11:55). Skipping execution.")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Outside execution window',
                    'current_time': current_time.isoformat()
                })
            }

        # Connect to database
        conn = get_source_connection()
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # Create table if not exists
        create_table_if_not_exists(cur)

        # Process each city
        for city, coords in CITIES.items():
            try:
                logger.info(f"Processing {city}")
                
                # Get current weather
                weather_data = fetch_current_weather(city, coords['lat'], coords['lon'])
                if not weather_data:
                    logger.error(f"Failed to fetch weather data for {city}")
                    failed_cities.append(city)
                    continue

                # Get air quality
                air_quality_data = fetch_air_quality(city, coords['lat'], coords['lon'])
                
                # Insert data
                insert_weather_data(cur, city, weather_data, air_quality_data)
                successful_cities.append(city)
                
            except Exception as e:
                logger.error(f"Error processing {city}: {str(e)}")
                failed_cities.append(city)

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Weather data collection completed',
                'successful_cities': successful_cities,
                'failed_cities': failed_cities,
                'timestamp': datetime.datetime.now().isoformat()
            })
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'successful_cities': successful_cities,
                'failed_cities': failed_cities
            })
        }
        
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
            logger.info("Database connections closed")

if __name__ == "__main__":
    # For local testing
    lambda_handler(None, None)