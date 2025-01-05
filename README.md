# Carbon Footprint Analysis of Swiss Shared Mobility

This repository contains the implementation of a data warehouse solution for analyzing carbon footprint patterns of shared mobility services across Swiss municipalities. The project is part of the Data Warehouse Lab course at Lucerne University of Applied Sciences and Arts (HSLU).

## Project Structure

```
├── lambda_data_lake/
│   ├── lambda_bike_status.py        # Shared bike status data collection
│   ├── lambda_weather.py            # Weather data collection
│   ├── lambda_air_quality.py        # Air quality metrics collection
│   └── lambda_municipality.py       # Swiss municipality boundary data
├── lambda_data_warehouse/
│   ├── lambda_etl_main.py          # Main ETL process
│   ├── lambda_carbon_calc.py       # Carbon savings calculations
│   └── lambda_spatial_analysis.py  # Geographic data processing
├── visualization/
│   ├── tableau_workbooks/          # Tableau visualization files
│   └── data_models/               # Data models and schemas
└── utils/
    ├── db_connection.py           # Database connection utilities
    └── data_validation.py         # Data validation functions
```

## Prerequisites

- Python 3.9+
- AWS Account with necessary permissions
- PostgreSQL database access
- Required Python packages:
  ```
  psycopg2-binary
  pandas
  geopandas
  pytz
  ```

## Installation

1. Create a Python virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   ```

2. Install required packages:
   ```bash
   pip install -r requirements.txt
   ```

3. Configure environment variables:
   ```bash
   cp .env.example .env
   # Edit .env with your database credentials
   ```

## Data Sources

- Swiss Shared Mobility GBFS API
- World Air Quality Index (WAQI) API
- Swiss Municipality Boundaries Dataset
- OpenWeatherMap API

## Implementation

### ETL Process
- Lambda functions for data collection
- PostgreSQL for data storage
- Automated daily updates
- Spatial data processing

### Data Warehouse Schema
- Fact table: all_trips
- Dimension tables:
  - dim_location
  - dim_vehicle
  - dim_weather

### Visualization
- Tableau dashboards for carbon footprint analysis
- Interactive maps of service distribution
- Provider performance metrics

## Usage

1. Set up AWS Lambda functions:
   ```bash
   cd lambda_data_lake
   zip -r function.zip .
   # Upload to AWS Lambda
   ```

2. Configure database connections:
   ```python
   # Update database credentials in config/db_config.py
   ```

3. Run ETL process:
   ```bash
   python lambda_data_warehouse/lambda_etl_main.py
   ```

## Team 7

- Arnold Olympio
- Duojie Jiahua


## Infrastructure Costs

Total monthly AWS infrastructure costs: 103 USD
- Virtual Machines: 52 USD
- AWS Academy Account (Lambda & Data Transfer): 51 USD

## License

MIT

## Acknowledgments

- HSLU Data Warehouse Lab course
- Swiss Shared Mobility
- OpenStreetMap contributors