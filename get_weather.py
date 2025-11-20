import requests
import arrow
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Conversion factor: meters per second to knots
MS_TO_KNOTS = 1.94384

# ============================================================================
# New functional-style transformation functions (single-pass processing)
# ============================================================================

def _flatten_hour(hour):
    """
    Flatten the nested 'sg' structure for a single hour's data.
    
    Args:
        hour: Single hourly weather data dictionary
        
    Returns:
        Dictionary with flattened structure
    """
    return {
        key: value['sg'] if isinstance(value, dict) and 'sg' in value else value 
        for key, value in hour.items()
    }

def _convert_hour_speeds(hour):
    """
    Convert windSpeed and gust from m/s to knots for a single hour.
    
    Args:
        hour: Single hourly weather data dictionary
        
    Returns:
        Dictionary with wind speeds converted to knots
    """
    return {
        key: value * MS_TO_KNOTS if key in ['windSpeed', 'gust'] else value
        for key, value in hour.items()
    }

def _convert_hour_time(hour, timezone='Asia/Jerusalem'):
    """
    Convert the time field for a single hour to local time string.
    
    Args:
        hour: Single hourly weather data dictionary with 'time' field in ISO 8601 format
        timezone: Target timezone (default: 'Asia/Jerusalem')
        
    Returns:
        Dictionary with time converted to local timezone string format 'YYYY-MM-DD HH:mm'
    """
    return {
        **hour,
        'time': arrow.get(hour['time']).to(timezone).format('YYYY-MM-DD HH:mm')
    }

def _transform_hour(hour, timezone='Asia/Jerusalem'):
    """
    Apply all transformations to a single hour's data.
    
    This combines flattening, speed conversion, and time conversion into
    a single transformation pipeline for efficient processing.
    
    Args:
        hour: Single hourly weather data dictionary
        timezone: Target timezone (default: 'Asia/Jerusalem')
        
    Returns:
        Fully transformed hourly data dictionary
    """
    hour = _flatten_hour(hour)
    hour = _convert_hour_speeds(hour)
    hour = _convert_hour_time(hour, timezone)
    return hour

def process_hours(hours, timezone='Asia/Jerusalem'):
    """
    Process all hourly data with transformations in a single pass.
    
    This function efficiently applies all transformations (flattening,
    speed conversion, and time conversion) to each hour in a single
    iteration, rather than making multiple passes over the data.
    
    Args:
        hours: List of hourly weather data dictionaries
        timezone: Target timezone (default: 'Asia/Jerusalem')
        
    Returns:
        List of fully transformed hourly data dictionaries
    """
    return [_transform_hour(hour, timezone) for hour in hours]


# ============================================================================
# Utility functions
# ============================================================================

def get_api_key():
    """
    Read API key from environment variable.
    
    The API key is loaded from the STORMGLASS_API_KEY environment variable,
    which can be set in a .env file or directly in the environment.
    
    Returns:
        str: The API key
        
    Raises:
        ValueError: If API key is not found
    """
    api_key = os.environ.get('STORMGLASS_API_KEY')
    
    if not api_key:
        raise ValueError(
            "API key not found. Please set STORMGLASS_API_KEY in your .env file or environment.\n"
            "See .env.example for the required format."
        )
    
    return api_key

# ============================================================================
# Main script
# ============================================================================

if __name__ == "__main__":
    # Get first hour of today
    start = arrow.now().floor('day')
    # Get last of day after tomorrow
    end = arrow.now().shift(days=2).ceil('day')

    api_key = get_api_key()
    stormglass_endpoint = f"https://api.stormglass.io/v2/weather/point"

    lat = 32.486722
    lng = 34.888722
    # 32°29'12.2"N 34°53'19.4"E

    params = [
        "airTemperature",
        "gust",
        "swellDirection",
        "swellHeight",
        "swellPeriod",
        "waterTemperature",
        "windDirection",
        "windSpeed",
    ]

    print(f"Fetching weather data from {start} to {end} for coordinates ({lat}, {lng})")

    response = requests.get(
      stormglass_endpoint,
      params={
        'lat': lat,
        'lng': lng,
        'params': ','.join(params),
        'start': start.to('UTC').timestamp(),  # Convert to UTC timestamp
        'end': end.to('UTC').timestamp(),
        'source': 'sg'  # Using Stormglass as the data source
      },
      headers={
        'Authorization': api_key
      }
      
    )

    json_data = response.json()

    # Process all hourly data with transformations in a single pass
    json_data['hours'] = process_hours(json_data['hours'])

    # Add current time to report
    json_data['meta']['report_generated_at'] = arrow.now().to('Asia/Jerusalem').format('YYYY-MM-DD HH:mm')
    json_data['meta']['units'] = {
        'windSpeed': 'Speed of wind at 10m above ground in knots',
        'gust': 'Wind gust in knots',
        'airTemperature': 'Air temperature in degrees celsius',
        'swellHeight': 'Height of swell waves in meters',
        'swellPeriod': 'Period of swell waves in seconds',
        'swellDirection': 'Direction of swell waves. 0° indicates swell coming from north',
        'waterTemperature': 'Water temperature in degrees celsius',
        'windDirection': 'Direction of wind at 10m above ground. 0° indicates wind coming from north'
    }

    # pretty print the json data to a files with timestamp
    weather_data_file_name = 'weather_data_3d_{}.json'.format(start.format("YYMMDD"))
    with open(weather_data_file_name, 'w') as f:
        json.dump(json_data, f, indent=4)
    print(json_data)
    with open(weather_data_file_name, 'r') as f:
        json_data = json.load(f)
        print(f"Loaded {len(json_data['hours'])} hourly data points from file.")
