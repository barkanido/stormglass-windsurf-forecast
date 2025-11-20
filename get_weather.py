import requests
import arrow
import json
import os
import argparse
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

def _process_hours(hours, timezone='Asia/Jerusalem'):
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

def _update_meta(meta):
    """
    Update the meta information with report generation time and units.
    
    Args:
        meta: Meta information dictionary
        
    Returns:
        Updated meta information dictionary
    """
    meta['report_generated_at'] = arrow.now().to('Asia/Jerusalem').format('YYYY-MM-DD HH:mm')
    meta['units'] = {
        'windSpeed': 'Speed of wind at 10m above ground in knots',
        'gust': 'Wind gust in knots',
        'airTemperature': 'Air temperature in degrees celsius',
        'swellHeight': 'Height of swell waves in meters',
        'swellPeriod': 'Period of swell waves in seconds',
        'swellDirection': 'Direction of swell waves. 0° indicates swell coming from north',
        'waterTemperature': 'Water temperature in degrees celsius',
        'windDirection': 'Direction of wind at 10m above ground. 0° indicates wind coming from north'
    }
    return meta

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


def _fetch_weather_data(start, end, api_key, lat, lng):
    stormglass_endpoint = f"https://api.stormglass.io/v2/weather/point"
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
    return json_data

def _write_weather_json(json_data, weather_data_file_name):
    with open(weather_data_file_name, 'w') as f:
        json.dump(json_data, f, indent=4)

def _read_weather_data_file(weather_data_file_name):
    with open(weather_data_file_name, 'r') as f:
        json_data = json.load(f)
        print(f"Loaded {len(json_data['hours'])} hourly data points from file.")

def parse_arguments():
    """
    Parse and validate command line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments with days_ahead and first_day_offset
        
    Raises:
        SystemExit: If validation fails
    """
    parser = argparse.ArgumentParser(
        description='Fetch weather forecast data from Storm Glass API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Get 4-day forecast starting today (default)
  python get_weather.py
  
  # Get 3-day forecast starting today
  python get_weather.py --days-ahead 3
  
  # Get 2-day forecast starting 3 days from now
  python get_weather.py --days-ahead 2 --first-day-offset 3
  
  # Get 5-day forecast starting tomorrow
  python get_weather.py --days-ahead 5 --first-day-offset 1

Note: days-ahead + first-day-offset must not exceed 7 to ensure reliable forecasts.
        """
    )
    
    parser.add_argument(
        '--days-ahead',
        type=int,
        default=4,
        metavar='N',
        help='Number of days to forecast ahead (1-7, default: 4)'
    )
    
    parser.add_argument(
        '--first-day-offset',
        type=int,
        default=0,
        metavar='N',
        help='Number of days to offset the start date (0-7, default: 0 for today)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.days_ahead < 1 or args.days_ahead > 7:
        parser.error(f"days-ahead must be between 1 and 7 (got {args.days_ahead})")
    
    if args.first_day_offset < 0 or args.first_day_offset > 7:
        parser.error(f"first-day-offset must be between 0 and 7 (got {args.first_day_offset})")
    
    # Check that total doesn't exceed 7 days for reliable forecasts
    total_days = args.days_ahead + args.first_day_offset
    if total_days > 7:
        parser.error(
            f"days-ahead ({args.days_ahead}) + first-day-offset ({args.first_day_offset}) "
            f"= {total_days} exceeds maximum of 7 days for reliable forecasts"
        )
    
    return args

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()
    
    # Calculate start and end dates based on arguments
    start = arrow.now().shift(days=args.first_day_offset).floor('day')
    end = arrow.now().shift(days=args.first_day_offset + args.days_ahead - 1).ceil('day')

    api_key = get_api_key()
    stormglass_endpoint = f"https://api.stormglass.io/v2/weather/point"

    lat = 32.486722
    lng = 34.888722
    # 32°29'12.2"N 34°53'19.4"E

    json_data = _fetch_weather_data(start, end, api_key, lat, lng)

    # Process all hourly data with transformations in a single pass
    json_data['hours'] = _process_hours(json_data['hours'])

    json_data['meta'] = _update_meta(json_data['meta'])
    
    # Generate filename with actual number of days
    weather_data_file_name = 'weather_data_{}d_{}.json'.format(args.days_ahead, start.format("YYMMDD"))
    _write_weather_json(json_data, weather_data_file_name)
    print(json_data)
    _read_weather_data_file(weather_data_file_name)
