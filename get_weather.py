import requests
import arrow
import json
import os
import argparse
import sys
from dotenv import load_dotenv
from typing import TypedDict

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# Type Definitions
# ============================================================================

class SourceData(TypedDict):
    """Data from a single source (e.g., 'sg' for StormGlass)"""
    sg: float

class RawHourlyData(TypedDict, total=False):
    """Hourly data as received from API (nested structure with sources)"""
    time: str  # ISO 8601 format (UTC)
    airTemperature: SourceData
    gust: SourceData
    swellDirection: SourceData
    swellHeight: SourceData
    swellPeriod: SourceData
    waterTemperature: SourceData
    windDirection: SourceData
    windSpeed: SourceData

class TransformedHourlyData(TypedDict):
    """Hourly data after flattening and unit conversion"""
    time: str  # Local timezone format 'YYYY-MM-DD HH:mm'
    airTemperature: float  # Celsius
    gust: float  # Knots (converted from m/s)
    swellDirection: float  # Degrees
    swellHeight: float  # Meters
    swellPeriod: float  # Seconds
    waterTemperature: float  # Celsius
    windDirection: float  # Degrees
    windSpeed: float  # Knots (converted from m/s)

class UnitDescriptions(TypedDict):
    """Unit descriptions for each weather parameter"""
    windSpeed: str
    gust: str
    airTemperature: str
    swellHeight: str
    swellPeriod: str
    swellDirection: str
    waterTemperature: str
    windDirection: str

class RawMetaData(TypedDict):
    """Meta information as received from API"""
    cost: int
    dailyQuota: int
    end: str
    lat: float
    lng: float
    params: list[str]
    requestCount: int
    start: str

class TransformedMetaData(RawMetaData):
    """Meta information with added fields after processing"""
    report_generated_at: str
    units: UnitDescriptions

class RawWeatherResponse(TypedDict):
    """Complete API response structure (raw)"""
    hours: list[RawHourlyData]
    meta: RawMetaData

class TransformedWeatherResponse(TypedDict):
    """Complete response structure after transformation"""
    hours: list[TransformedHourlyData]
    meta: TransformedMetaData

# ============================================================================
# Custom Exceptions
# ============================================================================

class StormGlassAPIError(Exception):
    """
    Custom exception for Storm Glass API errors.
    """
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        self.user_friendly_message = message
        super().__init__(self.user_friendly_message)

# ============================================================================
# Error Code Mappings
# ============================================================================

STORMGLASS_ERROR_MESSAGES = {
    402: (
        "Payment Required: You've exceeded the daily request limit for your subscription.\n"
        "Please consider upgrading if this happens frequently, or try again tomorrow."
    ),
    403: (
        "Forbidden: Your API key was not provided or is malformed.\n"
        "Please check that STORMGLASS_API_KEY in your .env file is correct."
    ),
    404: (
        "Not Found: The requested API resource does not exist.\n"
        "Please verify the API endpoint and review the API documentation."
    ),
    405: (
        "Method Not Allowed: The API resource was requested using an unsupported method.\n"
        "Please review the API documentation for correct usage."
    ),
    410: (
        "Gone: You've requested a legacy API resource that is no longer available.\n"
        "Please update your code to use the current API version."
    ),
    422: (
        "Unprocessable Content: Invalid request parameters.\n"
        "Please verify your coordinates, date range, and other parameters are correct."
    ),
    503: (
        "Service Unavailable: Storm Glass is experiencing technical difficulties.\n"
        "Please try again later."
    )
}

# ============================================================================
# New functional-style transformation functions (single-pass processing)
# ============================================================================

def _flatten_hour(hour: RawHourlyData) -> dict[str, float | str]:
    """
    Flatten the nested 'sg' structure for a single hour's data.
    """
    return {
        key: value['sg'] if isinstance(value, dict) and 'sg' in value else value 
        for key, value in hour.items()
    }

def _convert_hour_speeds(hour: dict[str, float | str]) -> dict[str, float | str]:
    """
    Convert windSpeed and gust from m/s to knots for a single hour.
    """
    MS_TO_KNOTS = 1.94384
    wind_speed_keys = ('windSpeed', 'gust')
    return {
        key: (value * MS_TO_KNOTS if isinstance(value, (int, float)) else value) if key in wind_speed_keys else value
        for key, value in hour.items()
    }

def _convert_hour_time(hour: dict[str, float | str], timezone: str = 'Asia/Jerusalem') -> dict[str, float | str]:
    """
    Convert the time field for a single hour to local time string.
    """
    return {
        **hour,
        'time': arrow.get(hour['time']).to(timezone).format('YYYY-MM-DD HH:mm')
    }

def _transform_hour(hour: RawHourlyData, timezone: str = 'Asia/Jerusalem') -> TransformedHourlyData:
    """
    Apply all transformations to a single hour's data.
    
    This combines flattening, speed conversion, and time conversion into
    a single transformation pipeline for efficient processing.
    """
    flattened = _flatten_hour(hour)
    with_speeds = _convert_hour_speeds(flattened)
    transformed = _convert_hour_time(with_speeds, timezone)
    return transformed  # type: ignore[return-value]

def _process_hours(hours: list[RawHourlyData], timezone: str = 'Asia/Jerusalem') -> list[TransformedHourlyData]:
    """
    Process all hourly data with transformations in a single pass.
    
    This function efficiently applies all transformations (flattening,
    speed conversion, and time conversion) to each hour in a single
    iteration, rather than making multiple passes over the data.
    """
    return [_transform_hour(hour, timezone) for hour in hours]

def _update_meta(meta: RawMetaData) -> TransformedMetaData:
    """
    Update the meta information with report generation time and units.
    """
    transformed_meta: TransformedMetaData = {
        **meta,
        'report_generated_at': arrow.now().to('Asia/Jerusalem').format('YYYY-MM-DD HH:mm'),
        'units': {
            'windSpeed': 'Speed of wind at 10m above ground in knots',
            'gust': 'Wind gust in knots',
            'airTemperature': 'Air temperature in degrees celsius',
            'swellHeight': 'Height of swell waves in meters',
            'swellPeriod': 'Period of swell waves in seconds',
            'swellDirection': 'Direction of swell waves. 0° indicates swell coming from north',
            'waterTemperature': 'Water temperature in degrees celsius',
            'windDirection': 'Direction of wind at 10m above ground. 0° indicates wind coming from north'
        }
    }
    return transformed_meta

# ============================================================================
# Utility functions
# ============================================================================

def get_api_key() -> str:
    """
    Read API key from environment variable.
    
    The API key is loaded from the STORMGLASS_API_KEY environment variable,
    which can be set in a .env file or directly in the environment.
    """
    api_key = os.environ.get('STORMGLASS_API_KEY')
    
    if not api_key:
        raise ValueError(
            "API key not found. Please set STORMGLASS_API_KEY in your .env file or environment.\n"
            "See .env.example for the required format."
        )
    
    return api_key


def _fetch_weather_data(start: arrow.Arrow, end: arrow.Arrow, api_key: str, lat: float, lng: float) -> RawWeatherResponse:
    """
    Fetch weather data from Storm Glass API.
        StormGlassAPIError: If the API returns an error status code
    """
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

    # Check for error status codes
    if response.status_code in STORMGLASS_ERROR_MESSAGES:
        error_message = STORMGLASS_ERROR_MESSAGES[response.status_code]
        raise StormGlassAPIError(response.status_code, error_message)
    
    # Check for any other non-200 status codes
    if response.status_code != 200:
        error_message = (
            f"Unexpected API error (HTTP {response.status_code}).\n"
            f"Response: {response.text}\n"
            "Please check the API documentation or try again later."
        )
        raise StormGlassAPIError(response.status_code, error_message)

    json_data = response.json()
    return json_data

def _write_weather_json(json_data: TransformedWeatherResponse, weather_data_file_name: str) -> None:
    with open(weather_data_file_name, 'w') as f:
        json.dump(json_data, f, indent=4)

def _read_weather_data_file(weather_data_file_name: str) -> None:
    with open(weather_data_file_name, 'r') as f:
        json_data = json.load(f)
        print(f"Loaded {len(json_data['hours'])} hourly data points from file.")

def _print_error_message(error_type: str, message: str, error_code: int | None = None) -> None:
    """
    Print a nicely formatted error message.
    """
    print("\n" + "="*70)
    print(error_type)
    print("="*70)
    if error_code is not None:
        print(f"\nError Code: {error_code}")
    print(f"\n{message}")
    print("\n" + "="*70)

def _parse_arguments() -> argparse.Namespace:
    """
    Parse and validate command line arguments.
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
    try:
        # Parse command line arguments
        args = _parse_arguments()
        
        # Calculate start and end dates based on arguments
        start = arrow.now().shift(days=args.first_day_offset).floor('day')
        end = arrow.now().shift(days=args.first_day_offset + args.days_ahead - 1).ceil('day')

        api_key = get_api_key()
        stormglass_endpoint = f"https://api.stormglass.io/v2/weather/point"

        lat = 32.486722
        lng = 34.888722
        # 32°29'12.2"N 34°53'19.4"E

        raw_data = _fetch_weather_data(start, end, api_key, lat, lng)

        # Process all hourly data with transformations in a single pass
        transformed_data: TransformedWeatherResponse = {
            'hours': _process_hours(raw_data['hours']),
            'meta': _update_meta(raw_data['meta'])
        }
        
        # Generate filename with actual number of days
        weather_data_file_name = 'weather_data_{}d_{}.json'.format(args.days_ahead, start.format("YYMMDD"))
        _write_weather_json(transformed_data, weather_data_file_name)
        print(transformed_data)
        _read_weather_data_file(weather_data_file_name)
        
    except StormGlassAPIError as e:
        _print_error_message("STORM GLASS API ERROR", e.user_friendly_message, e.status_code)
        sys.exit(1)
        
    except ValueError as e:
        _print_error_message("CONFIGURATION ERROR", str(e))
        sys.exit(1)
        
    except requests.exceptions.RequestException as e:
        error_msg = (
            f"Failed to connect to Storm Glass API.\n"
            f"Error: {str(e)}\n\n"
            "Please check your internet connection and try again."
        )
        _print_error_message("NETWORK ERROR", error_msg)
        sys.exit(1)
        
    except Exception as e:
        error_msg = (
            f"An unexpected error occurred: {str(e)}\n\n"
            "Please report this issue if it persists."
        )
        _print_error_message("UNEXPECTED ERROR", error_msg)
        sys.exit(1)
