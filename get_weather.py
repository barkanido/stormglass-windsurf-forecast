import requests
import arrow
import json
import os

# Conversion factor: meters per second to knots
MS_TO_KNOTS = 1.94384


def flatten_sg_structure(hours):
    """
    Flatten the nested 'sg' structure from each hour's data.
    
    Args:
        hours: List of hourly weather data dictionaries
        
    Returns:
        List of flattened hourly data dictionaries
    """
    return [
        {
            **{key: value['sg'] if isinstance(value, dict) and 'sg' in value else value 
               for key, value in hour.items()}
        }
        for hour in hours
    ]


def convert_speed_to_knots(hours):
    """
    Convert windSpeed and gust from m/s to knots.
    
    Args:
        hours: List of hourly weather data dictionaries
        
    Returns:
        List of hourly data with wind speeds converted to knots
    """
    return [
        {
            **{key: value * MS_TO_KNOTS if key in ['windSpeed', 'gust'] else value
               for key, value in hour.items()}
        }
        for hour in hours
    ]


def convert_time_to_local(hours, timezone='Asia/Jerusalem'):
    """
    Convert the time field of each hour to local time string.
    
    The function uses arrow.get() which automatically parses ISO 8601 timestamps
    (typically in UTC from the Stormglass API) and converts them to the target timezone.
    
    Args:
        hours: List of hourly weather data dictionaries with 'time' field in ISO 8601 format
        timezone: Target timezone (default: 'Asia/Jerusalem')
        
    Returns:
        List of hourly data with times converted to local timezone string format 'YYYY-MM-DD HH:mm'
    """
    return [
        {
            **hour,
            'time': arrow.get(hour['time']).to(timezone).format('YYYY-MM-DD HH:mm')
        }
        for hour in hours
    ]

def get_api_key():
    """
    Read API key from file with fallback to environment variable.
    
    Returns:
        str: The API key
        
    Raises:
        ValueError: If API key is not found in either location
    """
    api_key = None
    try:
        with open('api_key.txt', 'r') as f:
            api_key = f.read().strip()
    except FileNotFoundError:
        pass
    
    if not api_key:
        api_key = os.environ.get('API_KEY')
    
    if not api_key:
        raise ValueError("API key not found. Please provide it in api_key.txt or set the API_KEY environment variable.")
    
    return api_key

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

# Step 1: Flatten the nested 'sg' structure
json_data['hours'] = flatten_sg_structure(json_data['hours'])

# Step 2: Convert windSpeed and gust from m/s to knots
json_data['hours'] = convert_speed_to_knots(json_data['hours'])

# Step 3: Convert the "time" field of each hour to local time string (Jerusalem time)
json_data['hours'] = convert_time_to_local(json_data['hours'])

# step 4: add current time to report
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
