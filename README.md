# Storm Glass Weather Data Project

A Python project for retrieving and processing weather data using the Storm Glass API.
See API documentation [here](https://docs.stormglass.io/#/weather).

## Installation

Install the project and its dependencies using pip:

```bash
pip install -e .
```

## Dependencies

- arrow (1.x) - Better dates & times for Python
- requests (2.x) - HTTP library for API calls

## Usage

Run the weather data script:

```bash
python get_weather.py
```

## Configuration

The application reads the Storm Glass API key from one of two sources (checked in this order):

1. **api_key.txt file** (recommended) - Create a file named `api_key.txt` in the project root containing your API key
2. **Environment variable** - Set an `API_KEY` environment variable

Example `api_key.txt`:
```
your-api-key-here
```

Or set environment variable:
```bash
# Windows (Command Prompt)
set API_KEY=your-api-key-here

# Windows (PowerShell)
$env:API_KEY="your-api-key-here"

# Linux/macOS
export API_KEY=your-api-key-here
```

