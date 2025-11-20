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
- python-dotenv (1.x) - Load environment variables from .env file

## Usage

Run the weather data script with optional command line arguments:

```bash
python get_weather.py [--days-ahead N] [--first-day-offset N]
```

### Command Line Arguments

- `--days-ahead N`: Number of days to forecast ahead (1-7, default: 4)
- `--first-day-offset N`: Number of days to offset the start date (0-7, default: 0 for today)

**Important:** The sum of `days-ahead` and `first-day-offset` must not exceed 7 to ensure reliable forecasts.

### Usage Examples

```bash
# Get 4-day forecast starting today (default behavior)
python get_weather.py

# Get 3-day forecast starting today
python get_weather.py --days-ahead 3

# Get 2-day forecast starting 3 days from now
python get_weather.py --days-ahead 2 --first-day-offset 3

# Get 5-day forecast starting tomorrow
python get_weather.py --days-ahead 5 --first-day-offset 1

# Get help and see all options
python get_weather.py --help
```

### Output

The script generates a JSON file named `weather_data_{N}d_{date}.json` where:
- `{N}` is the number of days in the forecast
- `{date}` is the start date in YYMMDD format

## Configuration

The application reads the Storm Glass API key from the `STORMGLASS_API_KEY` environment variable. You can set this in one of two ways:

### Option 1: Using .env file (Recommended)

Create a `.env` file in the project root:

```bash
STORMGLASS_API_KEY=your-api-key-here
```

A `.env.example` template file is provided as a reference.

### Option 2: Set environment variable directly

Set the environment variable in your shell:

```bash
# Windows (Command Prompt)
set STORMGLASS_API_KEY=your-api-key-here

# Windows (PowerShell)
$env:STORMGLASS_API_KEY="your-api-key-here"

# Linux/macOS
export STORMGLASS_API_KEY=your-api-key-here
```

**Note:** The `.env` file is automatically ignored by git to keep your API key secure.
