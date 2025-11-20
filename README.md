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

Run the weather data script:

```bash
python get_weather.py
```

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
