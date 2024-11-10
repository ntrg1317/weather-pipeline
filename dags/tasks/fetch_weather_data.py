import logging
from datetime import datetime

import requests

def fetch_weather_data(city, lat, lon, api_key):
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return {
            'city': data['name'],
            'country': data['sys']['country'],
            'weather_main': data['weather']['main'],
            'weather_description': data['weather']['description'],
            'weather_icon': data['weather']['icon'],
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'sea_level': data['main']['sea_level'],
            'grnd_level': data['main']['grnd_level'],
            'visibility': data['visibility'],
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind']['deg'],
            'wind_gust': data['wind']['gust'],
            'rain': data['rain'],
            'clouds': data['clouds']['all'],
            'timestamp': datetime.fromtimestamp(int(data['dt'])).strftime('%Y-%m-%d %H:%M:%S'),
            'sunrise': data['sys']['sunrise'],
            'sunset': data['sys']['sunset'],
        }
    except Exception as e:
        logging.error(f"Error fetching weather data for {city}: {e}")
        return None