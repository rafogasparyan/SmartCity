import requests


class WeatherService:
    def __init__(self):
        self.api_url = "https://api.open-meteo.com/v1/forecast"

    def get_weather(self, latitude, longitude):
        """
        Fetch current weather from Open-Meteo API for the given coordinates.

        :param latitude: Latitude of the location
        :param longitude: Longitude of the location
        :return: A dictionary containing weather data
        """
        url = f"{self.api_url}?latitude={latitude}&longitude={longitude}&current_weather=true"
        response = requests.get(url)
        data = response.json()

        if response.status_code == 200:
            current_weather = data['current_weather']
            return {
                "temperature": current_weather["temperature"],  # Temperature in Celsius
                "weatherCondition": self.map_weather_code(current_weather["weathercode"]),  # Weather condition
                "windSpeed": current_weather["windspeed"],  # Wind speed in km/h
            }
        else:
            print("Error fetching weather data:", data.get("error", "Unknown error"))
            return None

    def map_weather_code(self, code):
        """
        Map Open-Meteo weather codes to human-readable weather conditions.

        :param code: Weather code from Open-Meteo
        :return: A string describing the weather condition
        """
        weather_conditions = {
            0: "Clear sky",
            1: "Mainly clear",
            2: "Partly cloudy",
            3: "Overcast",
            45: "Fog",
            48: "Depositing rime fog",
            51: "Light drizzle",
            53: "Moderate drizzle",
            55: "Dense drizzle",
            61: "Light rain",
            63: "Moderate rain",
            65: "Heavy rain",
            71: "Light snow",
            73: "Moderate snow",
            75: "Heavy snow",
            95: "Thunderstorms",
            96: "Thunderstorm with light hail",
            99: "Thunderstorm with heavy hail"
        }
        return weather_conditions.get(code, "Unknown")
