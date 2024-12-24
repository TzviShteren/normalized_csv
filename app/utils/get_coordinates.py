import requests

# מטמון לאחסון תוצאות
coordinates_cache = {}

api_key = "b469e8507e3d452c8ba8a18301221c30"


def get_coordinates_bulk(cities: list) -> dict:
    results = {}
    cities_to_query = []

    for city in cities:
        if city in coordinates_cache:
            results[city] = coordinates_cache[city]
        else:
            cities_to_query.append(city)

    for city in cities_to_query:
        url = f"https://api.opencagedata.com/geocode/v1/json?q={city}&key={api_key}"
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data['results']:
                    result = {
                        "latitude": data['results'][0]['geometry']['lat'],
                        "longitude": data['results'][0]['geometry']['lng']
                    }
                else:
                    result = {"latitude": None, "longitude": None}
            else:
                result = {"latitude": None, "longitude": None}
        except requests.exceptions.RequestException:
            result = {"latitude": None, "longitude": None}

        coordinates_cache[city] = result
        results[city] = result

    return results
