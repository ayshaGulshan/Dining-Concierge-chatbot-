import requests 
import boto3 
from decimal import Decimal
from datetime import datetime


class RestaurantInputData:
    def __init__(self):
        self.restaurant_ids = set()

    def _is_value_empty(self, value):
        return not value or len(str(value)) == 0

    def _format_data(self, entry, cuisine):
        formatted = {}
        self.restaurant_ids.add(entry['id'])

        formatted['id'] = entry['id']
        formatted['RestaurantID'] = entry['id']
        formatted['insertedAtTimestamp'] = str(datetime.now())
        formatted['cuisine_type'] = 'indian' if cuisine == 'indpak' else cuisine
        formatted['name'] = entry['name']
        formatted['url'] = entry['url']

        if not self._is_value_empty(entry.get("rating")):
            formatted["rating"] = Decimal(str(entry["rating"]))
        if not self._is_value_empty(entry.get("coordinates")):
            formatted["latitude"] = Decimal(str(entry["coordinates"]["latitude"]))
            formatted["longitude"] = Decimal(str(entry["coordinates"]["longitude"]))
        if not self._is_value_empty(entry.get("phone")):
            formatted["contact"] = entry["phone"]
        if not self._is_value_empty(entry.get("review_count")):
            formatted["review_count"] = entry["review_count"]
        if not self._is_value_empty(entry.get("price")):
            formatted["price"] = entry["price"]

        if entry.get('location'):
            address = "".join(entry['location']['display_address'])
            formatted['address'] = address
            formatted["zip_code"] = entry['location']['zip_code']

        return formatted
    
    def store_yelp_data(self, restaurant_data):
        print("store")
        dynamodb = boto3.resource('dynamodb',
                                  aws_access_key_id='',
                                  aws_secret_access_key='',
                                  region_name='us-east-1')
        table = dynamodb.Table('yelp-restaurants')
        print(restaurant_data)
        for data in restaurant_data:
            print(data)
            table.put_item(Item=data)

    def fetch_yelp_restaurant_data(self, api, api_key):
        headers = {"Authorization": "Bearer " + api_key}
        cuisines = ['indpak', 'italian', 'mexican', 'chinese', 'japanese', 'french', 'greek']
        location = 'Manhattan'
        all_data = []

        for cuisine in cuisines:
            total_entries = 500
            offset = 0
            while total_entries > 0:
                params = {
                    'location': location,
                    'categories': cuisine,
                    'limit': 50,
                }
                response = requests.get(api, headers=headers, params=params).json()
                print(response)
                if response.get("businesses"):
                    for entry in response["businesses"]:
                        if entry['id'] in self.restaurant_ids:
                            continue
                        all_data.append(self._format_data(entry, cuisine))
                    total_entries -= len(response["businesses"])
                    offset += len(response["businesses"])
                else:
                    break

        return all_data

if __name__ == '__main__':
    api_url = 'https://api.yelp.com/v3/businesses/search'
    # Removed the hard coded API keys for safety purposes
    api_access_key = ''

    processor = RestaurantInputData()
    data = processor.fetch_yelp_restaurant_data(api_url, api_access_key)
    processor.store_yelp_data(data)