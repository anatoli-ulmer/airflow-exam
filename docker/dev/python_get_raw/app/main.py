# main.py - get raw data

import requests
import os
from datetime import datetime
from time import sleep
import json

API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')
SAVE_DIR = '/app/raw_files'

def get_data_from_openweathermap(cities=['Berlin', 'Paris', 'London']):
    
    responses = []
    
    dt = datetime.now()
    print(f"Requesting weather data for {cities} ({dt.strftime('%Y-%m-%d %H:%M:%SZ')})")
    
    for city in cities:
        
        r = requests.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}")
        
        # date_str = r.headers['Date']
        # dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %Z")
        
        data_city = r.json()
        print(f"Received data: {data_city['main']['temp']-273.15:.1f}°C in {data_city['name']}")
        
        responses.append(data_city)
                    
    filename = dt.strftime("%Y-%m-%d %H:%M.json")
    savepath = os.path.join(SAVE_DIR, filename)
    os.makedirs(SAVE_DIR, exist_ok=True)
        
    print(f"Saving results to {savepath}")
    with open(savepath, 'w') as f:
        json.dump(responses, f)
    
    sleep(1)
        
    
if __name__ == '__main__':
    
    get_data_from_openweathermap()