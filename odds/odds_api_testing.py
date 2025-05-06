import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_URL = os.environ['API_URL']
API_KEY = os.environ['API_KEY']

res = requests.get(
    url=f"{API_URL}/historical/sports/basketball_nba/odds",
    params={
        'apiKey': API_KEY,
        'regions': 'us',
        'date': '2025-04-20T12:15:00Z'
    }
)

json.dump(res.json(), open("responses/historical_nba_odds_2025-04-20.json", "w"), indent=4)

# nothing