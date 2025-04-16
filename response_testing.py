import pandas as pd
import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()

DEBUG_MODE = os.environ['DEBUG_MODE']=="True"

# DOMAIN=os.environ['DEV_DOMAIN'] if DEBUG_MODE else os.environ['PROD_DOMAIN']
ROOT_PATH=os.environ['ROOT_PATH']

res = requests.get(f"http://127.0.0.1:5000/{ROOT_PATH}/find_players/luka")

print(res.text)