import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import time
import random
import multiprocessing
import logging

from nba_api.stats.static.players import find_players_by_full_name
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.playercareerstats import PlayerCareerStats

class Props:
    def __init__(self, item: dict):
        self.item = item
        # set values
        self.player_name: str = self.item['player_name']
        self.date = self.item['date']
        print(datetime.fromtimestamp(self.date/1000))
        # self.get_gamelogs()
        return
    def get_gamelogs(self):
        try:
            players = find_players_by_full_name(self.player_name)[0]
            season = datetime.fromtimestamp(float(self.item['date']))
            print(season)
        except Exception as e:
            logging.error(f"Error getting PlayerGameLog: {self.player_name}, {e} in {__name__}")
        return
    
# END Props

if __name__=="__main__":
    data: list[dict] = json.load(open("all_player_props_nba.json", "r"))
    Props(data[0])