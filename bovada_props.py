import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timezone, timedelta
import time
import random
import logging
import pprint
import regex as re

from nba_api.stats.static.players import find_players_by_full_name
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.playercareerstats import PlayerCareerStats
from nba_api.stats.endpoints.scoreboardv2 import ScoreboardV2
from nba_api.stats.endpoints.boxscoretraditionalv3 import BoxScoreTraditionalV3

from const import BOVADA_BOXSCORE_MAPPINGS
from logging_config import setup_logging

# setup_logging()

class PropObj:
    def __init__(self, item: dict):
        self.item = item
        # set values
        self.player_name: str = self.item['player_name']
        self.date: datetime = datetime.fromtimestamp(self.item['bovada_date']/1000)
        self.stat: str = self.item['stat']
        self.line: float = self.item['line_value']
        self.link: str = self.item['link']
        if self.link:
            game_date = self.link.split("-")[-1]
            self.date = datetime.strptime(game_date, "%Y%m%d%H%M")
        self.player_id = self.item['player_id']
        return

class Props:
    def __init__(self, data: list[dict], save_boxscores: bool = False, load_boxscores: bool = False):
        self.data = data
        self.save_boxscores = save_boxscores
        self.load_boxscores = load_boxscores
        # assign objects
        self.all_props, self.all_dates = [], []
        # for item in random.sample(data, k=50):
        for item in data:
            po = PropObj(item)
            if po.date < datetime.now():
                if po.date.date() not in self.all_dates:
                        self.all_dates.append(po.date.date())
                self.all_props.append(po)
        if any([len(self.all_props)==0, len(self.all_dates)==0]):
            logging.error("No games or dates found for props.")
            return
        self.boxscores: dict = {}
        self.set_boxscores()
        if save_boxscores:
            for key in self.boxscores:
                self.boxscores[key].to_csv(f"./data/{key.strftime("%Y-%m-%d")}_boxscores.csv", index=False)
        outcomes: list[dict] = self.get_bet_hits()
        new_df: pd.DataFrame = pd.DataFrame(outcomes)
        new_df = new_df.sort_values(by=['bovada_date', 'team_abbr', 'player_name'], ascending=False)
        new_df.insert(0, 'date', new_df['bovada_date'].apply(lambda x: datetime.fromtimestamp(x/1000).strftime("%Y-%m-%d")))
        new_df.drop(columns=['bovada_date', 'link']).to_csv(f"outcomes.csv", index=False)
        return
    def print_progress_bar(self, iteration, total, prefix = 'Progress', suffix = 'Complete', decimals = 1, length = 50, fill = '█', printEnd = "\r"):
        """
        Call in a loop to create terminal progress bar
        @params:
            iteration   - Required  : current iteration (Int)
            total       - Required  : total iterations (Int)
            prefix      - Optional  : prefix string (Str)
            suffix      - Optional  : suffix string (Str)
            decimals    - Optional  : positive number of decimals in percent complete (Int)
            length      - Optional  : character length of bar (Int)
            fill        - Optional  : bar fill character (Str)
            printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
        """
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
        # Print New Line on Complete
        if iteration == total: 
            print()
        return
    def fetch_boxscore(self, game_id: str):
        box_score = BoxScoreTraditionalV3(game_id=game_id)
        return box_score.get_data_frames()[0]
    def get_boxscore_data(self, dates: list[datetime]):
        for date in dates:
            games = ScoreboardV2(game_date=date.strftime("%Y-%m-%d"))
            game_ids: list[str] = list(games.get_data_frames()[0]['GAME_ID'].values)
            self.boxscores[date] = pd.DataFrame()
            for _id in game_ids:
                self.boxscores[date] = pd.concat([self.boxscores[date], self.fetch_boxscore(_id)])
                time.sleep(0.6)
        return
    def set_boxscores(self):
        if self.load_boxscores: # load
            for fn in os.listdir("./data/"):
                if re.search(r"_boxscores", fn):
                    self.boxscores[datetime.strptime(fn.split("_")[0], "%Y-%m-%d").date()] = pd.read_csv(f"./data/{fn}")
            return
        # fetch
        self.get_boxscore_data(self.all_dates)
        return
    def get_bet_hits(self):
        outcomes = []
        po: PropObj
        for index, po in enumerate(self.all_props):
            # pprint.pp(po.item)
            self.print_progress_bar(index, len(self.all_props))
            try:
                try:
                    df: pd.DataFrame = self.boxscores[po.date.date()]
                except IndexError:
                    print("No frame found!")
                    # print(f"Boxscore not fround for exact prop date, trying day before: {po.date.date()}")
                    # df: pd.DataFrame = self.get_boxscore_data(
                    #     data=[],
                    #     dates=[po.date-timedelta(days=1)],
                    #     just_frame=True
                    # )
                df: pd.DataFrame = df[df['personId']==po.player_id]
                game_vals = df[BOVADA_BOXSCORE_MAPPINGS[po.stat]].values[0]
                actual_total = int(sum(game_vals))
                new_item = po.item.copy()
                new_item['actual_total'] = actual_total
                if actual_total < po.line:
                    new_item['outcome'] = 'UNDER'
                    # print(f"{po.stat} was UNDER => {actual_total} < {po.line}")
                elif actual_total > po.line:
                    new_item['outcome'] = 'OVER'
                    # print(f"{po.stat} was OVER => {actual_total} > {po.line}")
                else:
                    new_item['outcome'] = 'EVEN'
                    # print(f"{po.stat} was EQUAL => {actual_total} = {po.line}")
                outcomes.append(new_item)
            except Exception as e:
                logging.error(f"Error getting player/boxscore: {po.date.date(), po.player_name}, {e}")
        return outcomes
    
# END Props

if __name__=="__main__":
    data: list[dict] = json.load(open("all_player_props_nba.json", "r"))
    # data = random.sample(data, k=50)
    # Props(data, save_boxscores=True)
    Props(data, load_boxscores=True)