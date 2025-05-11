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
        self.date: datetime = datetime.fromtimestamp(self.item['date']/1000, tz=timezone.utc)
        self.stat: str = self.item['stat']
        self.line: float = self.item['line_value']
        self.link: str = self.item['link']
        if self.link:
            game_date = self.link.split("-")[-1]
            self.date = datetime.strptime(game_date, "%Y%m%d%H%M")
        try:
            self.player_data = find_players_by_full_name(self.player_name)[0]
        except:
            logging.error(f"No player data found for: {self.player_name}")
            return
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
                if po.date not in self.all_dates:
                        self.all_dates.append(po.date)
                self.all_props.append(po)
        if any([len(self.all_props)==0, len(self.all_dates)==0]):
            logging.error("No games or dates found for props.")
            return
        self.boxscores: list[dict] = self.get_boxscores()
        if save_boxscores:
            for vals in self.boxscores:
                vals['frame'].to_csv(f"./data/{vals['date'].strftime("%Y-%m-%d")}_{vals['game_id']}_boxscore.csv", index=False)
        outcomes: list[dict] = self.get_bet_hits()
        pd.DataFrame(outcomes).drop(columns=['link']).to_csv(f"outcomes.csv", index=False)
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
    def get_boxscore_data(self, data: list, dates: list[datetime], just_frame: bool = False):
        for date in dates:
            s_date: str = date.strftime("%Y-%m-%d")
            games = ScoreboardV2("0", s_date, "00")
            gdf: pd.DataFrame = games.get_data_frames()[0]
            ids: list = list(gdf['GAME_ID'].values)
            for id in ids:
                try:
                    df: pd.DataFrame = BoxScoreTraditionalV3(id).get_data_frames()[0]
                    data.append({ "date": date, "game_id": id, "frame": df })
                    time.sleep(0.6)
                except Exception as e:
                    logging.error(f"Game not found (possible not played yet): {id}")
        if just_frame:
            return pd.concat([d['frame'] for d in data])
        return data
    def get_boxscores(self):
        data = []
        if self.load_boxscores: # load
            for fn in os.listdir("./data/"):
                if re.search(r"_boxscore", fn):
                    data.append({ "date": datetime.strptime(fn.split("_")[0], "%Y-%m-%d"), "game_id": fn.split("_")[1], "frame": pd.read_csv(f"./data/{fn}") })
            return data
        # fetch
        data = self.get_boxscore_data(data, self.all_dates)
        return data
    def get_bet_hits(self):
        outcomes = []
        po: PropObj
        for index, po in enumerate(self.all_props):
            # pprint.pp(po.item)
            self.print_progress_bar(index, len(self.all_props))
            try:
                try:
                    df: pd.DataFrame = [
                        b['frame'] for b in self.boxscores \
                        if b['date'].date()==po.date.date() and po.player_data['id'] and \
                        po.player_data['id'] in b['frame']['personId'].values
                    ][0]
                except IndexError:
                    print(f"Boxscore not fround for exact prop date, trying day before: {po.date.date()}")
                    df: pd.DataFrame = self.get_boxscore_data(
                        data=[],
                        dates=[po.date-timedelta(days=1)],
                        just_frame=True
                    )
                df: pd.DataFrame = df[df['personId']==po.player_data['id']]
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
    Props(data, save_boxscores=True)
    # Props(data, load_boxscores=True)