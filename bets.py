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

import logging_config

LOCAL_PLAYER_ID = 1629029 # luka

INITIAL_VALUES = {
    'player': {
        'first_name': 'LeBron',
        'full_name': 'LeBron James',
        'id': 2544,
        'is_active': True,
        'last_name': 'James'
    },
    'bet_type': {
        'name': 'over',
        'code': 'ov'
    },
    'number_value': 20.5,
    'stat': {
        'name': 'PTS',
        'code': 'points'
    }
}

class Bets:
    def __init__(
        self, player: dict, bet_type: dict, number_value: float, stat: dict, 
        save: bool = False, load: bool = False
    ):
        logging_config.setup_logging()
        # bet attributes
        self.player = player
        self.bet_type = bet_type['name']
        self.number_value = number_value
        self.stat = stat['name']
        self.player_id: int = player['id']
        self.data_dir: str = "./data/"
        if load: # from local
            self.player_info_df: pd.DataFrame = pd.read_csv(f"{self.data_dir}player_info.csv")
            career_stat_keys = [
                'season_totals_regular_season', 'career_totals_regular_season', 'season_totals_post_season',
                'career_totals_post_season', 'season_rankings_regular_season', 'season_rankings_post_season'
            ]
            self.career_stats: dict[pd.DataFrame] = {}
            for key in career_stat_keys:
                self.career_stats[key] = pd.read_csv(f"{self.data_dir}{key}.csv")
            self.seasons: list[int] = json.load(open(f"{self.data_dir}seasons.json", "r"))
            self.all_gamelogs: pd.DataFrame = pd.read_csv(f"{self.data_dir}all_gamelogs.csv")
            self.all_gamelogs['GAME_DATE'] = self.all_gamelogs['GAME_DATE'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date())
        else: # from nba_api
            # set player_info_df (position, name draft_year, etc.)
            self.player_info_df: pd.DataFrame = self.get_player_info()
            # career stats
            self.career_stats: dict[pd.DataFrame] = self.get_career_stats()
            # seasons active in NBA
            self.seasons: list[int] = self.get_seasons()
            # set all gamelogs
            self.all_gamelogs: pd.DataFrame = self.get_all_gamelogs()
            # convert + sort by GAME_DATEs
            self.all_gamelogs['GAME_DATE'] = self.all_gamelogs['GAME_DATE'].apply(lambda x: datetime.strptime(x, "%b %d, %Y").date())
            self.all_gamelogs = self.all_gamelogs.sort_values(by=['GAME_DATE'], ascending=False)
        # save locally for testing
        if save:
            self.write_locally()
        # current season + playoffs started
        self.current_season = self.seasons[-1]
        self.playoffs_started = any((self.all_gamelogs['SEASON']==self.current_season)&(self.all_gamelogs['SEASON_TYPE']=='post'))
        return
    def write_locally(self):
        try:
            os.mkdir(self.data_dir)
        except Exception:
            pass
        self.player_info_df.to_csv(f"{self.data_dir}player_info.csv", index=False)
        for key in self.career_stats:
            self.career_stats[key].to_csv(f"{self.data_dir}{key}.csv", index=False)
        json.dump(self.seasons, open(f"{self.data_dir}seasons.json", "w"))
        self.all_gamelogs.to_csv(f"{self.data_dir}all_gamelogs.csv", index=False)
        return
    def get_player_info(self):
        cpi_frames: list[pd.DataFrame] = CommonPlayerInfo(self.player_id).get_data_frames()
        time.sleep(0.6)
        return cpi_frames[0]
    def get_career_stats(self):
        """
        https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints_output/playercareerstats_output.md
        """
        frames = PlayerCareerStats(self.player_id).get_data_frames()
        time.sleep(0.6)
        return {
            'season_totals_regular_season': frames[0],
            'career_totals_regular_season': frames[1],
            'season_totals_post_season': frames[2],
            'career_totals_post_season': frames[3],
            'season_rankings_regular_season': frames[10],
            'season_rankings_post_season': frames[11]
        }
    def get_seasons(self):
        seasons = self.career_stats['season_totals_regular_season']['SEASON_ID'].values
        seasons: list[int] = list(set([int(s.split("-")[0]) for s in seasons]))
        seasons.sort()
        return seasons
    def get_all_gamelogs(self):
        df_list = []
        for season in self.seasons:
            logging.info(f"Getting gamelogs for season: {season}")
            # regular
            reg_df: pd.DataFrame = PlayerGameLog(self.player_id, season, r"Regular Season").get_data_frames()[0]
            reg_df.insert(0, 'SEASON', season)
            reg_df.insert(1, 'SEASON_TYPE', 'regular')
            if not reg_df.empty:
                df_list.append(reg_df)
            time.sleep(0.6)
            # post
            post_df: pd.DataFrame = PlayerGameLog(self.player_id, season, r"Playoffs").get_data_frames()[0]
            post_df.insert(0, 'SEASON', season)
            post_df.insert(1, 'SEASON_TYPE', 'post')
            if not post_df.empty:
                df_list.append(post_df)
            time.sleep(0.6)
        try:
            return pd.concat(df_list)
        except ValueError:
            print(f"No gamelogs found for {self.player_id}")
            return None
    # MAIN functions
    def get_hits_last_n(self, n: int):
        try:
            df = self.all_gamelogs.copy()
            df = df.sort_values(by=['GAME_DATE'], ascending=False)
            df = df.head(n)
            if self.bet_type == 'over':
                return len(df[(df[self.stat]>self.number_value)])
            elif self.bet_type == 'under':
                return len(df[(df[self.stat]>self.number_value)])
            elif self.bet_type == 'at least':
                return len(df[(df[self.stat]>=self.number_value)])
            return None
        except Exception:
            logging.error(f"Error getting get_hits_last_n({n})")
            return None
    def get_season_avg(self):
        try:
            df: pd.DataFrame = self.career_stats['season_totals_regular_season']
            vals = df[df['SEASON_ID'].str.contains(str(self.current_season))][['GP', self.stat]].values[0]
            _avg = round(vals[1]/vals[0], 2)
            return _avg
        except Exception:
            logging.error("Error getting season_avg")
            return None
    def get_career_avg(self):
        try:
            df: pd.DataFrame = self.career_stats['career_totals_regular_season']
            vals = df[['GP', self.stat]].values[0]
            _avg = round(vals[1]/vals[0], 2)
            return _avg
        except Exception:
            logging.error("Error getting career_avg")
            return None
    def get_season_avg_post(self):
        try:
            if self.playoffs_started:
                df: pd.DataFrame = self.career_stats['season_totals_post_season']
                vals = df[df['SEASON_ID'].str.contains(str(self.current_season))][['GP', self.stat]].values[0]
                _avg = round(vals[1]/vals[0], 2)
                return _avg
            logging.info("season_avg_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting season_avg_post")
            return None
    def get_career_avg_post(self):
        try:
            if self.playoffs_started:
                df: pd.DataFrame = self.career_stats['career_totals_post_season']
                vals = df[['GP', self.stat]].values[0]
                _avg = round(vals[1]/vals[0], 2)
                return _avg
            logging.info("career_avg_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting career_avg_post")
            return None
    def get_season_rank(self):
        try:
            df: pd.DataFrame = self.career_stats['season_rankings_regular_season']
            val = df[df['SEASON_ID'].str.contains(str(self.current_season))][f'RANK_{self.stat}'].values[0]
            return int(val)
        except Exception:
            logging.error("Error getting season_rank")
            return None
    def get_season_rank_post(self):
        try:
            if self.playoffs_started:
                df: pd.DataFrame = self.career_stats['season_rankings_post_season']
                val = df[df['SEASON_ID'].str.contains(str(self.current_season))][f'RANK_{self.stat}'].values[0]
                return int(val)
            logging.info("season_rank_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting season_rank_post")
            return None
    def get_data(self):
        df: pd.DataFrame = self.all_gamelogs.copy()[['GAME_DATE', self.stat]].head(10)
        df['GAME_DATE'] = df['GAME_DATE'].apply(str)
        df[self.stat] = df[self.stat].apply(float)
        # construct response JSON (dict)
        self.res = {
            'playoffs_started': self.playoffs_started,
            'hit_last_5_games': self.get_hits_last_n(5),
            'hit_last_10_games': self.get_hits_last_n(10),
            'hit_last_20_games': self.get_hits_last_n(20),
            'season_avg': self.get_season_avg(),
            'career_avg': self.get_career_avg(),
            'season_avg_post': self.get_season_avg_post(),
            'career_avg_post': self.get_season_avg_post(),
            'season_rank': self.get_season_rank(),
            'season_rank_post': self.get_season_rank_post(),
            'last_10_stats': {
                'labels': list(df['GAME_DATE'].values),
                'data': list(df[self.stat].values),
            }
        }
        return self.res
    
# if __name__=="__main__":
#     # bets = Bets(
#     #     player=INITIAL_VALUES['player'],
#     #     bet_type=INITIAL_VALUES['bet_type'],
#     #     number_value=INITIAL_VALUES['number_value'],
#     #     stat=INITIAL_VALUES['stat'],
#     #     save=True
#     # )
#     bets = Bets(
#         player=INITIAL_VALUES['player'],
#         bet_type=INITIAL_VALUES['bet_type'],
#         number_value=INITIAL_VALUES['number_value'],
#         stat=INITIAL_VALUES['stat'],
#         load=True
#     )
#     json.dump(bets.get_data(), open("dummy_bets_info_response.json", "w"), indent=4)