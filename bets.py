import pandas as pd
import numpy as np
import os
import json
from datetime import datetime
import time
import logging
import asyncio
import aiohttp

from nba_api.stats.static.players import find_players_by_full_name
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.playercareerstats import PlayerCareerStats

from logging_config import setup_logging
from const import BOVADA_PROP_STAT_MAPPINGS, DATETIME_FORMAT
from aws import get_dynamo_table

INITIAL_VALUES = [{
    "id": 1,
    "bet": {
        "primary_key": "698f2ecb1a2884175a350206b63b0743176c327fbd80e7a3f6b8a9f8e13f4a3f",
        "over_odds": -120,
        "bovada_date": "23/05/2025, 20:00:00",
        "parent_path": "bovada_data/25-05-22/16/nba",
        "player_name": "Tyrese Haliburton",
        "player_id": 1630169,
        "stat": "total_points_and_rebounds",
        "under_odds": -110,
        "team_abbr": "IND",
        "line_value": 24.5,
        "date_downloaded": "22/05/2025, 11:37:41",
        "date_collected": "22/05/2025, 16:00:00",
        "id": "indiana-pacers-new-york-knicks-202505232000",
        "date": "2025-05-24T01:00:00.000Z",
        "bet": "Points and Rebounds"
    },
    "user_option": "under"
}]

class PlayerDataObj:
    def __init__(self, player_id, save: bool = False, load: bool = False):
        self.player_id = player_id
        self.data_dir = "./data/"
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
            self.all_gamelogs: pd.DataFrame = asyncio.run(self.get_all_gamelogs(self.seasons, self.player_id))
            # convert + sort by GAME_DATEs
            self.all_gamelogs['GAME_DATE'] = self.all_gamelogs['GAME_DATE'].apply(lambda x: datetime.strptime(x, "%b %d, %Y").date())
            self.all_gamelogs = self.all_gamelogs.sort_values(by=['GAME_DATE'], ascending=False)
        # save locally for testing
        if save:
            self.write_locally()
        return
    def write_locally(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self.player_info_df.to_csv(f"{self.data_dir}{self.player_id}_player_info.csv", index=False)
        for key in self.career_stats:
            self.career_stats[key].to_csv(f"{self.data_dir}{self.player_id}_{key}.csv", index=False)
        json.dump(self.seasons, open(f"{self.data_dir}{self.player_id}_seasons.json", "w"))
        self.all_gamelogs.to_csv(f"{self.data_dir}{self.player_id}_all_gamelogs.csv", index=False)
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
    async def get_gamelog_reg(self, season: int, player_id: int):
        # regular
        reg_df: pd.DataFrame = PlayerGameLog(player_id, season, r"Regular Season").get_data_frames()[0]
        reg_df.insert(0, 'SEASON', season)
        reg_df.insert(1, 'SEASON_TYPE', 'regular')
        if not reg_df.empty:
            time.sleep(0.6)
            return reg_df
        return None
    async def get_gamelog_post(self, season: int, player_id: int):
        # post
        post_df: pd.DataFrame = PlayerGameLog(player_id, season, r"Playoffs").get_data_frames()[0]
        post_df.insert(0, 'SEASON', season)
        post_df.insert(1, 'SEASON_TYPE', 'post')
        if not post_df.empty:
            time.sleep(0.6)
            return post_df
        return None
    async def get_all_gamelogs(self, seasons: list[int], player_id: int):
        # Using asyncio.gather and a list comprehension
        reg = await asyncio.gather(*(self.get_gamelog_reg(s, player_id) for s in seasons))
        reg_df = pd.concat([r for r in reg if r is not None])
        post = await asyncio.gather(*(self.get_gamelog_post(s, player_id) for s in seasons))
        post_df = pd.concat([r for r in post if r is not None])
        try:
            df = pd.concat([reg_df, post_df])
            df['is_home'] = ~df['MATCHUP'].str.contains('@')
            return df
        except ValueError:
            print(f"No gamelogs found for {self.player_id}")
            return None
# END PlayerDataObj

class BetResponseObj:
    def __init__(self, bet_slip_object: dict, player_data: PlayerDataObj, props_df: pd.DataFrame):
        self.bet_slip_object = bet_slip_object
        self.player_data = player_data
        self.props_df = props_df
        # bet attributes
        self.bet_type: str = self.bet_slip_object['user_option']
        self.number_value: float = self.bet_slip_object['bet']['line_value']
        self.raw_stat: str = self.bet_slip_object['bet']['stat']
        self.bovada_stat: str = str(self.raw_stat).upper()
        self.stats: list[str] = BOVADA_PROP_STAT_MAPPINGS[self.bet_slip_object['bet']['stat']]
        self.player_id: int = self.bet_slip_object['bet']['player_id']
        self.game_id: str = self.bet_slip_object['bet']['id']
        # set data
        self.player_info_df: pd.DataFrame = self.player_data.player_info_df
        self.career_stats: dict[pd.DataFrame] = self.player_data.career_stats
        self.seasons: list[int] = self.player_data.seasons
        self.all_gamelogs: pd.DataFrame = self.player_data.all_gamelogs
        self.all_gamelogs[self.bovada_stat] = self.all_gamelogs[self.stats].sum(axis=1)
        # current season + playoffs started
        self.current_season = self.seasons[-1]
        self.playoffs_started = any((self.all_gamelogs['SEASON']==self.current_season)&(self.all_gamelogs['SEASON_TYPE']=='post'))
        self.add_bovada_stat_to_frames()
        return
    def add_bovada_stat_to_frames(self):
        for key in self.career_stats:
            if 'rankings' not in key:
                df: pd.DataFrame = self.career_stats[key]
                df[self.bovada_stat] = df[self.stats].sum(axis=1)
                self.career_stats[key] = df
        return
    def get_hits_last_n(self, n: int):
        try:
            df = self.all_gamelogs.copy()
            df = df.sort_values(by=['GAME_DATE'], ascending=False)
            df = df.head(n)
            if self.bet_type == 'over':
                return len(df[(df[self.bovada_stat]>self.number_value)])
            elif self.bet_type == 'under':
                return len(df[(df[self.bovada_stat]<self.number_value)])
            elif self.bet_type == 'at least':
                return len(df[(df[self.bovada_stat]>=self.number_value)])
            return None
        except Exception as e:
            logging.error(f"Error getting get_hits_last_n({n}): {e}")
            return None
    def get_season_avg(self):
        try:
            df: pd.DataFrame = self.career_stats['season_totals_regular_season']
            vals = df[df['SEASON_ID'].str.contains(str(self.current_season))][['GP', self.bovada_stat]+self.stats].values[0]
            games_played = vals[0]
            arr: np.ndarray = (vals/games_played)[1:]
            attrs = ['total'] + self.stats
            return { a: round(arr[index], 2) for index, a in enumerate(attrs) }
        except Exception:
            logging.error("Error getting season_avg")
            return None
    def get_career_avg(self):
        try:
            df: pd.DataFrame = self.career_stats['career_totals_regular_season']
            vals = df[['GP', self.bovada_stat]+self.stats].values[0]
            games_played = vals[0]
            arr: np.ndarray = (vals/games_played)[1:]
            attrs = ['total'] + self.stats
            return { a: round(arr[index], 2) for index, a in enumerate(attrs) }
        except Exception:
            logging.error("Error getting career_avg")
            return None
    def get_season_avg_post(self):
        try:
            if self.playoffs_started:
                df: pd.DataFrame = self.career_stats['season_totals_post_season']
                vals = df[df['SEASON_ID'].str.contains(str(self.current_season))][['GP', self.bovada_stat]+self.stats].values[0]
                games_played = vals[0]
                arr: np.ndarray = (vals/games_played)[1:]
                attrs = ['total'] + self.stats
                return { a: round(arr[index], 2) for index, a in enumerate(attrs) }
            logging.info("season_avg_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting season_avg_post")
            return None
    def get_career_avg_post(self):
        try:
            if self.playoffs_started:
                df: pd.DataFrame = self.career_stats['career_totals_post_season']
                vals = df[['GP', self.bovada_stat]+self.stats].values[0]
                games_played = vals[0]
                arr: np.ndarray = (vals/games_played)[1:]
                attrs = ['total'] + self.stats
                return { a: round(arr[index], 2) for index, a in enumerate(attrs) }
            logging.info("career_avg_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting career_avg_post")
            return None
    def get_season_rank(self):
        try:
            if self.bovada_stat != "total_personal_fouls".upper(): # no personal fouls ranks
                df: pd.DataFrame = self.career_stats['season_rankings_regular_season']
                df = df[df['SEASON_ID'].str.contains(str(self.current_season))]
                rank_cols = [f"RANK_{col}" for col in self.stats]
                ranks = df[rank_cols]
                ranks.columns = self.stats
                _dict = ranks.to_dict(orient="records")[0]
                new_dict = {}
                new_dict["total"] = round(np.mean(ranks.values[0]), 2)
                new_dict.update(_dict)
                return new_dict
            return None
        except Exception as e:
            logging.error(f"Error getting season_rank: {e}")
            return None
    def get_season_rank_post(self):
        try:
            if self.playoffs_started:
                if self.bovada_stat != "total_personal_fouls".upper(): # no personal fouls ranks
                    df: pd.DataFrame = self.career_stats['season_rankings_post_season']
                    df = df[df['SEASON_ID'].str.contains(str(self.current_season))]
                    rank_cols = [f"RANK_{col}" for col in self.stats]
                    ranks = df[rank_cols]
                    ranks.columns = self.stats
                    _dict = ranks.to_dict(orient="records")[0]
                    new_dict = {}
                    new_dict["total"] = round(np.mean(ranks.values[0]), 2)
                    new_dict.update(_dict)
                    return new_dict
                return None
            logging.info("season_rank_post - playoffs haven't started yet.")
            return None
        except Exception:
            logging.error("Error getting season_rank_post")
            return None
    def get_avg_and_rank_table_data(self):
        data = {
            'season_avg': self.get_season_avg(),
            'career_avg': self.get_career_avg(),
            'season_avg_post': self.get_season_avg_post(),
            'career_avg_post': self.get_season_avg_post(),
            'season_rank': self.get_season_rank(),
            'season_rank_post': self.get_season_rank_post(),
        }
        return data
    def get_same_game_props(self):
        df: pd.DataFrame = self.props_df[
            (self.props_df['id']==self.game_id)& # same game
            (self.props_df['player_id']==self.player_id)& # same player
            (self.props_df['stat']==self.raw_stat) # same stat
        ].sort_values(by=['date_collected_obj'], ascending=True)
        df = df[['id', 'date_collected', 'line_value', 'over_odds', 'under_odds']]
        return json.loads(df.to_json(orient='records'))
    def get_player_bet_data(self):
        df: pd.DataFrame = self.all_gamelogs.copy()[['GAME_DATE', 'MATCHUP']+self.stats].head(10)
        df = df.sort_values(by=['GAME_DATE'], ascending=True)
        df[self.bovada_stat.upper()] = df.apply(lambda x: sum(x[self.stats]), axis=1)
        # construct response JSON (dict)
        res = {
            'primary_key': self.bet_slip_object['bet']['primary_key'],
            'playoffs_started': self.playoffs_started,
            'hit_last_5_games': self.get_hits_last_n(5),
            'hit_last_10_games': self.get_hits_last_n(10),
            'hit_last_20_games': self.get_hits_last_n(20),
            'avg_and_rank_table_data': self.get_avg_and_rank_table_data(),
            'last_10_stats': json.loads(df.to_json(orient='records')),
            'same_game_props': self.get_same_game_props()
        }
        return res
# END BetResponseObj

class Bets:
    def __init__(self, data: list[dict]):
        self.data = data
        self.data = [item for item in self.data if 'bet' in item]
        self.player_ids = [d['bet']['player_id'] for d in self.data if 'id' in d]
        self.player_data = { pid: PlayerDataObj(pid) for pid in self.player_ids }
        self.props_df: pd.DataFrame = self.get_props()
        self.props_df['date_collected_obj'] = self.props_df['date_collected'].apply(lambda x: datetime.strptime(x, DATETIME_FORMAT))
        return
    def get_props(self):
        # return get_dynamo_table_dataframe('nba_props')
        return pd.read_csv("nba_props.csv")
    def get_data(self):
        responses = []
        for pid in self.player_ids:
            player_data = self.player_data[pid]
            bets = [item for item in self.data if item['bet']['player_id']==pid]
            for bet in bets:
                try:
                    responses.append(
                        BetResponseObj(
                            bet, 
                            player_data,
                            self.props_df[self.props_df['player_id']==pid]
                        ).get_player_bet_data()
                    )
                except Exception as e:
                    logging.error(f"Error getting bet response for {pid} : {bet['bet']['primary_key']} : {e}")
        return responses

if __name__=="__main__":
    setup_logging()
    start = time.time()
    data = json.load(open("dummy_bet_slip_submit_data.json", "r"))
    # data = INITIAL_VALUES
    bets = Bets(data)
    json.dump(bets.get_data(), open("dummy_bets_info_response.json", "w"), indent=4)
    end = time.time()
    elapsed = end - start
    print(f"Time elapsed: {elapsed}")