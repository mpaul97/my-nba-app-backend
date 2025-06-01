import pandas as pd
import json
import asyncio
from datetime import datetime
import time
import os

from nba_api.stats.static.players import find_players_by_full_name
from nba_api.stats.endpoints.commonplayerinfo import CommonPlayerInfo
from nba_api.stats.endpoints.playergamelog import PlayerGameLog
from nba_api.stats.endpoints.playercareerstats import PlayerCareerStats

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
        return seasons[-2:]
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
    def as_dict(self):
        data = self.__dict__
        for key in data:
            if type(data[key]) is pd.DataFrame:
                data[key] = json.loads(data[key].to_json(orient='records'))
            if type(data[key]) is dict:
                temp_data = data[key]
                for key_1 in temp_data:
                    temp_data[key_1] = json.loads(temp_data[key_1].to_json(orient='records'))
                data[key] = temp_data
        return data
# END PlayerDataObj

# if __name__ == "__main__":
#     pdo = PlayerDataObj(1630169)
#     print(pdo.as_dict())