import pandas as pd
import numpy as np
import os
from datetime import datetime, date
from dotenv import load_dotenv
import os
import json

from nba_api.stats.static.players import find_players_by_first_name, find_players_by_last_name, find_players_by_full_name, get_players
from nba_api.stats.endpoints import playercareerstats, playergamelog

def get_gamelogs(player_id: int):
    """
    Get player gamelogs in JSON format for most current season, last season
    Both regular season and playoffs
    when month is greater than July use current year else current year is -1
    """
    now = datetime.now()
    curr_month, curr_season = now.month, now.year
    if curr_month < 7: # before august
        curr_season -= 1
    data = {
        "current_season_reg": playergamelog.PlayerGameLog(player_id, str(curr_season), r"Regular Season").get_dict(),
        "current_season_po": playergamelog.PlayerGameLog(player_id, str(curr_season), r"Playoffs").get_dict(),
        "last_season_reg": playergamelog.PlayerGameLog(player_id, str(curr_season-1), r"Regular Season").get_dict(),
        "last_season_po": playergamelog.PlayerGameLog(player_id, str(curr_season-1), r"Playoffs").get_dict()
    }
    return data

def get_curr_gamelog_frames(player_id: int):
    """
    Get current season and playoff gameslogs
    """
    now = datetime.now()
    curr_month, curr_season = now.month, now.year
    if curr_month < 7: # before august
        curr_season -= 1
    df_list = []
    reg: pd.DataFrame = playergamelog.PlayerGameLog(player_id, str(curr_season), r"Regular Season").get_data_frames()[0]
    if not reg.empty:
        df_list.append(reg)
    po: pd.DataFrame = playergamelog.PlayerGameLog(player_id, str(curr_season), r"Playoffs").get_data_frames()[0]
    if not po.empty:
        df_list.append(po)
    return pd.concat(df_list)

def get_bet_info(player_id: int):
    df = get_curr_gamelog_frames(player_id)
    df['GAME_DATE'] = df['GAME_DATE'].apply(lambda x: datetime.strptime(x, "%b %d, %Y").date())
    print(df)
    return

if __name__=="__main__":
    # json.dump(get_gamelogs(1629029), open("dummy_player_data.json", "w"), indent=4)
    get_bet_info(1629029)