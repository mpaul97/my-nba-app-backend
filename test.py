from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import json
import pandas as pd
import random

from nba_api.stats.static.players import find_players_by_first_name, find_players_by_last_name, find_players_by_full_name, get_players
from nba_api.stats.endpoints import playercareerstats, playergamelog, scoreboardv2, boxscoreplayertrackv3, boxscoretraditionalv3

from const import DATETIME_FORMAT
from aws import get_dynamo_table_dataframe

load_dotenv()

def get_all_players():
    return get_players()

def find_players(name: str):
    players = find_players_by_first_name(f"{name}")
    [players.append(p) for p in find_players_by_last_name(f"{name}") if p not in players]
    [players.append(p) for p in find_players_by_full_name(f"{name}") if p not in players]
    return players

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

def get_gamelogs_frame(player_id: int):
    """
    Get player gamelogs in JSON format for most current season, last season
    Both regular season and playoffs
    when month is greater than July use current year else current year is -1
    """
    now = datetime.now()
    curr_month, curr_season = now.month, now.year
    if curr_month < 7: # before august
        curr_season -= 1
    pairs = [
        { 'type': 'current_season_reg', 'year': str(curr_season), 'regex': r"Regular Season" },
        { 'type': 'current_season_po', 'year': str(curr_season), 'regex': r"Playoffs" },
        { 'type': 'last_season_reg', 'year': str(curr_season-1), 'regex': r"Regular Season" },
        { 'type': 'last_season_po', 'year': str(curr_season-1), 'regex': r"Playoffs" },
    ]
    df_list = []
    for p in pairs:
        for df in playergamelog.PlayerGameLog(player_id, p['year'], p['regex']).get_data_frames():
            df.insert(0, 'type', p['type'])
            df_list.append(df)
    df = pd.concat(df_list)
    print(df)
    return

def get_boxscores(date):
    games = scoreboardv2.ScoreboardV2("0", "2025-05-07", "00")
    df: pd.DataFrame = games.get_data_frames()[0]
    ids: list = list(df['GAME_ID'].values)
    for id in ids:
        print(boxscoretraditionalv3.BoxScoreTraditionalV3(id).get_data_frames()[0])
    return

def make_bet_slip_submit_data():
    props_df = pd.DataFrame(data=json.load(open("nba_props.json", "r")))
    props_df = props_df[props_df['stat']!="total_points-1stquarter"]
    props_df['bovada_datetime_obj'] = props_df['bovada_date'].apply(lambda x: datetime.strptime(x, DATETIME_FORMAT))
    props_df = props_df.sort_values(by=['bovada_datetime_obj'], ascending=False)
    top_id = props_df['id'].values[0]
    props_df: pd.DataFrame = props_df[props_df['id']==top_id].sample(n=4)
    props_df['player_id'] = props_df['player_id'].astype(int)
    props_df = props_df.drop(columns=['bovada_datetime_obj'])
    data = [
        {
            "separator": True
        }
    ]
    sample_data = json.loads(props_df.to_json(orient='records'))
    user_options = ['under', 'over']
    for index, item in enumerate(sample_data):
        data.append({
            'id': index + 1,
            'bet': item,
            'user_option': user_options[random.randrange(0, len(user_options))]
        })
    json.dump(data, open("dummy_bet_slip_submit_data.json", "w"), indent=4)
    return

def write_dynamo_props_and_outcomes_json(league: str):
    for suffix in ['_props', '_outcomes']:
        table_name: str = f"{league}{suffix}"
        df = get_dynamo_table_dataframe(table_name)
        json.dump(json.loads(df.to_json(orient='records')), open(f"{table_name}.json", "w"), indent=4)
    return

if __name__=="__main__":
    # json.dump(get_all_players(), open("dummy_all_players.json", "w"), indent=4)
    # json.dump(find_players("luka"), open("dummy_players.json", "w"), indent=4)
    # json.dump(get_gamelogs(1629029), open("dummy_player_data.json", "w"), indent=4)
    # get_gamelogs_frame(1629029)
    # print(playergamelog.PlayerGameLog(player_id=1629029, date_to_nullable=datetime.now().date()).get_data_frames())
    # get_boxscores(date=datetime.now()-timedelta(days=1))
    # make_bet_slip_submit_data()
    write_dynamo_props_and_outcomes_json('mlb')