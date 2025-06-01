from flask import Flask, jsonify, request, session
from datetime import datetime
from dotenv import load_dotenv
import os
from flask_cors import CORS, cross_origin
import logging
import json
import simplejson
import boto3
from boto3.dynamodb.types import TypeDeserializer
import pandas as pd
import secrets

from const import DATETIME_FORMAT
from logging_config import setup_logging
from aws import get_dynamo_table_dataframe, get_props_by_date
from PlayerDataObj import PlayerDataObj
from bets import Bets

from nba_api.stats.static.players import find_players_by_first_name, find_players_by_last_name, find_players_by_full_name, get_players
from nba_api.stats.endpoints import playercareerstats, playergamelog

load_dotenv()

setup_logging()

DEBUG_MODE = os.environ['DEBUG_MODE']=="True"

app = Flask(__name__)
cors = CORS(app) # allow CORS for all domains on all routes.
app.config['CORS_HEADERS'] = 'Content-Type'
app.config['SECRET_KEY'] = secrets.token_hex(16)

@app.route(f'/find_players/<name>', methods=['GET'])
def find_players(name: str):
    players = find_players_by_first_name(f"{name}")
    [players.append(p) for p in find_players_by_last_name(f"{name}") if p not in players]
    [players.append(p) for p in find_players_by_full_name(f"{name}") if p not in players]
    return jsonify(players)

@app.route(f'/get_gamelogs/<player_id>', methods=['GET'])
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
    return jsonify(data)

@app.route(f'/get_all_players', methods=['GET'])
def get_all_players():
    return get_players()

@app.route(f'/get_player_data_obj/<player_id>', methods=['GET'])
def get_player_data_obj(player_id: int):
    data = PlayerDataObj(player_id).as_dict()
    if not data:
        return jsonify({
            "message": f"Error getting player data for {player_id}"
        }), 400
    return jsonify(data), 200

@app.route(f'/post_bet_info', methods=['GET', 'POST'])
def post_bet_info():
    data = request.get_json()
    return Bets(data).get_data()

@app.route(f'/get_table/<table_name>', methods=['GET'])
def get_tables(table_name: str):
    return json.loads(get_dynamo_table_dataframe(table_name).to_json(orient='records'))

@app.route(f'/get_upcoming_props/<league>', methods=['GET'])
def get_upcoming_props(league: str):
    today = datetime.now().date()
    df: pd.DataFrame = get_props_by_date(league, today)
    df['datetime_downloaded_obj'] = df['date_downloaded'].apply(lambda x: datetime.strptime(x, DATETIME_FORMAT))
    df = df.sort_values(by=['datetime_downloaded_obj'], ascending=True)
    df = df.drop_duplicates(subset=['player_id', 'stat', 'id'], keep='last')
    return json.loads(df.to_json(orient='records'))

if __name__=="__main__":
    app.run(debug=True)
    # json.dump(get_tables("nba_props"), open("nba_props.json", "w"), indent=4)
    # json.dump(get_tables("nba_outcomes"), open("nba_outcomes.json", "w"), indent=4)