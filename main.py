from flask import Flask, jsonify, request
from datetime import datetime
from dotenv import load_dotenv
import os
from flask_cors import CORS, cross_origin
import logging
import json

import logging_config
from bets import Bets, run as bets_run

from nba_api.stats.static.players import find_players_by_first_name, find_players_by_last_name, find_players_by_full_name, get_players
from nba_api.stats.endpoints import playercareerstats, playergamelog

load_dotenv()

logging_config.setup_logging()

DEBUG_MODE = os.environ['DEBUG_MODE']=="True"

ROOT_PATH=os.environ['ROOT_PATH']

app = Flask(__name__)
cors = CORS(app) # allow CORS for all domains on all routes.
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route(f'/{ROOT_PATH}/find_players/<name>', methods=['GET'])
def find_players(name: str):
    players = find_players_by_first_name(f"{name}")
    [players.append(p) for p in find_players_by_last_name(f"{name}") if p not in players]
    [players.append(p) for p in find_players_by_full_name(f"{name}") if p not in players]
    return jsonify(players)

@app.route(f'/{ROOT_PATH}/get_gamelogs/<player_id>', methods=['GET'])
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

@app.route(f'/{ROOT_PATH}/get_all_players', methods=['GET'])
def get_all_players():
    return get_players()

@app.route(f'/{ROOT_PATH}/post_bet_info', methods=['GET', 'POST'])
def post_bet_info():
    data = request.get_json()
    return bets_run(data)

if __name__=="__main__":
    app.run(debug=True)