import pandas as pd
import numpy as np
import json
import os
import regex as re
from datetime import datetime
import time
import logging
from dotenv import load_dotenv
import boto3
import shutil
from io import StringIO

from nba_api.stats.static.players import find_players_by_full_name

from PlayerProps import PlayerProps

from logging_config import setup_logging

pd.options.mode.chained_assignment = None

load_dotenv()
setup_logging()

BUCKET_NAME: str = os.environ['BUCKET_NAME']

s3 = boto3.client(
    's3',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
)

class Main:
    def __init__(self):
        self.data_dir: str = "./data/"
        os.makedirs(self.data_dir, exist_ok=True)
        self.clean_data_dir: str = "./clean_data/"
        os.makedirs(self.clean_data_dir, exist_ok=True)
        self.download_data()
        self.found_players = {}
        return
    def download_data(self):
        logs: dict[pd.DataFrame] = {}
        res = s3.list_objects_v2(Bucket=BUCKET_NAME)
        for object in res['Contents']:
            s3_key = object['Key']
            _dir: str = os.path.dirname(s3_key)
            if re.search(r"all_bets.+\.json", s3_key):
                pass
            #     os.makedirs(f"{self.data_dir}/{_dir}", exist_ok=True)
            #     fn: str = os.path.basename(object['Key'])
            #     s3.download_file(BUCKET_NAME, object['Key'], f"./{self.data_dir}/{_dir}/{fn}")
            elif re.search(r".+.csv", s3_key):
                league_type = str(os.path.dirname(s3_key)).split("/")[-1]
                csv_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
                body = csv_obj['Body']
                csv_string = body.read().decode('utf-8')
                df: pd.DataFrame = pd.read_csv(StringIO(csv_string))
                df.insert(0, 'path', _dir)
                if league_type in logs:
                    logs[league_type] = pd.concat([logs[league_type], df])
                else:
                    logs[league_type] = df
        for key in logs:
            new_df: pd.DataFrame = logs[key].drop_duplicates()
            new_df.insert(0, 'bovada_date', new_df['link'].apply(lambda x: datetime.strptime(x.split("-")[-1], "%Y%m%d%H%M")))
            new_df = new_df.sort_values(by=['bovada_date'])
            new_df.to_csv(f"{self.clean_data_dir}all_game_logs_{key}.csv", index=False)
        return
    def delete_data_dir(self):
        """Deletes all files and subdirectories within the given directory path.

        Args:
            dir_path: The path to the directory to be cleared.
        """
        if not os.path.exists(self.data_dir):
            print(f"Directory not found: {self.data_dir}")
            return

        for item in os.listdir(self.data_dir):
            item_path = os.path.join(self.data_dir, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)  # Remove files and symbolic links
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)  # Remove directories and their contents
            except Exception as e:
                print(f"Error deleting {item_path}: {e}")
        os.rmdir(self.data_dir)
        return
    def get_player_id(self, player_name: str, league_type: str):
        try:
            if league_type == "nba":
                if player_name in self.found_players:
                    print(f"Getting {player_name} from found_players")
                    return self.found_players[player_name]
                players = find_players_by_full_name(player_name)
                if len(players) > 1:
                    print(f"ALERT found multiple players for {player_name}")
                    return np.nan
                player_id = players[0]['id']
                if player_name not in self.found_players:
                    self.found_players[player_name] = player_id
                print(f"Found player_id:{player_id} for {player_name} and added to found_players")
                time.sleep(0.6)
                return player_id
        except Exception as e:
            print(f"ERROR getting player_id: {e}")
            return np.nan
        return np.nan
    def run(self):
        frames: dict = {}
        for root, _, files in os.walk(self.data_dir):
            for file in files:
                fn, extension = os.path.splitext(file) # filename and extensions split
                parent_path = os.path.dirname(root) # only directories of root dir
                relative_path = os.path.relpath(parent_path, './data/') # date and hour batch dirs
                league_type = os.path.basename(root) # base path above file
                file_path = os.path.join(root, file) # path to file
                file_dir = os.path.dirname(file_path)
                if re.search(r"all_bets.+\.json", file):
                    logging.info(f"Getting props from: {fn}")
                    _date, batch_hour_num = os.path.dirname(relative_path), os.path.basename(relative_path)
                    # print(f"Date: {_date}, Hour: {batch_hour_num}")
                    df: pd.DataFrame = PlayerProps(json.load(open(file_path, "r"))).get_props()
                    date_collected: str = f"{_date}T{batch_hour_num}"
                    df.insert(0, 'date_collected', datetime.strptime(date_collected, "%y-%m-%dT%H"))
                    df.insert(1, 'date_collected_string', date_collected)
                    df.insert(1, 'batch_num', batch_hour_num)
                    # add bovada link
                    try:
                        games = pd.concat([
                            pd.read_csv(f"{file_dir}/{f}") for f in os.listdir(file_dir) \
                                if re.search(r"games_log.+\.csv", f)
                        ])
                        f_key: str = fn.replace("all_bets_", "")
                        link = games[games['key']==f_key]['link'].values[0]
                        df.insert(2, 'link', link)
                        # df['date'] = datetime.strptime(item['link'].split("-")[-1], "%Y%m%d%H%M")
                        df.insert(0, 'bovada_date', df['link'].apply(lambda x: datetime.strptime(x.split("-")[-1], "%Y%m%d%H%M")))
                        if 'player_name' in df.columns:
                            df.insert(1, 'player_id', df['player_name'].apply(lambda x: self.get_player_id(x, league_type)))
                    except Exception as e:
                        df.insert(2, 'link', np.nan)
                    # assign to dict : frame value
                    if league_type in frames and not frames[league_type].empty and not df.empty:
                        frames[league_type] = pd.concat([frames[league_type], df])
                    else:
                        frames[league_type] = df
        for key in frames:
            df: pd.DataFrame = frames[key]
            df = df.sort_values(by=['date_collected'], ascending=False)
            data = json.loads(df.to_json(orient="records"))
            json.dump(data, open(f"{self.clean_data_dir}all_player_props_{key}.json", "w"), indent=4)
        # clear and delete data/
        self.delete_data_dir()
        return
# END Main

if __name__=="__main__":
    # Main()
    parsed_logs = []
    for fn in os.listdir("./clean_data/"):
        if re.search(r".+.csv", fn):
            temp_df: pd.DataFrame = pd.read_csv(f"./clean_data/{fn}")
            parsed_logs.append(temp_df[['bovada_date', 'path', 'key']])
    df = pd.concat(parsed_logs)
    df = df.sort_values(by=['bovada_date'])
    df.to_csv(f"./clean_data/parsed_logs.csv", index=False)