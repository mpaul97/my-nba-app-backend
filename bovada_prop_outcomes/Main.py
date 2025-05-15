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
from botocore.exceptions import NoCredentialsError

from nba_api.stats.static.players import find_players_by_full_name

from PlayerProps import PlayerProps
from BovadaPropOutcomes import BovadaPropOutcomes

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
        self.data_dir: str = "./data/bovada_data/"
        os.makedirs(self.data_dir, exist_ok=True)
        self.s3_data_dir: str = "./s3_data/"
        os.makedirs(self.s3_data_dir, exist_ok=True)
        self.found_players = {}
        self.s3_loaded_files = {}
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
    def get_s3_file(self, key: str):
        try:
            response = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            file_content = response["Body"].read().decode("utf-8")
            if re.search(r".+.csv", key): # csv files
                return pd.read_csv(StringIO(file_content))
            elif re.search(r".+.json", key): # json files
                return json.loads(file_content)
            return file_content # return just reponse body for other files
        except Exception as e:
            logging.error(f"File not found in S3 Bucket: {BUCKET_NAME}, {key}: {e}")
            return None
    def load_existing_files(self, write_files: bool = False):
        res = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="data/")
        try:
            for object in res['Contents']:
                s3_key = object['Key']
                file = os.path.basename(s3_key)
                fn, extension = os.path.splitext(file) # filename and extensions split
                self.s3_loaded_files[fn] = self.get_s3_file(s3_key)
                if write_files:
                    logging.info(f"Writing s3_loaded_files[{fn}] to s3_data_dir")
                    if re.search(r"\.json", extension):
                        json.dump(self.s3_loaded_files[fn], open(f"{self.s3_data_dir}{file}", "w"), indent=4)
                    elif re.search(r"\.csv", extension):
                        self.s3_loaded_files[fn].to_csv(f"{self.s3_data_dir}{file}", index=False)
                logging.info(f"{file} added to s3_loaded_files dict")
        except KeyError:
            logging.info(f"Folder: data/ is empty in {BUCKET_NAME}. Writing all new data.")
        return
    def s3_parsed_logs_exists(self, _dir: str):
        if len(self.s3_loaded_files) == 0: # no files in dict, download all
            return True
        if self.s3_loaded_files['all_parsed_logs'].empty: # empty frame, download all
            return True
        if _dir not in self.s3_loaded_files['all_parsed_logs']['path'].values: # _dir has not been downloaded
            return True
        return False
    def download_bovada_data(self):
        logs: dict[pd.DataFrame] = {}
        res = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bovada_data/")
        for index, object in enumerate(res['Contents']):
            self.print_progress_bar(index, len(res['Contents']))
            s3_key = object['Key']
            _dir: str = os.path.dirname(s3_key).replace("bovada_data/", "")
            if self.s3_parsed_logs_exists(_dir):
                fn: str = os.path.basename(object['Key'])
                if re.search(r"all_bets.+\.json", s3_key):
                    os.makedirs(f"{self.data_dir}/{_dir}", exist_ok=True)
                    s3.download_file(BUCKET_NAME, object['Key'], f"{self.data_dir}/{_dir}/{fn}")
                elif re.search(r".+\.csv", s3_key):
                    os.makedirs(f"{self.data_dir}/{_dir}", exist_ok=True)
                    league_type = str(os.path.dirname(s3_key)).split("/")[-1]
                    df: pd.DataFrame = self.get_s3_file(s3_key)
                    df.insert(0, 'path', _dir)
                    df.to_csv(f"{self.data_dir}/{_dir}/{fn}", index=False)
                    if league_type in logs:
                        logs[league_type] = pd.concat([logs[league_type], df])
                    else:
                        logs[league_type] = df
        # if logs empty return False, end program
        if len(logs) == 0:
            logging.info("All files up-to-date. Nothing new to download. Terminating.")
            return False
        for key in logs:
            new_df: pd.DataFrame = logs[key].drop_duplicates()
            new_df.insert(0, 'bovada_date', new_df['link'].apply(lambda x: datetime.strptime(x.split("-")[-1], "%Y%m%d%H%M")))
            new_df = new_df.sort_values(by=['bovada_date'])
            new_df.to_csv(f"{self.s3_data_dir}all_game_logs_{key}.csv", index=False)
        return True
    def create_parsed_log(self):
        parsed_logs = []
        for fn in os.listdir(self.s3_data_dir):
            if re.search(r".+.csv", fn):
                temp_df: pd.DataFrame = pd.read_csv(f"{self.s3_data_dir}{fn}")
                parsed_logs.append(temp_df[['bovada_date', 'path', 'key']])
        df = pd.concat(parsed_logs)
        df = df.sort_values(by=['bovada_date'])
        if len(self.s3_loaded_files) != 0:
            logging.info(f"Only getting data for batch range: {list(set(df['path'].values))}")
        df.to_csv(f"{self.s3_data_dir}all_parsed_logs.csv", index=False)
        return
    def get_player_id(self, player_name: str, league_type: str):
        try:
            if league_type == "nba":
                if player_name in self.found_players:
                    logging.debug(f"Getting {player_name} from found_players")
                    return self.found_players[player_name]
                players = find_players_by_full_name(player_name)
                if len(players) > 1:
                    logging.error(f"ALERT found multiple players for {player_name}")
                    return np.nan
                player_id = players[0]['id']
                if player_name not in self.found_players:
                    self.found_players[player_name] = player_id
                logging.debug(f"Found player_id:{player_id} for {player_name} and added to found_players")
                time.sleep(0.6)
                return player_id
        except Exception as e:
            print(f"ERROR getting player_id: {e}")
            return np.nan
        return np.nan
    def generate_props(self):
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
                    _date: str = re.sub(r"bovada_data\\", "", _date)
                    _date: str = re.sub(r"bovada_data\/", "", _date)
                    df: pd.DataFrame = PlayerProps(json.load(open(file_path, "r"))).get_props()
                    date_collected: str = f"{_date}T{batch_hour_num}"
                    df.insert(0, 'date_collected', datetime.strptime(date_collected, "%y-%m-%dT%H"))
                    df.insert(1, 'date_collected_string', date_collected)
                    df.insert(1, 'batch_num', batch_hour_num)
                    # TODO GET player_id for other leagues
                    try: # get games_log for batch, insert link, bovada_date, and player_id to all_players_props
                        games = pd.concat([
                            pd.read_csv(f"{file_dir}/{f}") for f in os.listdir(file_dir) \
                                if re.search(r"games_log.+\.csv", f)
                        ])
                        f_key: str = fn.replace("all_bets_", "")
                        link = games[games['key']==f_key]['link'].values[0]
                        df.insert(2, 'link', link)
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
            df = df.sort_values(by=['bovada_date'])
            data = json.loads(df.to_json(orient="records"))
            json.dump(data, open(f"{self.s3_data_dir}all_player_props_{key}.json", "w"), indent=4)
        return
    def get_props(self, league: str):
        return json.load(open(f"{self.s3_data_dir}all_player_props_{league}.json", "r"))
    def delete_data_dir(self):
        """Deletes all files and subdirectories within the given directory path.

        Args:
            dir_path: The path to the directory to be cleared.
        """
        if not os.path.exists(self.data_dir):
            logging.error(f"Directory not found: {self.data_dir}")
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
    def upload_s3_data(self):
        for fn in os.listdir(self.s3_data_dir):
            local_file: str = f"{self.s3_data_dir}{fn}"
            s3_file: str = f"data/{fn}"
            try:
                s3.upload_file(local_file, BUCKET_NAME, s3_file)
                logging.info(f"Upload Successful: {local_file} to s3://{BUCKET_NAME}/{s3_file}")
            except FileNotFoundError:
                logging.error(f"The file {local_file} was not found")
            except NoCredentialsError:
                logging.error("Credentials not available")
        return
    def run(self):
        # load existing files
        self.load_existing_files(write_files=True)
        # ------------------------------
        # download data
        has_downloaded = self.download_bovada_data()
        if has_downloaded:
            # ------------------------------
            # create download/parsed log
            self.create_parsed_log()
            # ------------------------------
            # generate props, store in s3_data/
            self.generate_props()
            # ------------------------------
            # generate outcomes
            # TODO other leagues
            df: pd.DataFrame = BovadaPropOutcomes(self.get_props('nba')).get_outcomes()
            df.to_csv(f"{self.s3_data_dir}outcomes_nba.csv", index=False)
            # ------------------------------
            # clear and delete data/
            self.delete_data_dir()
            # ------------------------------
            # upload s3_data to BUCKET_NAME/data/
            self.upload_s3_data()
        return
# END Main

if __name__=="__main__":
    Main().generate_props()
    # m = Main()
    # props = m.get_props('nba')
    # print(set([p['bovada_date'] for p in props]))
    # # BovadaPropOutcomes()