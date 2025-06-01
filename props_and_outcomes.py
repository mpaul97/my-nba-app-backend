import pandas as pd
import numpy as np
import os
from datetime import datetime, date
from dotenv import load_dotenv
import os
import json
import time

from const import DATETIME_FORMAT
from aws import get_dynamo_table_dataframe

class PropsAndOutcomes:
    def __init__(self, league: str):
        self.league = league
        self.props_df: pd.DataFrame = self.get_props()
        self.outcomes_df: pd.DataFrame = self.get_outcomes()
        self.outcomes_df = self.outcomes_df[~self.outcomes_df['stat'].str.contains('1stquarter')]
        self.start_date = datetime.now()
        # self.start_date = datetime.strptime("20/05/2025, 20:00:00", DATETIME_FORMAT)
        self.props_df['bovada_datetime_obj'] = self.props_df['bovada_date'].apply(lambda x: datetime.strptime(x, DATETIME_FORMAT))
        # ONLY upcoming props/games
        self.props_df = self.props_df[self.props_df['bovada_datetime_obj']>self.start_date]
        self.players_df = self.props_df[['player_name', 'player_id']].drop_duplicates()
        self.stat_outcome_distributions = self.get_stat_outcome_distributions()
        # PASS highest hitting N stats
        data = self.get_top_stat_outcomes_sample([s['stat'] for s in self.stat_outcome_distributions[:2]])
        json.dump(data, open("top_stat_outcomes_samples.json", "w"), indent=4)
        return
    def get_props(self):
        # return get_dynamo_table_dataframe('nba_props')
        return pd.DataFrame(data=json.load(open(f"{self.league}_props.json", "r")))
    def get_outcomes(self):
        # return get_dynamo_table_dataframe('nba_outcomes')
        return pd.DataFrame(data=json.load(open(f"{self.league}_outcomes.json", "r")))
    def get_player_outcome_distributions(self):
        df: pd.DataFrame = self.outcomes_df.copy()[['id', 'player_id', 'stat', 'outcome']].drop_duplicates()
        df_list = []
        for index, row in self.players_df.iterrows():
            player_name, player_id = row[['player_name', 'player_id']]
            temp_df: pd.DataFrame = df.copy()[df['player_id']==player_id]
            # Get counts and group totals
            outcome_counts = temp_df.groupby(by=['stat'])['outcome'].value_counts().reset_index(name='count')
            group_totals = temp_df.groupby(by=['stat']).size().reset_index(name='group_total')
            # Merge and calculate proportion
            outcome_counts = outcome_counts.merge(group_totals, on='stat')
            outcome_counts['proportion'] = round((outcome_counts['count'] / outcome_counts['group_total']) * 100.0, 2)
            outcome_counts['weighted_proportion'] = round((outcome_counts['count'] * outcome_counts['proportion']) / 100.0, 2)
            # Add name and id
            outcome_counts.insert(0, 'player_name', player_name)
            outcome_counts.insert(1, 'player_id', player_id)
            df_list.append(outcome_counts)
        new_df = pd.concat(df_list)
        # Sort and save
        new_df = new_df.sort_values(by=['weighted_proportion'], ascending=False)
        new_df.to_csv("player_outcome_distributions.csv", index=False)
        return json.loads(new_df.to_json(orient='records'))
    def get_stat_outcome_distributions(self):
        df: pd.DataFrame = self.outcomes_df.copy()[['id', 'player_id', 'stat', 'outcome']].drop_duplicates()
        # Get counts and group totals
        outcome_counts = df.groupby(by=['stat'])['outcome'].value_counts().reset_index(name='count')
        group_totals = df.groupby(by=['stat']).size().reset_index(name='group_total')
        # Merge and calculate proportion
        outcome_counts = outcome_counts.merge(group_totals, on='stat')
        outcome_counts['proportion'] = round((outcome_counts['count'] / outcome_counts['group_total']) * 100.0, 2)
        outcome_counts['weighted_proportion'] = round((outcome_counts['count'] * outcome_counts['proportion']) / 100.0, 2)
        # Sort and save
        outcome_counts = outcome_counts.sort_values(by=['weighted_proportion'], ascending=False)
        outcome_counts.to_csv("stat_outcome_distributions.csv", index=False)
        return json.loads(outcome_counts.to_json(orient='records'))
    def get_top_stat_outcomes_sample(self, stats: list):
        df = self.outcomes_df.copy()[self.outcomes_df['stat'].isin(stats)]
        return json.loads(df.sample(n=10).to_json(orient='records'))
# END PropsAndOutcomes

# if __name__ == "__main__":
#     pao = PropsAndOutcomes('nba')
#     pao.get_player_outcome_distributions()
#     pao.get_stat_outcome_distributions()