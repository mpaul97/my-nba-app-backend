import pandas as pd
import regex as re
import logging
import json

# from logging_config import setup_logging

# setup_logging()

class PlayerProps:
    def __init__(self, data: dict):
        self.data = data
        # props dataframe - player_name, team_abbr, stat, line_value, over_odds, under_odds
        self.keys = {
            'Over', 'Under', 'index', 'data_type',
            'column', 'player_name', 'team_abbr'
        }
        return
    def get_normal_prop(self, key: str, vals: dict, _dict: dict):
        key = re.sub(r"\xa0", " ", key)
        if re.search(r"-\s.{3}\sQuarter", key): # Bam Adebayo   - 1st Quarter
            quarter = str(re.findall(r"-\s.{3}\sQuarter", key)[0]).strip()
            quarter = re.sub(r"\s+", "", quarter).lower()
            _dict['stat'] = (key.split(" - ")[0]).lower().replace(" ", "_") + quarter
        else:
            _dict['stat'] = (key.split(" - ")[0]).lower().replace(" ", "_")
        _dict['line_value'] = vals['index'][0]
        _dict['over_odds'] = vals['Over'][0]
        _dict['under_odds'] = vals['Under'][0]
        return _dict
    def get_milestones_prop(self, key: str, vals: dict, _dict: dict):
        """
        Nothing now, maybe fill later
        """
        return _dict
    def get_props(self):
        props: list = []
        key: str
        for key in self.data:
            vals = self.data[key]
            try:
                _dict: dict = {}
                _dict['player_name'] = vals['player_name']
                _dict['team_abbr'] = vals['team_abbr']
                try:
                    if 'Milestones' not in key and 'Alternate Strikeouts' not in key:
                        if all([vals[key] for key in vals]): # make sure all vals are filled
                            props.append(self.get_normal_prop(key, vals, _dict))            
                    # elif 'Milestones' in key:
                    #     self.get_milestones_prop(key, vals, _dict)
                except Exception as e:
                    logging.error(f"Error getting player props for table: {key}, {e}")
            except:
                pass
        return pd.DataFrame(data=props)
# END PlayerProps

# if __name__ == "__main__":
#     data = json.load(open("bets.json", "r"))
#     PlayerProps(data).get_props().to_csv("%s.csv" % "player_props", index=False)