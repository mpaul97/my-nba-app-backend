import awswrangler as wr
from datetime import datetime

def get_dynamo_table_dataframe(table_name: str):
    return wr.dynamodb.read_partiql_query(query=f'SELECT * FROM {table_name}')

def get_props_by_date(league: str, date: datetime):
    return wr.dynamodb.read_partiql_query(
        query=f"""
            SELECT *
            FROM {league}_props 
            WHERE bovada_date >= '{date}'
        """
    )