import awswrangler

def get_dynamo_table_dataframe(table_name: str):
    return awswrangler.dynamodb.read_partiql_query(query=f'SELECT * FROM {table_name}')