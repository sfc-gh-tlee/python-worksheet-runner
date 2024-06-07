
def query_id_to_snovi_url(query_id: str):
    base_url = "https://snowflake.awsuswest2temptest000006.external-zone.snowflakecomputing.com:8085/console#/monitoring/queries/detail?queryId="
    return base_url + query_id