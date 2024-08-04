from src.metabase import Mb_Client
from dotenv import dotenv_values
import pandas as pd


def get_query(query_name, params) -> str:
        sql_req = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params)

def get_payload(query) -> dict:
    payload: dict = {
        "database": 2,
        "type": "native",
        "format_rows": False,
        "pretty": False,
        "native": {
            "query": query
        }
    }
    return payload

secrets: dict = dotenv_values(".env")

mb_client: Mb_Client = Mb_Client(
    url=f"{secrets['mb_url']}",
    username=secrets["username"],
    password=secrets["password"]
)

query = get_query("get_exp_info", params=dict({"id": 4821}))
payload = get_payload(query)
query_result = mb_client.post("dataset/json", payload)
# query_result = mb_client.post("dataset/csv", payload)
# query_result = mb_client.post("dataset/native", payload)
df = pd.json_normalize(query_result)
int(df.id[0].replace(',', ''))


payload: dict = {
    "database": 2,
    "type": "native",
    "format_rows": False,
    "native": {
        "query": """
            select count()
            from default.ug_rt_events_app
            where date = today()
        """,
    },
}
print(mb_client.post("dataset/json", payload))

# get active exps
