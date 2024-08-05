from metabase import Mb_Client
from dotenv import dotenv_values
import pandas as pd
import datetime


class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}",
            username=secrets["username"],
            password=secrets["password"]
        )

    def get_query(self, query_name: str, params: dict = {}) -> str:
        sql_req: str = open(f"queries/{query_name}.sql").read()
        return sql_req.format(**params) if bool(params) else sql_req

    def get_payload(self, query: str) -> dict:
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

    def convert_string_int2int(self, value: str) -> int:
        return int(value.replace(',', ''))

    def get_active_experiments(self) -> pd.DataFrame:
        query = self.get_query("get_active_exps")
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        df = pd.json_normalize(query_result)
        df.id = df.id.apply(self.convert_string_int2int)
        df.date_start = df.date_start.apply(self.convert_string_int2int)
        df.date_end = df.date_end.apply(self.convert_string_int2int)
        df.variations = df.variations.apply(self.convert_string_int2int)
        return df

    def get_experiment(self, id) -> dict:
        query = self.get_query("get_exp_info", params=dict({"id": id}))
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        df = pd.json_normalize(query_result)
        df.id = df.id.apply(self.convert_string_int2int)
        df.date_start = df.date_start.apply(self.convert_string_int2int)
        df.date_end = df.date_end.apply(self.convert_string_int2int)
        df.variations = df.variations.apply(self.convert_string_int2int)
        exp_info: dict = {
            "id": df.id[0],
            "date_start": df.date_start[0],
            "date_end": df.date_end[0],
            "variations": df.variations[0],
            "experiment_event_start": df.experiment_event_start[0],
            "configuration": df.configuration[0]
        }
        return exp_info

    def get_users(self, exp_info: pd.DataFrame) -> pd.DataFrame:
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"])
        exp_end_dt = datetime.datetime.today()
        if exp_info["date_end"] > 0:
            exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_start"])
        # print(exp_start_dt.date())
        # print(exp_end_dt.date())
        # print((exp_end_dt.date() - exp_start_dt.date()).days + 1)
        for day in range((exp_end_dt.date() - exp_start_dt.date()).days + 1):
            current_day = exp_start_dt + datetime.timedelta(days=day)
            print(current_day.strftime('%Y-%m-%d'))
            params=dict({
                "exp_id": exp_info["id"],
                "date": current_day.strftime("%Y-%m-%d"),
                "custom_confirm_event": exp_info["experiment_event_start"],
                "platform": "all",
                "custom_confirm_include_values": "",
                "custom_confirm_exclude_values": "",
                "pro_rights": "Free",
                "edu_rights": "all",
                "sing_rights": "all",
                "practice_rights": "all",
                "book_rights": "all",
                "country": "all",
                "source": "all"
            })
            query = self.get_query("get_exp_users", params)
            payload = self.get_payload(query)
            query_result = self._mb_client.post("dataset/json", payload)
            df = pd.json_normalize(query_result)
            df.unified_id = df.unified_id.apply(self.convert_string_int2int)
            df.session_id = df.session_id.apply(self.convert_string_int2int)
            df.exp_start_dt = df.exp_start_dt.apply(self.convert_string_int2int)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df

    def get_subscriptions(self, exp_info: pd.DataFrame) -> pd.DataFrame:
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"])
        exp_end_dt = datetime.datetime.today()
        for day in range((exp_end_dt.date() - exp_start_dt.date()).days + 1):
            current_day = exp_start_dt + datetime.timedelta(days=day)
            print(current_day.strftime('%Y-%m-%d'))
            params=dict({
                "date": current_day.strftime("%Y-%m-%d"),
                "funnel_source_include": "",
                "funnel_source_exclude": ""
            })
            query = self.get_query("get_exp_subscriptions", params)
            payload = self.get_payload(query)
            query_result = self._mb_client.post("dataset/json", payload)
            df = pd.json_normalize(query_result)
            df.payment_account_id = df.payment_account_id.apply(self.convert_string_int2int)
            df.unified_id = df.unified_id.apply(self.convert_string_int2int)
            df.subscribed_dt = df.subscribed_dt.apply(self.convert_string_int2int)
            df.charge_dt = df.charge_dt.apply(self.convert_string_int2int)
            df.cancel_dt = df.cancel_dt.apply(self.convert_string_int2int)
            df.refund_dt = df.refund_dt.apply(self.convert_string_int2int)
            df.upgrade_dt = df.upgrade_dt.apply(self.convert_string_int2int)
            full_df = pd.concat([full_df, df], ignore_index=True)
        return full_df