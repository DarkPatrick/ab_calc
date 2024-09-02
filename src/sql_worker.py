from metabase import Mb_Client
from dotenv import dotenv_values
import pandas as pd
import datetime
import math
import re


class SqlWorker():
    def __init__(self) -> None:
        secrets: dict = dotenv_values(".env")

        self._mb_client: Mb_Client = Mb_Client(
            url=f"{secrets['mb_url']}",
            username=secrets["username"],
            password=secrets["password"]
        )
    
    def generate_sql_rights_filter(self, rights_type: str, rights: str):
        rights_level_list = ['pro', 'edu', 'sing', 'practice', 'book']
        rights_level = int(math.pow(10, rights_level_list.index(rights_type)))
        rights_dict: dict = {
            'Free': f'toUInt32(rights / {rights_level}) % 10 in (0, 4, 5)',
            'Finite subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2)',
            'Lifetime': f'toUInt32(rights / {rights_level}) % 10 in (3)',
            'Any paid': f'toUInt32(rights / {rights_level}) % 10 in (2, 3)',
            'Any subscription': f'toUInt32(rights / {rights_level}) % 10 in (1, 2, 3)',
            'Trial': f'toUInt32(rights / {rights_level}) % 10 in (1)',
            'Expired subscription': f'toUInt32(rights / {rights_level}) % 10 in (5)',
            'Expired trial': f'toUInt32(rights / {rights_level}) % 10 in (4)',
            'Expired any': f'toUInt32(rights / {rights_level}) % 10 in (4, 5)',
            'All': f'1'
        }
        return rights_dict[rights]
        

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
        clients_pattern = r'(\w+)'
        df['clients_list'] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
        return df

    def get_experiments(self) -> pd.DataFrame:
        print('get exps')
        query = self.get_query("get_exps")
        payload = self.get_payload(query)
        query_result = self._mb_client.post("dataset/json", payload)
        print('got exps')
        df = pd.json_normalize(query_result)
        df.id = df.id.apply(self.convert_string_int2int)
        df.date_start = df.date_start.apply(self.convert_string_int2int)
        df.date_end = df.date_end.apply(self.convert_string_int2int)
        df.variations = df.variations.apply(self.convert_string_int2int)
        df.status = df.status.apply(self.convert_string_int2int)
        clients_pattern = r'(\w+)'
        df['clients_list'] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
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
        clients_pattern = r'(\w+)'
        df['clients_list'] = df.clients.apply(lambda x: re.findall(clients_pattern, x))
        exp_info: dict = {
            "id": df.id[0],
            "date_start": df.date_start[0],
            "date_end": df.date_end[0],
            "variations": df.variations[0],
            "experiment_event_start": df.experiment_event_start[0],
            "configuration": df.configuration[0],
            'clients_list': df.clients_list[0]
        }
        return exp_info

    def get_users(self, exp_info: pd.DataFrame, progress_bar, filters) -> pd.DataFrame:
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
        exp_end_dt = datetime.datetime.today()
        if exp_info["date_end"] > exp_info["date_start"]:
            exp_end_dt = datetime.datetime.fromtimestamp(exp_info["date_end"], datetime.timezone.utc)
            print(exp_end_dt)
        # print(exp_start_dt.date())
        # print(exp_end_dt.date())
        # print((exp_end_dt.date() - exp_start_dt.date()).days + 1)
        days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days + 1
        print(days_cnt)
        for day in range(days_cnt):
            progress_bar.progress(round(day / days_cnt * 100), text='getting users...')
            current_day = exp_start_dt + datetime.timedelta(days=day)
            print(current_day.strftime('%Y-%m-%d'))
            params=dict({
                "exp_id": exp_info["id"],
                "date": current_day.strftime("%Y-%m-%d"),
                'datetime_start': exp_info["date_start"],
                'datetime_end': exp_info["date_end"],
                "custom_confirm_event": exp_info["experiment_event_start"],
                "platform": filters['platform_filter'],
                "custom_confirm_include_values": "",
                "custom_confirm_exclude_values": "",
                # "pro_rights": "Free",
                # 'pro_rights': f"{filters['pro_rights_filter']}",
                'pro_rights': self.generate_sql_rights_filter('pro', filters['pro_rights_filter']),
                # # "edu_rights": "all",
                # 'edu_rights': f"{filters['edu_rights_filter']}",
                'edu_rights': self.generate_sql_rights_filter('edu', filters['edu_rights_filter']),
                # # "sing_rights": "all",
                # 'sing_rights': f"{filters['sing_rights_filter']}",
                'sing_rights': self.generate_sql_rights_filter('sing', filters['sing_rights_filter']),
                # # "practice_rights": "all",
                # 'practice_rights': f"{filters['practice_rights_filter']}",
                'practice_rights': self.generate_sql_rights_filter('practice', filters['practice_rights_filter']),
                # # "book_rights": "all",
                # 'book_rights': f"{filters['books_rights_filter']}",
                'book_rights': self.generate_sql_rights_filter('book', filters['books_rights_filter']),
                "country": "all",
                "source": filters['source_filter'],
                'custom_sql': 1 if filters['custom_sql_filter'] == '' else filters['custom_sql_filter']
            })
            query = self.get_query("get_exp_users", params)
            print(query)
            # return
            payload = self.get_payload(query)
            # print(payload)
            query_result = self._mb_client.post("dataset/json", payload)
            df = pd.json_normalize(query_result)
            print("df", df.shape)
            df.to_csv('res.csv')
            print(df)
            df.unified_id = df.unified_id.apply(self.convert_string_int2int)
            df.session_id = df.session_id.apply(self.convert_string_int2int)
            df.exp_start_dt = df.exp_start_dt.apply(self.convert_string_int2int)
            full_df = pd.concat([full_df, df], ignore_index=True)
            full_df = full_df.sort_values(by=['unified_id', 'exp_start_dt'])
            full_df = full_df.drop_duplicates(subset='unified_id', keep='first')
        progress_bar.empty()
        return full_df

    def get_subscriptions(self, exp_info: pd.DataFrame, progress_bar) -> pd.DataFrame:
        full_df = pd.DataFrame({})
        exp_start_dt = datetime.datetime.fromtimestamp(exp_info["date_start"], datetime.timezone.utc)
        exp_end_dt = datetime.datetime.today()
        days_cnt = (exp_end_dt.date() - exp_start_dt.date()).days + 1
        for day in range(days_cnt):
            progress_bar.progress(round(day / days_cnt * 100), text='getting subscriptions...')
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
        progress_bar.empty()
        return full_df