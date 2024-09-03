import pandas as pd
import datetime
import numpy as np
from ast import literal_eval

from sql_worker import SqlWorker


class MetricComposer():
    def __init__(self) -> None:
        self._sql_worker: SqlWorker = SqlWorker()

    def compose_exp(self, exp_id: int, progress_bar, filters) -> pd.DataFrame:
        exp_info: dict = self._sql_worker.get_experiment(exp_id)

        exp_end_dt = int(round(datetime.datetime.today().timestamp()))
        if exp_info['date_end'] > exp_info['date_start']:
            exp_end_dt = exp_info['date_end']

        exp_members = self._sql_worker.get_users(exp_info, progress_bar, filters)
        # exp_members = exp_members.loc[exp_members['exp_start_dt'].between(exp_info['date_start'], exp_end_dt)]
        exp_subscriptions = self._sql_worker.get_subscriptions(exp_info, progress_bar)
        # print("AAAAAAAAAAAAA")
        # print(exp_members.groupby('variation')['unified_id'].nunique().reset_index())

        merged_df = pd.merge(exp_members, exp_subscriptions, on='unified_id', how='left')
        merged_df['exp_end_dt'] = exp_end_dt
        merged_df['subscribed_dt'] = merged_df['subscribed_dt'].fillna("0").astype(int)
        merged_df['charge_dt'] = merged_df['charge_dt'].fillna("0").astype(int)
        merged_df['cancel_dt'] = merged_df['cancel_dt'].fillna("0").astype(int)
        merged_df['revenue'] = merged_df['revenue'].fillna("0").astype(float)
        merged_df['dt'] = pd.to_datetime(merged_df['exp_start_dt'], unit='s').dt.tz_localize('UTC').dt.date
        between_subscribed_exp = merged_df['subscribed_dt'].between(merged_df['exp_start_dt'], merged_df['exp_end_dt'])
        merged_df['trial'] = pd.to_numeric(merged_df['trial'], errors='coerce').fillna(0).astype(np.int64)
        merged_df['sub_prod_id'] = merged_df['subscription_id'].astype(str) + '_' + merged_df['product_id'].astype(str)
        merged_df['unified_revenue'] = merged_df.apply(lambda row: (row['unified_id'], row['revenue']), axis=1)
        trial_condition = merged_df['trial'] > 0

        # merged_df.to_csv("merged_df.csv")

        agg_dict = {
            'members': pd.NamedAgg(column='unified_id', aggfunc=pd.Series.nunique),
            'subscriber_cnt': pd.NamedAgg(column='unified_id', aggfunc=lambda x: x[between_subscribed_exp].nunique()),
            'access_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp].nunique()),
            'access_instant_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['trial'] == 0)].nunique()),
            'access_ex_trial_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & trial_condition & (merged_df['charge_dt'].between(merged_df['subscribed_dt'], merged_df['subscribed_dt'] + pd.Timedelta(hours=24).total_seconds()))].nunique()),
            'access_trial_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & trial_condition & ~(merged_df['charge_dt'].between(merged_df['subscribed_dt'], merged_df['subscribed_dt'] + pd.Timedelta(hours=24).total_seconds()))].nunique()),
            'trial_subscriber_cnt': pd.NamedAgg(column='unified_id', aggfunc=lambda x: x[between_subscribed_exp & trial_condition].nunique()),
            'charged_trial_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & trial_condition & (merged_df['charge_dt'].between(merged_df['subscribed_dt'] + pd.Timedelta(days=6).total_seconds(), merged_df['subscribed_dt'] + pd.Timedelta(days=9).total_seconds()))].nunique()),
            'trial_buyer_cnt': pd.NamedAgg(column='unified_id', aggfunc=lambda x: x[between_subscribed_exp & trial_condition & (merged_df['charge_dt'].between(merged_df['subscribed_dt'] + pd.Timedelta(days=6).total_seconds(), merged_df['subscribed_dt'] + pd.Timedelta(days=9).total_seconds()))].nunique()),
            'late_charged_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] > merged_df['subscribed_dt'] + pd.Timedelta(days=9).total_seconds())].nunique()),
            'buyer_cnt': pd.NamedAgg(column='unified_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt'])].nunique()),
            'charge_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt'])].nunique()),
            'revenue': pd.NamedAgg(column='revenue', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt'])].sum()),
            'trial_revenue': pd.NamedAgg(column='revenue', aggfunc=lambda x: x[between_subscribed_exp & trial_condition & (merged_df['charge_dt'].between(merged_df['subscribed_dt'] + pd.Timedelta(days=6).total_seconds(), merged_df['subscribed_dt'] + pd.Timedelta(days=9).total_seconds()))].sum()),
            'upgrade_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[merged_df['upgrade_dt'].between(merged_df['exp_start_dt'], merged_df['exp_end_dt'])].nunique()),
            'upgrade_revenue': pd.NamedAgg(column='upgrade_revenue', aggfunc=lambda x: x[merged_df['upgrade_dt'].between(merged_df['exp_start_dt'], merged_df['exp_end_dt'])].sum()),
            'prices': pd.NamedAgg(column='revenue', aggfunc=lambda x: list(x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt'])])),
            'prices_per_buyer': pd.NamedAgg(column='unified_revenue', aggfunc=lambda x: list(x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt'])])),
            'cancel_14d_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt']) & (merged_df['cancel_dt'].between(merged_df['charge_dt'], merged_df['charge_dt'] + pd.Timedelta(days=14).total_seconds()))].nunique()),
            'cancel_1m_cnt': pd.NamedAgg(column='sub_prod_id', aggfunc=lambda x: x[between_subscribed_exp & (merged_df['charge_dt'] >= merged_df['subscribed_dt']) & (merged_df['cancel_dt'].between(merged_df['charge_dt'], merged_df['charge_dt'] + pd.Timedelta(days=30).total_seconds()))].nunique())
        }

        print("split=", filters['split'])
        if filters['split'] == None or filters['split'] == []:
            bydate = merged_df.groupby(['dt', 'variation']).agg(**agg_dict).reset_index()
        elif 'Source' in filters['split']:
            bydate = merged_df.groupby(['dt', 'variation', 'source']).agg(**agg_dict).reset_index()
        if filters['split'] == None or filters['split'] == []:
            bydate = bydate.sort_values(['variation', 'dt'])
        elif 'Source' in filters['split']:
            bydate = bydate.sort_values(['variation', 'dt', 'source'])
        # TOFIX: group by variation, source
        bydate['members'] = bydate.groupby('variation')['members'].cumsum()
        bydate['subscriber_cnt'] = bydate.groupby('variation')['subscriber_cnt'].cumsum()
        bydate['access_cnt'] = bydate.groupby('variation')['access_cnt'].cumsum()
        bydate['access_instant_cnt'] = bydate.groupby('variation')['access_instant_cnt'].cumsum()
        bydate['access_trial_cnt'] = bydate.groupby('variation')['access_trial_cnt'].cumsum()
        bydate['trial_subscriber_cnt'] = bydate.groupby('variation')['trial_subscriber_cnt'].cumsum()
        bydate['charged_trial_cnt'] = bydate.groupby('variation')['charged_trial_cnt'].cumsum()
        bydate['trial_buyer_cnt'] = bydate.groupby('variation')['trial_buyer_cnt'].cumsum()
        bydate['late_charged_cnt'] = bydate.groupby('variation')['late_charged_cnt'].cumsum()
        bydate['buyer_cnt'] = bydate.groupby('variation')['buyer_cnt'].cumsum()
        bydate['charge_cnt'] = bydate.groupby('variation')['charge_cnt'].cumsum()
        bydate['cancel_14d_cnt'] = bydate.groupby('variation')['cancel_14d_cnt'].cumsum()
        bydate['cancel_1m_cnt'] = bydate.groupby('variation')['cancel_1m_cnt'].cumsum()
        bydate['revenue'] = bydate.groupby('variation')['revenue'].cumsum()
        bydate['trial_revenue'] = bydate.groupby('variation')['trial_revenue'].cumsum()

        bydate['accesses per subscriber'] = bydate['access_cnt'] / bydate['subscriber_cnt']
        bydate['member -> subscriber, %'] = bydate['subscriber_cnt'] / bydate['members'] * 100
        bydate['trial -> charge, %'] = bydate['charged_trial_cnt'] / bydate['access_trial_cnt'] * 100
        bydate['trial subscriber -> buyer, %'] = bydate['trial_buyer_cnt'] / bydate['trial_subscriber_cnt'] * 100
        bydate['subscriber -> buyer, %'] = bydate['buyer_cnt'] / bydate['subscriber_cnt'] * 100
        bydate['member -> buyer, %'] = bydate['buyer_cnt'] / bydate['members'] * 100
        bydate['subscription -> charge, %'] = bydate['charge_cnt'] / bydate['access_cnt'] * 100
        bydate['charge -> 14d cancel, %'] = bydate['cancel_14d_cnt'] / bydate['charge_cnt'] * 100
        bydate['charge -> 1m cancel, %'] = bydate['cancel_1m_cnt'] / bydate['charge_cnt'] * 100
        bydate['arppu'] = bydate['revenue'] / bydate['buyer_cnt']
        bydate['aov'] = bydate['revenue'] / bydate['charge_cnt']
        bydate['exp_arpu'] = bydate['revenue'] / bydate['members']
        bydate['exp_trial_arpu'] = bydate['trial_revenue'] / bydate['members']
        bydate['exp_instant_arpu'] = (bydate['revenue'] - bydate['trial_revenue']) / bydate['members']

        def flatten_concatenation(matrix):
            flat_list = []
            for row in matrix:
                flat_list += row
            return flat_list

        # bydate['prices_cum'] = bydate.groupby('variation')['prices'].apply(lambda x: [item for sublist in x for item in sublist]).reset_index(drop=True)
        # bydate['prices_cum'] = bydate.groupby('variation')['prices'].apply(lambda x: [x.explode().tolist()]).reset_index(drop=True)
        
        bydate['prices'] = [y['prices'].tolist()[:z+1] for x, y in bydate.groupby('variation')for z in range(len(y))]
        # bydate.to_csv("cumulative.csv")
        # print(bydate["prices"])
        # print(bydate["prices"][0])
        # bydate["prices"] = bydate["prices"].map(lambda x: flatten_concatenation(literal_eval(x)))
        bydate["prices"] = bydate["prices"].map(lambda x: flatten_concatenation(x))
        # bydate['prices_per_buyer'] = bydate.groupby('variation')['prices_per_buyer'].apply(lambda x: [item for sublist in x for item in sublist]).reset_index(drop=True)
        bydate['prices_per_buyer'] = [y['prices_per_buyer'].tolist()[:z+1] for x, y in bydate.groupby('variation')for z in range(len(y))]
        bydate["prices_per_buyer"] = bydate["prices_per_buyer"].map(lambda x: flatten_concatenation(x))
        # print(bydate['prices_per_buyer'])
        # print(bydate['prices_per_buyer_cum'])
        # bydate.to_csv("cumulative.csv")

        def process_row(row):
            prices_df = pd.DataFrame(row['prices_per_buyer'], columns=['key', 'value'])
            grouped_sums = prices_df.groupby('key')['value'].sum().values.tolist()
            return grouped_sums

        bydate['grouped_sums'] = bydate.apply(process_row, axis=1)

        bydate['denom'] = 1

        if filters['split'] == None or filters['split'] == []:
            cumulative = bydate[[
                'dt', 'variation', 'members', 'subscriber_cnt', 'access_cnt', 
                'accesses per subscriber', 'member -> subscriber, %', 'access_instant_cnt', 'access_trial_cnt',
                'trial_subscriber_cnt', 'charged_trial_cnt', 'trial_buyer_cnt', 'trial -> charge, %',
                'trial subscriber -> buyer, %', 'late_charged_cnt', 'buyer_cnt', 'subscriber -> buyer, %',
                'member -> buyer, %', 'charge_cnt', 'subscription -> charge, %', 'cancel_14d_cnt',
                'charge -> 14d cancel, %', 'cancel_1m_cnt', 'charge -> 1m cancel, %',
                'revenue', 'trial_revenue', 'arppu', 'aov', 'exp_arpu', 'exp_trial_arpu',
                'exp_instant_arpu', 'prices', 'prices_per_buyer', 'grouped_sums',
                'denom'
            ]]
        elif 'Source' in filters['split']:
            cumulative = bydate[[
                'dt', 'variation', 'source', 'members', 'subscriber_cnt', 'access_cnt', 
                'accesses per subscriber', 'member -> subscriber, %', 'access_instant_cnt', 'access_trial_cnt',
                'trial_subscriber_cnt', 'charged_trial_cnt', 'trial_buyer_cnt', 'trial -> charge, %',
                'trial subscriber -> buyer, %', 'late_charged_cnt', 'buyer_cnt', 'subscriber -> buyer, %',
                'member -> buyer, %', 'charge_cnt', 'subscription -> charge, %', 'cancel_14d_cnt',
                'charge -> 14d cancel, %', 'cancel_1m_cnt', 'charge -> 1m cancel, %',
                'revenue', 'trial_revenue', 'arppu', 'aov', 'exp_arpu', 'exp_trial_arpu',
                'exp_instant_arpu', 'prices', 'prices_per_buyer', 'grouped_sums',
                'denom'
            ]]
        
        cumulative.to_csv("cumulative.csv")

        def calculate_variance(row, values, denominator):
            mean = np.sum(values) / denominator
            return np.sum((np.array(values) - mean)**2) / (denominator - 1)

        # df.assign(aov_var = lambda x: x.apply(lambda row: calculate_variance(row, row['prices'], len(row['prices'])), axis=1),)
        result = cumulative.assign(
            members = lambda x: np.round(x['members'] / x['denom']),
            subscriber_cnt = lambda x: np.round(x['subscriber_cnt'] / x['denom']),
            access_cnt = lambda x: np.round(x['access_cnt'] / x['denom']),
            access_instant_cnt = lambda x: np.round(x['access_instant_cnt'] / x['denom']),
            access_trial_cnt = lambda x: np.round(x['access_trial_cnt'] / x['denom']),
            trials_part = lambda x: x['access_trial_cnt'] / x['access_cnt'] * 100,
            trial_subscriber_cnt = lambda x: np.round(x['trial_subscriber_cnt'] / x['denom']),
            charged_trial_cnt = lambda x: np.round(x['charged_trial_cnt'] / x['denom']),
            trial_buyer_cnt = lambda x: np.round(x['trial_buyer_cnt'] / x['denom']),
            late_charged_cnt = lambda x: np.round(x['late_charged_cnt'] / x['denom']),
            buyer_cnt = lambda x: np.round(x['buyer_cnt'] / x['denom']),
            charge_cnt = lambda x: np.round(x['charge_cnt'] / x['denom']),
            cancel_14d_cnt = lambda x: np.round(x['cancel_14d_cnt'] / x['denom']),
            cancel_1m_cnt = lambda x: np.round(x['cancel_1m_cnt'] / x['denom']),
            revenue = lambda x: np.round(x['revenue'] / x['denom']),
            trial_revenue = lambda x: np.round(x['trial_revenue'] / x['denom']),
            aov_var = lambda x: x.apply(lambda row: calculate_variance(row, row['prices'], len(row['prices'])), axis=1),
            arppu_var = lambda x: x.apply(lambda row: calculate_variance(row, row['grouped_sums'], len(row['grouped_sums'])), axis=1),
            exp_arpu_var = lambda x: x.apply(lambda row: calculate_variance(row, row['prices'], len(row['prices']) + row['members'] - row['charge_cnt']), axis=1),
        )

        # Select and order columns
        if filters['split'] == None or filters['split'] == []:
            columns = [
                'dt', 'variation', 'members', 'subscriber_cnt', 'access_cnt', 
                'accesses per subscriber', 'member -> subscriber, %', 
                'access_instant_cnt', 'access_trial_cnt', 'trials_part', 
                'trial_subscriber_cnt', 'charged_trial_cnt', 'trial_buyer_cnt', 
                'trial -> charge, %', 'trial subscriber -> buyer, %', 
                'late_charged_cnt', 'buyer_cnt', 'subscriber -> buyer, %', 
                'member -> buyer, %', 'charge_cnt', 'subscription -> charge, %', 
                'cancel_14d_cnt', 'charge -> 14d cancel, %', 'cancel_1m_cnt', 
                'charge -> 1m cancel, %', 'revenue', 'trial_revenue', 'arppu', 
                'aov', 'exp_arpu', 'exp_trial_arpu', 'exp_instant_arpu',
                'aov_var', 'arppu_var', 'exp_arpu_var'
            ]
        elif 'Source' in filters['split']:
            columns = [
                'dt', 'variation', 'source', 'members', 'subscriber_cnt', 'access_cnt', 
                'accesses per subscriber', 'member -> subscriber, %', 
                'access_instant_cnt', 'access_trial_cnt', 'trials_part', 
                'trial_subscriber_cnt', 'charged_trial_cnt', 'trial_buyer_cnt', 
                'trial -> charge, %', 'trial subscriber -> buyer, %', 
                'late_charged_cnt', 'buyer_cnt', 'subscriber -> buyer, %', 
                'member -> buyer, %', 'charge_cnt', 'subscription -> charge, %', 
                'cancel_14d_cnt', 'charge -> 14d cancel, %', 'cancel_1m_cnt', 
                'charge -> 1m cancel, %', 'revenue', 'trial_revenue', 'arppu', 
                'aov', 'exp_arpu', 'exp_trial_arpu', 'exp_instant_arpu',
                'aov_var', 'arppu_var', 'exp_arpu_var'
            ]

        if filters['split'] == None or filters['split'] == []:
            result = result[columns].sort_values(['dt', 'variation'])
        elif 'Source' in filters['split']:
            result = result[columns].sort_values(['dt', 'variation', 'source'])

        returned_user_df = self._sql_worker.get_returned_users(exp_info, progress_bar, filters)
        returned_user_df['cohort_date'] = pd.to_datetime(returned_user_df['cohort_date'], unit='s').dt.tz_localize('UTC').dt.date
        returned_user_df_cum = returned_user_df.sort_values(['cohort_date', 'variation'])
        returned_user_df_cum['members'] = returned_user_df_cum.groupby('variation')['user_cnt'].cumsum()
        returned_user_df_cum['session_cnt'] = returned_user_df_cum.groupby('variation')['session_cnt'].cumsum()
        returned_user_df_cum['retention_1d_cnt'] = returned_user_df_cum.groupby('variation')['retention_1d_cnt'].cumsum()
        returned_user_df_cum['retention_7d_cnt'] = returned_user_df_cum.groupby('variation')['retention_7d_cnt'].cumsum()
        returned_user_df_cum['long_tab_view_cnt'] = returned_user_df_cum.groupby('variation')['long_tab_view_cnt'].cumsum()
        columns = [
            'cohort_date',
            'variation',
            'members',
            'session_cnt',
            'retention_1d_cnt',
            'retention_7d_cnt',
            'long_tab_view_cnt'
        ]
        result_2 = returned_user_df_cum[columns].sort_values(['cohort_date', 'variation'])
        result_2.to_csv('returned_user_df_cum.csv')

        return result
