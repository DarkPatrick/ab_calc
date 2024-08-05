# from src.metabase import Mb_Client
# from dotenv import dotenv_values
import pandas as pd

from sql_worker import SqlWorker
from confluence import ConfluenceWorker

sql_worker: SqlWorker = SqlWorker()
exp_4821_info: dict = sql_worker.get_experiment(4821)
exp_4821_members = sql_worker.get_users(exp_4821_info)
exp_4821_members.to_csv('exp_4821_m.csv')
exp_4821_subscriptions = sql_worker.get_subscriptions(exp_4821_info)
exp_4821_subscriptions.to_csv('exp_4821_s.csv')

# print(exp_4821)
# print(exp_4821.configuration)
# exp_4821.to_csv('exp_4821.csv')
# print(exp_4821.columns)

# active_exps: pd.DataFrame = sql_worker.get_active_experiments()
# print(active_exps)


# confluence_worker: ConfluenceWorker = ConfluenceWorker()
# page_494814392 = confluence_worker.get_page_info("https://alice.mu.se/pages/viewpage.action?pageId=494814392")
# print(page_494814392)
