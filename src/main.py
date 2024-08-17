# from src.metabase import Mb_Client
# from dotenv import dotenv_values
import pandas as pd

from metrics_calc import MetricComposer

# metric_omposer: MetricComposer = MetricComposer()
# res = metric_omposer.compose_exp(4920)
# res.to_csv("res.csv")
# from sql_worker import SqlWorker
from confluence import ConfluenceWorker

# sql_worker: SqlWorker = SqlWorker()
# exp_4821_info: dict = sql_worker.get_experiment(4821)
# print(exp_4821_info)
# exps = sql_worker.get_experiments()
# print(exps.loc[exps["id"] == 5031])
# print(exps.loc[exps["id"] == 5031]["status"])
# exp_4821_members = sql_worker.get_users(exp_4821_info)
# exp_4821_members.to_csv('exp_4821_m.csv')
# exp_4821_subscriptions = sql_worker.get_subscriptions(exp_4821_info)
# exp_4821_subscriptions.to_csv('exp_4821_s.csv')

# print(exp_4821)
# print(exp_4821.configuration)
# exp_4821.to_csv('exp_4821.csv')
# print(exp_4821.columns)

# active_exps: pd.DataFrame = sql_worker.get_active_experiments()
# print(active_exps)


confluence_worker: ConfluenceWorker = ConfluenceWorker()
# page_494814392 = confluence_worker.get_page_info("https://alice.mu.se/pages/viewpage.action?pageId=494814392")
page_info = confluence_worker.get_page_info_by_title('CRO', 'api+test')
# print(page_info)
# confluence_worker.upload_image(
#     '/Users/egorsemin/Downloads/retention 1d, %_pvalues_diff_confidence_intervals.png',
#     'pvalues_diff_confidence_intervals.png',
#     page_info['page_id']
#     )
image_markup = confluence_worker.generate_image_markup('pvalues_diff_confidence_intervals.png')
content_to_insert = f"""
<table>
    <tbody class="">
    <tr>
        <th class="highlight-#eae6ff confluenceTd" data-highlight-colour="#eae6ff">
            Header 1
        </th>
        <th class="highlight-#eae6ff confluenceTd" data-highlight-colour="#eae6ff">
            Header 2
        </th>
    </tr>
    <tr>
        <th class="highlight-#eae6ff confluenceTd" data-highlight-colour="#eae6ff">
            Row 1, Cell 1
        </th>
        <td>{image_markup}</td>
    </tr>
    </tbody>
</table>
"""
new_content = confluence_worker.update_expand_element(page_info['current_content'], 'Iteration #2', 'Significance analysis', content_to_insert)
updated_content = {
        "version": {
            "number": page_info['page_version'] + 1
        },
        'title': page_info['page_title'],
        "type": "page",
        "body": {
            "storage": {
                "value": new_content,
                "representation": "storage"
            }
        }
    }
print(new_content)
print(page_info['page_url'])
confluence_worker.upload_data(page_info['page_url'], updated_content)
# print(page_494814392)

