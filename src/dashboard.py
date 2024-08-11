import streamlit as st
from streamlit_modal import Modal
import yaml
import os
import uuid
from decimal import Decimal


from sql_worker import SqlWorker
from metrics_calc import MetricComposer
from df_processing import DF_Processor
from confluence import ConfluenceWorker
from stats import Stats


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)


if 'session_id' not in st.session_state:
    st.session_state['session_id'] = str(uuid.uuid4())

session_id = st.session_state['session_id']

metric_omposer: MetricComposer = MetricComposer()
sql_worker: SqlWorker = SqlWorker()
confluence_worker: ConfluenceWorker = ConfluenceWorker()

st.title("Monetization results builder")


experiments = sql_worker.get_active_experiments()
experiment_options = {experiments['name'][id]: experiments['id'][id] for id in range(len(experiments.index))}

selected_experiment_name = st.selectbox("Select Experiment", list(experiment_options.keys()))
selected_experiment_id = experiment_options[selected_experiment_name]

if selected_experiment_id:
    experiment_config : dict = sql_worker.get_experiment(selected_experiment_id)

    st.sidebar.header("Experiment Configuration")
    st.sidebar.text_input("ID", experiment_config['id'], key="id", disabled=True)
    st.sidebar.text_input("Date Start", experiment_config['date_start'], key="date_start")
    st.sidebar.text_input("Date End", experiment_config['date_end'], key="date_end")
    st.sidebar.text_input("Exposure Event", experiment_config['experiment_event_start'], key="exposure_event")
    st.sidebar.text_input("Project Page", experiment_config['configuration'], key="project_page")
    
    rights_configuration = list(['All', 'Free', 'Subscription', 'Lifetime', 'Any paid', 'Trial', 'Expired subscription', 'Expired trial', 'Any trial'])
    platform_configuration = list(['All', 'Web', 'Mobile', 'Tablet', 'MobWeb'])
    os_configuration = list(['All', 'iOS', 'Android'])
    country_configuration = list(['All', 'US', 'CA', 'GB', 'Europe', 'Asia', 'Latam'])
    
    
    with st.sidebar.expander("Filters"):
        pro_rights_filter = st.selectbox("Pro Rights", rights_configuration, key="pro_rights_filter")
        edu_rights_filter = st.selectbox("Edu Rights", rights_configuration, key="edu_rights_filter")
        sing_rights_filter = st.selectbox("Sing Rights", rights_configuration, key="sing_rights_filter")
        practice_rights_filter = st.selectbox("Practice Rights", rights_configuration, key="practice_rights_filter")
        books_rights_filter = st.selectbox("Books Rights", rights_configuration, key="books_rights_filter")
        platform_filter = st.selectbox("Platform", platform_configuration, key="platform_filter")
        os_filter = st.selectbox("OS", os_configuration, key="os_filter")
        country_filter = st.selectbox("OS", country_configuration, key="country_filter")

metrics_folder = 'metrics/'
yaml_files = [f for f in os.listdir(metrics_folder) if f.endswith('.yaml') or f.endswith('.yml')]

with st.expander("Metrics Config"):
    selected_metrics_file = st.selectbox("Select Metrics File", yaml_files)
    metrics_file_path = os.path.join(metrics_folder, selected_metrics_file)
    with open(metrics_file_path, 'r') as file:
        metrics_config = yaml.safe_load(file)
    metrics_file = st.file_uploader("Select Metrics File", type=["yaml", "yml"])
    if st.button("Edit Metrics Config"):
        modal = Modal("Metrics Config Editor", key="metrics_config_modal")
        with modal.container():
            metrics_yaml = st.text_area("Metrics Config", yaml.safe_dump(metrics_config), height=100)
            
            if st.button("Save"):
                # save_metrics_file(yaml.safe_load(metrics_yaml), metrics_file_path)
                st.success("Metrics configuration saved successfully")
                modal.close()
                
            if st.button("Cancel"):
                modal.close()

    if metrics_file:
        if st.button("Save Metrics Config"):
            # save_metrics_file(yaml.safe_load(metrics_yaml), metrics_file.name)
            st.success("Metrics configuration saved successfully")

if st.button("Run Calculations"):
    st.write("Running calculations...")
    metric_omposer: MetricComposer = MetricComposer()
    res = metric_omposer.compose_exp(experiment_config['id'])
    res.to_csv("results/res.csv", index=False)
    
    df_processor: DF_Processor = DF_Processor("results/res.csv", os.path.join(metrics_folder, selected_metrics_file))
    column_groups = df_processor.column_groups
    # print(column_groups)
    #     if not self._column_groups["date cohort"]:
    #         await update.message.reply_text("There is no valid date column. Please upload a new CSV.")
    #         os.remove(self._csv_path)
    #         return self.UPLOAD_CSV
    #     if not self._column_groups["variation"]:
    #         await update.message.reply_text("There is no valid variation column. Please upload a new CSV.")
    #         os.remove(self._csv_path)
    #         return self.UPLOAD_CSV
    stats: Stats = Stats()
    results_df, stat_results_df = stats.evaluate_metrics(df_processor)
    summary_results_df = stats.create_summary_table(results_df)
    summary_stat_results_df = stats.create_summary_table(stat_results_df, True)

    loaded_csvs = f'results/loaded_csvs/{st.session_state["session_id"]}/'
    ensure_dir(loaded_csvs)
    results_file_path = f'{loaded_csvs}results.csv'
    stat_results_file_path = f'{loaded_csvs}stat_results.csv'
    summary_results_file_path = f'{loaded_csvs}summary_results.csv'
    summary_stat_results_file_path = f'{loaded_csvs}summary_stat_results.csv'
    results_df.to_csv(results_file_path)
    stat_results_df.to_csv(stat_results_file_path)
    summary_results_df.to_csv(summary_results_file_path)
    summary_stat_results_df.to_csv(summary_stat_results_file_path)
    
    app_monetization_stats_rows = ""
    for id in range(len(summary_stat_results_df.index)):
        if (summary_stat_results_df.index[id] == 'control' or 'variation' in summary_stat_results_df.index[id]):
            rows_dict: dict = {
                "variation": summary_stat_results_df.index[id],
                "members": int(summary_stat_results_df["members"].iloc[id]) if (summary_stat_results_df.index[id] == 'control' or 'variation' in summary_stat_results_df.index[id]) else f"""{Decimal(f'{summary_stat_results_df["members"].iloc[id]:.2g}'):f}""",
                "subscribers": int(summary_stat_results_df["subscribers"].iloc[id]),
                "accesses": int(summary_stat_results_df["accesses"].iloc[id]),
                "instants": int(summary_stat_results_df["instants"].iloc[id]),
                "trials": int(summary_stat_results_df["trials"].iloc[id]),
                "charged_trials": int(summary_stat_results_df["charged trials"].iloc[id]),
                "buyers": int(summary_stat_results_df["buyers"].iloc[id]),
                "charges": int(summary_stat_results_df["charges"].iloc[id]),
                "revenue": f'${int(summary_stat_results_df["revenue"].iloc[id])}'
            }
        else:
            rows_dict: dict = {
                "variation": summary_stat_results_df.index[id],
                "members": f"""{Decimal(f'{summary_stat_results_df["members"].iloc[id]:.2g}'):f}%""",
                "subscribers": f"""{Decimal(f'{summary_stat_results_df["subscribers"].iloc[id]:.2g}'):f}%""",
                "accesses": f"""{Decimal(f'{summary_stat_results_df["accesses"].iloc[id]:.2g}'):f}%""",
                "instants": f"""{Decimal(f'{summary_stat_results_df["instants"].iloc[id]:.2g}'):f}%""",
                "trials": f"""{Decimal(f'{summary_stat_results_df["trials"].iloc[id]:.2g}'):f}%""",
                "charged_trials": f"""{Decimal(f'{summary_stat_results_df["charged trials"].iloc[id]:.2g}'):f}%""",
                "buyers": f"""{Decimal(f'{summary_stat_results_df["buyers"].iloc[id]:.2g}'):f}%""",
                "charges": f"""{Decimal(f'{summary_stat_results_df["charges"].iloc[id]:.2g}'):f}%""",
                "revenue": f"""{Decimal(f'{summary_stat_results_df["revenue"].iloc[id]:.2g}'):f}%"""
            }
        with open('html/app_monetization_stats_row.html', 'r') as file:
            html_content = file.read().format(
                **rows_dict
            )
            app_monetization_stats_rows += html_content + "\n"

    with open('html/app_monetization_stats_header.html', 'r') as file:
        st.session_state['app_monetization_stats_html_content'] = file.read().format(rows=app_monetization_stats_rows)

    with st.expander("Confluence Content To Insert"):
        if 'app_monetization_stats_html_content' in st.session_state:
            st.html(st.session_state['app_monetization_stats_html_content'])
    
if st.button("Upload Results"):
    print("Uploading results...")
    st.write("Uploading results...")
    page_info = confluence_worker.get_page_info_by_title('CRO', 'api+test')
    page_id = page_info['id']
    page_version = page_info['version']["number"]
    page_title = page_info['title']
    current_content = page_info['body']['storage']['value']
    page_url = f'{confluence_worker._base_url}/rest/api/content/{page_id}'
    outer_title = "Iteration #2"
    inner_title = "Significance analysis"
    if 'app_monetization_stats_html_content' in st.session_state:
        app_monetization_stats_html_content = st.session_state['app_monetization_stats_html_content']
        new_content = confluence_worker.update_expand_element(current_content, outer_title, inner_title, app_monetization_stats_html_content)
        updated_content = {
            "version": {
                "number": page_version + 1
            },
            "title": page_title,
            "type": "page",
            "body": {
                "storage": {
                    "value": new_content,
                    "representation": "storage"
                }
            }
        }
        confluence_worker.upload_data(page_url, updated_content)
        st.success("Results uploaded successfully")
