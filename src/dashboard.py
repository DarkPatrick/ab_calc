import streamlit as st
from streamlit_modal import Modal
import yaml
import os
import uuid
from decimal import Decimal
from dotenv import dotenv_values
import pandas as pd
import datetime
import re


from sql_worker import SqlWorker
from metrics_calc import MetricComposer
from df_processing import DF_Processor
from confluence import ConfluenceWorker
from stats import Stats


class StreamlitApp:
    def __init__(self):
        if 'initialized' not in st.session_state:
            st.session_state['initialized'] = True
            self._metric_composer: MetricComposer = MetricComposer()
            self._sql_worker: SqlWorker = SqlWorker()
            self._confluence_worker: ConfluenceWorker = ConfluenceWorker()
            self._config: dict = dotenv_values('.config')
            self._experiments = self._sql_worker.get_active_experiments()
            self._yaml_files = [f for f in os.listdir(self._config['metrics_folder']) if f.endswith('.yaml') or f.endswith('.yml')]
            print("init here")
            self._content_generated = 0
            self._current_experimnet_name = ''
            self.setup_session_state()

    def ensure_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)
    
    def setup_session_state(self):
        if 'app_monetization_stats_html_content' not in st.session_state:
            st.session_state['app_monetization_stats_html_content'] = None
        if 'selected_metrics_file' not in st.session_state:
            st.session_state['selected_metrics_file'] = self._config['default_metric_config']
        if 'session_id' not in st.session_state:
            st.session_state['session_id'] = str(uuid.uuid4())
    
    def build_html_content(self, table, htm_template):
        htm_rows = ""
        for id in range(len(table.index)):
            if (table.index[id] == 'control' or 'variation' in table.index[id]):
                rows_dict: dict = {
                    'variation': table.index[id],
                    'members': int(table['members'].iloc[id]),
                    'subscribers': int(table['subscribers'].iloc[id]),
                    'accesses': int(table['accesses'].iloc[id]),
                    'instants': int(table['instants'].iloc[id]),
                    'trials': int(table['trials'].iloc[id]),
                    'charged_trials': int(table['charged trials'].iloc[id]),
                    'buyers': int(table['buyers'].iloc[id]),
                    'charges': int(table['charges'].iloc[id]),
                    'revenue': f"${int(table['revenue'].iloc[id])}"
                }
            else:
                rows_dict: dict = {
                    'variation': table.index[id],
                    'members': f"""{Decimal(f"{table['members'].iloc[id]:.2g}"):f}%""",
                    'subscribers': f"""{Decimal(f"{table['subscribers'].iloc[id]:.2g}"):f}%""",
                    'accesses': f"""{Decimal(f"{table['accesses'].iloc[id]:.2g}"):f}%""",
                    'instants': f"""{Decimal(f"{table['instants'].iloc[id]:.2g}"):f}%""",
                    'trials': f"""{Decimal(f"{table['trials'].iloc[id]:.2g}"):f}%""",
                    'charged_trials': f"""{Decimal(f"{table['charged trials'].iloc[id]:.2g}"):f}%""",
                    'buyers': f"""{Decimal(f"{table['buyers'].iloc[id]:.2g}"):f}%""",
                    'charges': f"""{Decimal(f"{table['charges'].iloc[id]:.2g}"):f}%""",
                    'revenue': f"""{Decimal(f"{table['revenue'].iloc[id]:.2g}"):f}%"""
                }
            with open(f"{self._config['htm_folder']}{htm_template}_row.html", 'r') as file:
                html_content = file.read().format(
                    **rows_dict
                )
                htm_rows += html_content + '\n'
        with open(f"{self._config['htm_folder']}{htm_template}_header.html", 'r') as file:
            st.session_state[f'{htm_template}_html_content'] = file.read().format(rows=htm_rows)
        
    def run_calculations(self):
        if st.button('Run Calculations'):
            placeholder = st.empty()
            print("run calculations")
            self._content_generated = 0
            placeholder.write('Running calculations...')
            res = self._metric_composer.compose_exp(self._experiment_config['id'])
            result_path = f"{self._config['results_folder']}{st.session_state['session_id']}/res.csv"
            self.ensure_dir(result_path)
            res.to_csv(result_path, index=False)

            df_processor: DF_Processor = DF_Processor(result_path, os.path.join(self._config['metrics_folder'], st.session_state['selected_metrics_file']))
            column_groups = df_processor.column_groups
            # if not self._column_groups["date cohort"]:
            #     await update.message.reply_text("There is no valid date column. Please upload a new CSV.")
            # if not self._column_groups["variation"]:
            #     await update.message.reply_text("There is no valid variation column. Please upload a new CSV.")
            stats: Stats = Stats()
            results_df, stat_results_df = stats.evaluate_metrics(df_processor)
            summary_results_df = stats.create_summary_table(results_df)
            summary_stat_results_df = stats.create_summary_table(stat_results_df, True)
            loaded_csvs_path = f"{self._config['results_folder']}loaded_csvs/{st.session_state['session_id']}/"
            self.ensure_dir(loaded_csvs_path)
            results_file_path = f'{loaded_csvs_path}results.csv'
            stat_results_file_path = f'{loaded_csvs_path}stat_results.csv'
            summary_results_file_path = f'{loaded_csvs_path}summary_results.csv'
            summary_stat_results_file_path = f'{loaded_csvs_path}summary_stat_results.csv'
            results_df.to_csv(results_file_path)
            stat_results_df.to_csv(stat_results_file_path)
            summary_results_df.to_csv(summary_results_file_path)
            summary_stat_results_df.to_csv(summary_stat_results_file_path)
            # TODO: app, web, mobweb
            self.build_html_content(summary_stat_results_df, 'app_monetization_stats')
            self._content_generated = 1
            placeholder.empty()
    
    def display_htm_tables(self):
        with st.expander("Confluence Content To Insert"):
            if 'app_monetization_stats_html_content' in st.session_state and st.session_state['app_monetization_stats_html_content'] is not None:
                st.html(st.session_state['app_monetization_stats_html_content'])
    
    def display_existing_experiments(self):
        experiment_options = ['']
        for id in range(len(self._experiments.index)):
            experiment_options.append(self._experiments['name'][id])
        # experiment_options = {self._experiments['name'][id]: self._experiments['id'][id] for id in range(len(self._experiments.index))}
        selected_experiment_name = st.selectbox('Select Experiment', experiment_options)
        if selected_experiment_name != self._current_experimnet_name and selected_experiment_name in self._experiments['name'].values:
            print("changed experiment")
            self._current_experimnet_name = selected_experiment_name
            st.session_state['app_monetization_stats_html_content'] = None
            self._content_generated = 0
            selected_experiment_id = self._experiments.loc[self._experiments['name'] == selected_experiment_name]['id'].values[0]
            self._experiment_config : dict = self._sql_worker.get_experiment(selected_experiment_id)

            st.sidebar.header('Experiment Configuration')
            st.sidebar.text_input(
                'ID', self._experiment_config['id'], key='id', disabled=True
            )
            st.sidebar.text_input(
                'Date Start', 
                datetime.datetime.fromtimestamp(
                    self._experiment_config['date_start'], 
                    datetime.timezone.utc
                ).strftime('%Y-%m-%d'),
                key='date_start',
                disabled=True
            )
            st.sidebar.text_input(
                'Date End', self._experiment_config['date_end'], key='date_end',
                disabled=True
            )
            st.sidebar.text_input(
                'Exposure Event', self._experiment_config['experiment_event_start'], 
                key='exposure_event'
            )
            pattern = r'https://alice\.mu\.se[^\s#?"]*(?:\?[^\s#"]*)?'
            urls = re.findall(pattern, self._experiment_config['configuration'])
            if not urls:
                url = ''
            else:
                url = urls[0]
            st.sidebar.text_input(
                # 'Project Page', self._experiment_config['configuration'], 
                'Project Page', url, 
                key='project_page'
            )

            rights_configuration = list([
                'All', 'Free', 'Subscription', 'Lifetime', 'Any paid',
                'Trial', 'Expired subscription', 'Expired trial', 'Any trial'
            ])
            platform_configuration = list([
                'All', 'Web', 'Mobile', 'Tablet', 'MobWeb'
            ])
            os_configuration = list(['All', 'iOS', 'Android'])
            country_configuration = list([
                'All', 'US', 'CA', 'GB', 'Europe', 'Asia', 'Latam'
            ])

            with st.sidebar.expander('Filters'):
                pro_rights_filter = st.selectbox(
                    'Pro Rights', rights_configuration, key='pro_rights_filter'
                )
                edu_rights_filter = st.selectbox(
                    'Edu Rights', rights_configuration, key='edu_rights_filter'
                )
                sing_rights_filter = st.selectbox(
                    'Sing Rights', rights_configuration, 
                    key='sing_rights_filter'
                )
                practice_rights_filter = st.selectbox(
                    'Practice Rights', rights_configuration, 
                    key='practice_rights_filter'
                )
                books_rights_filter = st.selectbox(
                    'Books Rights', rights_configuration, 
                    key='books_rights_filter'
                )
                platform_filter = st.selectbox(
                    'Platform', platform_configuration, key='platform_filter'
                )
                os_filter = st.selectbox(
                    'OS', os_configuration, key='os_filter'
                )
                country_filter = st.selectbox(
                    'OS', country_configuration, key='country_filter'
                )
    
    def display_metrics_config(self):
        with open(f"{self._config['metrics_folder']}{self._config['default_metric_config']}", 'r') as file:
            metrics_config = yaml.safe_load(file)
        if st.button('Edit Metrics Config'):
            modal = Modal('Metrics Config Editor', key='metrics_config_modal')
            with modal.container():
                metrics_yaml = st.text_area('Metrics Config', yaml.safe_dump(metrics_config), height=100)
                if st.button('Save'):
                    # TODO:
                    st.success('Metrics configuration saved successfully')
                    modal.close()
                if st.button('Cancel'):
                    modal.close()

    def uplad_results(self):
        print(self._content_generated)
        if self._content_generated:
            if st.button('Upload Results'):
                print('Uploading results...')
                st.write('Uploading results...')
                page_info = self._confluence_worker.get_page_info_by_title('CRO', 'api+test')
                page_id = page_info['id']
                page_version = page_info['version']["number"]
                page_title = page_info['title']
                current_content = page_info['body']['storage']['value']
                page_url = f'{self._confluence_worker._base_url}/rest/api/content/{page_id}'
                # TODO: remove hardcode
                outer_title = 'Iteration #2'
                inner_title = 'Significance analysis'
                if 'app_monetization_stats_html_content' in st.session_state:
                    app_monetization_stats_html_content = st.session_state['app_monetization_stats_html_content']
                    new_content = self._confluence_worker.update_expand_element(current_content, outer_title, inner_title, app_monetization_stats_html_content)
                    updated_content = {
                        'version': {
                            'number': page_version + 1
                        },
                        'title': page_title,
                        'type': 'page',
                        'body': {
                            'storage': {
                                'value': new_content,
                                'representation': 'storage'
                            }
                        }
                    }
                    self._confluence_worker.upload_data(page_url, updated_content)
                    st.success('Results uploaded successfully')

    def render(self):
        st.title('Monetization results builder')

        self.display_existing_experiments()
        self.display_metrics_config()
        self.run_calculations()
        self.display_htm_tables()
        self.uplad_results()


if __name__ == '__main__':
    if 'app_instance' not in st.session_state:
        st.session_state['app_instance'] = StreamlitApp()
    st.session_state['app_instance'].render()
