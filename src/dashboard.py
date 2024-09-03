# TODO:
# different progress bars
# ios / android
# mobweb
# product/seo metrics
# intractive map with active connections
# st.map
# save last used config
# TOFIX:
# pvalue cell colors
# df limits by counting
import streamlit as st
from streamlit_modal import Modal
from code_editor import code_editor
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
from plot_builder import PlotBuilder


class StreamlitApp:
    def __init__(self):
        if 'initialized' not in st.session_state:
            st.session_state['initialized'] = True
            print('init 1')
            self._metric_composer: MetricComposer = MetricComposer()
            self._sql_worker: SqlWorker = SqlWorker()
            self._confluence_worker: ConfluenceWorker = ConfluenceWorker()
            self._stats: Stats = Stats()
            self._config: dict = dotenv_values('.config')
            # self._experiments = self._sql_worker.get_active_experiments()
            # print('init 2')
            self._experiments = self._sql_worker.get_experiments()
            print(self._experiments)
            self._yaml_files = [f for f in os.listdir(self._config['metrics_folder']) if f.endswith('.yaml') or f.endswith('.yml')]
            self._content_generated = 0
            self._current_experimnet_name = ''
            self._page_info = None
            self.setup_session_state()

    def ensure_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def pvalue_round(self, number: float, alpha: float=0.05, min_val: float=1e-4) -> float:
        if number <= min_val:
            return 0.0
        elif number <= alpha:
            return round(number, 3)
        else:
            return round(number, 2)
    
    def setup_session_state(self):
        if 'app_monetization_stats_html_content' not in st.session_state:
            st.session_state['app_monetization_stats_html_content'] = None
        if 'app_monetization_metrics_html_content' not in st.session_state:
            st.session_state['app_monetization_metrics_html_content'] = None
        if 'confluence_insert_segment_#1' not in st.session_state:
            st.session_state['confluence_insert_segment_#1'] = None
        if 'confluence_insert_segment_#2' not in st.session_state:
            st.session_state['confluence_insert_segment_#2'] = None
        if 'selected_metrics_file' not in st.session_state:
            st.session_state['selected_metrics_file'] = self._config['default_metric_config']
        if 'session_id' not in st.session_state:
            st.session_state['session_id'] = str(uuid.uuid4())
        if 'metrics_yaml' not in st.session_state:
            st.session_state['metrics_yaml'] = None
        if 'project_page' not in st.session_state:
            st.session_state['project_page'] = ''
        if 'project_filters' not in st.session_state:
            st.session_state['project_filters'] = {}
    
    def build_html_content(self, table, htm_template):
        # full_html_code = """
        # <p class="auto-cursor-target">
        # <strong>Monetization Stats</strong>
        # </p>
        # """
        htm_rows = ''
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
            # st.session_state[f'{htm_template}_html_content'] = full_html_code + file.read().format(rows=htm_rows)
            st.session_state[f'{htm_template}_html_content'] = file.read().format(rows=htm_rows)

    def generate_metric_color(self, value, diff, is_positive=True):
        if value >= 0.05:
            return 'class="highlight-#fffae6 confluenceTd" data-highlight-colour="#fffae6" bgcolor="#fffae6"'
        elif is_positive and diff > 0 or not is_positive and diff < 0:
            return 'class="highlight-#e3fcef confluenceTd" data-highlight-colour="#e3fcef" bgcolor="#e3fcef"'
        else:
            return 'class="highlight-#ffebe6 confluenceTd" data-highlight-colour="#ffebe6" bgcolor="#ffebe6"'

    def build_html_monetization_metrics_content(self, table):
        # full_html_code = """
        # <p class="auto-cursor-target">
        # <strong>Monetization Metrics</strong>
        # </p>
        # """
        htm_rows = ''
        for id in range(len(table.index)):
            if table.index[id] == 'pvalue':
                rows_dict: dict = {
                    'arpu_color':  self.generate_metric_color(table['arpu'].iloc[id], table['arpu'].iloc[id - 1]),
                    'aov_color': self.generate_metric_color(table['aov'].iloc[id], table['aov'].iloc[id - 1]),
                    'arppu_color': self.generate_metric_color(table['arppu'].iloc[id], table['arppu'].iloc[id - 1]),
                    'access cr, %_color': self.generate_metric_color(table['access cr, %'].iloc[id], table['access cr, %'].iloc[id - 1]),
                    'charge cr, %_color': self.generate_metric_color(table['charge cr, %'].iloc[id], table['charge cr, %'].iloc[id - 1]),
                    'trial -> charge, %_color': self.generate_metric_color(table['trial -> charge, %'].iloc[id], table['trial -> charge, %'].iloc[id - 1]),
                    'charge -> 14d cancel, %_color': self.generate_metric_color(table['charge -> 14d cancel, %'].iloc[id], table['charge -> 14d cancel, %'].iloc[id - 1], False),
                    'variation': table.index[id],
                    'arpu': f"{self.pvalue_round(table['arpu'].iloc[id])}",
                    'aov': f"{self.pvalue_round(table['aov'].iloc[id])}",
                    'arppu': f"{self.pvalue_round(table['arppu'].iloc[id])}",
                    'access cr, %': f"{self.pvalue_round(table['access cr, %'].iloc[id])}",
                    'charge cr, %': f"{self.pvalue_round(table['charge cr, %'].iloc[id])}",
                    'trial -> charge, %': f"{self.pvalue_round(table['trial -> charge, %'].iloc[id])}",
                    'charge -> 14d cancel, %': f"{self.pvalue_round(table['charge -> 14d cancel, %'].iloc[id])}"
                }
                # print(rows_dict)
            elif table.index[id] == 'cumulatives':
                rows_dict: dict = {
                    'arpu_color': '',
                    'aov_color': '',
                    'arppu_color': '',
                    'access cr, %_color': '',
                    'charge cr, %_color': '',
                    'trial -> charge, %_color': '',
                    'charge -> 14d cancel, %_color': '',
                    'variation': table.index[id],
                    'arpu': self._confluence_worker.generate_image_markup(f'arpu_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'aov': self._confluence_worker.generate_image_markup(f'aov_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'arppu': self._confluence_worker.generate_image_markup(f'arppu_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'access cr, %': self._confluence_worker.generate_image_markup(f'access cr, %_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'charge cr, %': self._confluence_worker.generate_image_markup(f'charge cr, %_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'trial -> charge, %': self._confluence_worker.generate_image_markup(f'trial -> charge, %_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png'),
                    'charge -> 14d cancel, %': self._confluence_worker.generate_image_markup(f'charge -> 14d cancel, %_pvalues_diff_confidence_intervals_{self._stats._calc_session}.png')
                }
            else:
                money_prefix = '$'
                money_suffix = ''
                if table.index[id] == 'diff, %':
                    money_prefix = ''
                    money_suffix = '%'
                rows_dict: dict = {
                    'arpu_color': '',
                    'aov_color': '',
                    'arppu_color': '',
                    'access cr, %_color': '',
                    'charge cr, %_color': '',
                    'trial -> charge, %_color': '',
                    'charge -> 14d cancel, %_color': '',
                    'variation': table.index[id],
                    'arpu': f"""{money_prefix}{Decimal(f"{table['arpu'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'aov': f"""{money_prefix}{Decimal(f"{table['aov'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'arppu': f"""{money_prefix}{Decimal(f"{table['arppu'].iloc[id]:.3g}"):f}{money_suffix}""",
                    'access cr, %': f"""{Decimal(f"{table['access cr, %'].iloc[id]:.3g}"):f}%""",
                    'charge cr, %': f"""{Decimal(f"{table['charge cr, %'].iloc[id]:.3g}"):f}%""",
                    'trial -> charge, %': f"""{Decimal(f"{table['trial -> charge, %'].iloc[id]:.3g}"):f}%""",
                    'charge -> 14d cancel, %': f"""{Decimal(f"{table['charge -> 14d cancel, %'].iloc[id]:.3g}"):f}%"""
                }
            with open(f"{self._config['htm_folder']}app_monetization_metrics_row.html", 'r') as file:
                html_content = file.read().format(
                    **rows_dict
                )
                htm_rows += html_content + '\n'
        with open(f"{self._config['htm_folder']}app_monetization_metrics_header.html", 'r') as file:
            # st.session_state['app_monetization_metrics_html_content'] = full_html_code + file.read().format(rows=htm_rows)
            st.session_state['app_monetization_metrics_html_content'] = file.read().format(rows=htm_rows)
        # print(st.session_state['app_monetization_metrics_html_content'])
        
    def run_calculations(self):
        if self._current_experimnet_name not in self._experiments['name'].values:
            return
        if st.button('Run Calculations'):
            placeholder = st.empty()
            self._content_generated = 0
            placeholder.write('Running calculations...')
            my_bar = st.progress(0, text="")
            res = self._metric_composer.compose_exp(self._experiment_config['id'], my_bar, st.session_state['project_filters'])
            result_path = f"{self._config['results_folder']}{st.session_state['session_id']}/res.csv"
            self.ensure_dir(result_path)
            res.to_csv(result_path, index=False)

            df_processor: DF_Processor = DF_Processor(result_path, os.path.join(self._config['metrics_folder'], st.session_state['selected_metrics_file']))
            column_groups = df_processor.column_groups
            # if not self._column_groups["date cohort"]:
            #     await update.message.reply_text("There is no valid date column. Please upload a new CSV.")
            # if not self._column_groups["variation"]:
            #     await update.message.reply_text("There is no valid variation column. Please upload a new CSV.")
            results_df, stat_results_df = self._stats.evaluate_metrics(df_processor)
            summary_results_df = self._stats.create_summary_table(results_df)
            summary_stat_results_df = self._stats.create_summary_table(stat_results_df, True)
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
            
            plot_dir = f"{self._config['results_folder']}plots/"
            # event_id = str(uuid.uuid4())
            self.ensure_dir(plot_dir)
            plot_builder = PlotBuilder(plot_dir)
            plot_builder.save_plots(results_df)

            # TODO: app, web, mobweb
            self.build_html_content(summary_stat_results_df, 'app_monetization_stats')
            self.build_html_monetization_metrics_content(summary_results_df)
            self._content_generated = 1
            placeholder.empty()
    
    def display_htm_tables(self):
        with st.expander("Confluence Content To Insert"):
            if 'app_monetization_stats_html_content' in st.session_state and st.session_state['app_monetization_stats_html_content'] is not None:
                st.html(st.session_state['app_monetization_stats_html_content'])
            if 'app_monetization_metrics_html_content' in st.session_state and st.session_state['app_monetization_metrics_html_content'] is not None:
                st.html(st.session_state['app_monetization_metrics_html_content'])
        if self._page_info is not None:
            segments_cols = st.columns(2)
            iterations_options = self._confluence_worker.get_iterations_list(self._page_info['current_content'])
            first_level = list(iterations_options.keys())
            if first_level != []:
                with segments_cols[0]:
                    first_level_segment = st.selectbox('Select Update Segment Level #1', first_level)
                    if first_level_segment != '':
                        st.session_state['confluence_insert_segment_#1'] = first_level_segment
                if len(iterations_options[first_level_segment].keys()) > 0:
                    # segments_cols = st.columns(3)
                    second_level = list(iterations_options[first_level_segment].keys())
                        # first_level_segment = st.selectbox('Select Update Segment Level #1', first_level)
                        # if first_level_segment != '':
                        #     st.session_state['confluence_insert_segment_#1'] = first_level_segment
                    with segments_cols[1]:
                        second_level_segment = st.selectbox('Select Update Segment Level #2', second_level)
                        if second_level_segment != '':
                            st.session_state['confluence_insert_segment_#2'] = second_level_segment
            # if first_level != []:
            #     with segments_cols[-1]:
            #         st.text_input('Insert segment', '')
            # else:
            #     with segments_cols[0]:
            #         st.text_input('Insert segment', '')
    
    def display_existing_experiments(self):
        scope_options = [''] + list(self._experiments['name'].str.extract(r'^\s*\[(?!\d{4}-[Xx\d]{2}-[Xx\d]{2})(.*?)(?:/.*?)?\]')[0].dropna().unique())
        domain_options = self._experiments['product'].unique()
        filtered_experiments = self._experiments.copy()
        col1, col2 = st.columns([3, 1])
        with col2:
            active_experiments = st.checkbox("Active", value=True)
            selected_domain = st.selectbox('Domain', domain_options)
            selected_scope = st.selectbox('Scope', scope_options)

        if active_experiments:
            filtered_experiments = filtered_experiments.loc[filtered_experiments['status'] == 1]
        if selected_domain:
            filtered_experiments = filtered_experiments.loc[filtered_experiments['product'] == selected_domain]
        if selected_scope:
            filtered_experiments= filtered_experiments.loc[
                filtered_experiments['name'].apply(
                    lambda name: any(
                        re.match(rf'\[.*{selected_scope}.*\]', name)
                        for domain in domain_options if domain
                    )
                )
            ]

        with col1:
            # selected_experiment_name = st.selectbox('Select Experiment', [''] + list(self._experiments['name']))
            selected_experiment_name = st.selectbox('Select Experiment', [''] + list(filtered_experiments['name']))
        # experiment_options = ['']
        # for id in range(len(self._experiments.index)):
        #     experiment_options.append(self._experiments['name'][id])
        # selected_experiment_name = st.selectbox('Select Experiment', experiment_options)
        if selected_experiment_name != self._current_experimnet_name and selected_experiment_name in self._experiments['name'].values:
            self._current_experimnet_name = selected_experiment_name
            st.session_state['app_monetization_stats_html_content'] = None
            st.session_state['app_monetization_metrics_html_content'] = None
            self._content_generated = 0
        elif selected_experiment_name != self._current_experimnet_name or selected_experiment_name == '':
            self._current_experimnet_name = ''
            st.session_state['app_monetization_stats_html_content'] = None
            st.session_state['app_monetization_metrics_html_content']  = None
            self._content_generated = 0
            return
        selected_experiment_id = self._experiments.loc[self._experiments['name'] == self._current_experimnet_name]['id'].values[0]
        self._experiment_config : dict = self._sql_worker.get_experiment(selected_experiment_id)

        st.sidebar.header('Experiment Configuration')
        st.sidebar.text_input(
            'ID', self._experiment_config['id'], key='id', disabled=True
        )
        date_col1, date_col2 = st.sidebar.columns([1, 1])
        # date_col1.text_input(
        #         'Date Start', 
        #         datetime.datetime.fromtimestamp(
        #             self._experiment_config['date_start'], 
        #             datetime.timezone.utc
        #         ).strftime('%Y-%m-%d'),
        #         # key='date_start',
        #         disabled=True
        #     )
        with date_col1:
            date_start_label = st.text_input(
                'Date Start', 
                datetime.datetime.fromtimestamp(
                    self._experiment_config['date_start'], 
                    datetime.timezone.utc
                ).strftime('%Y-%m-%d'),
                # key='date_start',
                disabled=True
            )
        with date_col2:
            date_end_label = st.text_input(
                'Date End', 
                0 if self._experiment_config['date_end'] == 0 else 
                    datetime.datetime.fromtimestamp(
                        self._experiment_config['date_end'], 
                        datetime.timezone.utc
                    ).strftime('%Y-%m-%d'), 
                # key='date_end',
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
        # if url != '':
        project_page = st.sidebar.text_input(
            'Project Page', f'{url}'
        )
        if st.session_state['project_page'] != project_page:
            st.session_state['project_page'] = project_page
        if 'pageId=' in project_page:
            self._page_info = self._confluence_worker.get_page_info(project_page)
        else:
            searched_space_key, searched_page_title = self._confluence_worker.parse_confluence_url(project_page)
            if searched_space_key is not None:
                self._page_info = self._confluence_worker.get_page_info_by_title(searched_space_key, searched_page_title)

        rights_configuration = list([
            'All', 'Free', 'Finite subscription', 'Lifetime', 'Any paid',
            'Any subscription', 'Trial', 'Expired subscription', 'Expired trial', 'Expired any'
        ])
        source_configuration = list(['All', 'UG_WEB', 'UGT_ANDROID', 'UGT_IOS', 'UG_ANDROID', 'UG_IOS'])
        platform_configuration = list([
            'All', 'Desktop', 'Phone', 'Tablet', 'Mobile'
        ])
        os_configuration = list(['All', 'iOS', 'Android'])
        country_configuration = list([
            'All', 'US', 'CA', 'GB', 'AU', 'Europe', 'Asia', 'Latam'
        ])

        default_split = None
        if len(self._experiment_config['clients_list']) > 1:
            default_split = 'Source'
        st.session_state['project_filters']['split'] = st.sidebar.multiselect(
            'Split By', ['Platform', 'OS', 'Source'], default_split
        )
        with st.sidebar.expander('Filters'):
            st.session_state['project_filters']['pro_rights_filter'] = st.selectbox(
                'Pro Rights', rights_configuration
            )
            st.session_state['project_filters']['edu_rights_filter'] = st.selectbox(
                'Edu Rights', rights_configuration
            )
            st.session_state['project_filters']['sing_rights_filter'] = st.selectbox(
                'Sing Rights', rights_configuration
            )
            st.session_state['project_filters']['practice_rights_filter'] = st.selectbox(
                'Practice Rights', rights_configuration
            )
            st.session_state['project_filters']['books_rights_filter'] = st.selectbox(
                'Books Rights', rights_configuration
            )
            st.session_state['project_filters']['source_filter'] = st.selectbox(
                'Source', source_configuration
            )
            st.session_state['project_filters']['platform_filter'] = st.selectbox(
                'Platform', platform_configuration
            )
            st.session_state['project_filters']['os_filter'] = st.selectbox(
                'OS', os_configuration
            )
            st.session_state['project_filters']['country_filter'] = st.selectbox(
                'Country', country_configuration
            )
            st.session_state['project_filters']['custom_sql_filter'] = st.text_input(
                'Custom SQL'
            )
    
    def display_metrics_config(self):
        with open(f"{self._config['metrics_folder']}{self._config['default_metric_config']}", 'r') as file:
            metrics_config = yaml.safe_load(file)
        if st.button('Edit Metrics Config'):
            modal = Modal('Metrics Config Editor', key='metrics_config_modal')
            with modal.container():
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button('Save'):
                        # TODO:
                        st.session_state['metrics_yaml'] = yaml.safe_dump(metrics_yaml)
                        st.success('Metrics configuration saved successfully')
                        modal.close()
                with col2:
                    if 'save_as_clicked' not in st.session_state:
                        st.session_state['save_as_clicked'] = False
                    if st.button('Save as ...'):
                        st.session_state['save_as_clicked'] = True

                    if st.session_state['save_as_clicked']:
                        file_name = st.text_input('Enter file name (including .yaml):', '')
                        if file_name:
                            with open(f"{self._config['metrics_folder']}{file_name}", 'w') as file:
                                file.write(metrics_yaml)
                            st.success(f'Metrics configuration saved successfully {file_name}')
                        modal.close()
                metrics_yaml = code_editor(yaml.safe_dump(metrics_config), lang='yaml')

    def upload_results(self):
        if self._content_generated:
            if st.button('Upload Results'):
                st.write('Uploading results...')
                # page_info = self._confluence_worker.get_page_info_by_title('CRO', 'api+test')
                # page_id = self._page_info['id']
                # page_version = self._page_info['version']["number"]
                # page_title = self._page_info['title']
                # current_content = self._page_info['body']['storage']['value']
                # page_url = f'{self._confluence_worker._base_url}/rest/api/content/{page_id}'
                page_id = self._page_info['page_id']
                page_version = self._page_info['page_version']
                page_title = self._page_info['page_title']
                current_content = self._page_info['current_content']
                page_url = self._page_info['page_url']
                plot_dir = f"{self._config['results_folder']}plots/"
                list_dir = os.listdir(plot_dir)
                number_files = len(list_dir)
                file_num = 1
                
                image_loading_bar = st.progress(0, text="")
                for plot_file in list_dir:
                    image_loading_bar.progress(round(file_num / number_files * 100), text='uploading images...')
                    self._confluence_worker.upload_image(
                        # '/Users/egorsemin/Downloads/retention 1d, %_pvalues_diff_confidence_intervals.png',
                        f'{plot_dir}{plot_file}',
                        f'{os.path.splitext(plot_file)[0]}_{self._stats._calc_session}.png',
                        page_id
                    )
                    file_num += 1
                # TODO: remove hardcode
                
                outer_title = st.session_state['confluence_insert_segment_#1']
                inner_title = st.session_state['confluence_insert_segment_#2']
                # print("confluence_insert_segment_#1 / confluence_insert_segment_#2")
                # print(outer_title, inner_title)
                # print("outer_title=", outer_title, "inner_title=", inner_title)
                content_to_insert = ''
                if 'app_monetization_stats_html_content' in st.session_state:
                    content_to_insert += st.session_state['app_monetization_stats_html_content']
                if 'app_monetization_metrics_html_content' in st.session_state:
                    content_to_insert += st.session_state['app_monetization_metrics_html_content']
                if content_to_insert != '':
                    # print(content_to_insert)
                    new_content = self._confluence_worker.update_expand_element(current_content, outer_title, inner_title, content_to_insert)
                    # print(new_content)
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
                    # text_file = open("updated_content.html", "w")
                    # text_file.write(new_content)
                    # text_file.close()
                    self._confluence_worker.upload_data(page_url, updated_content)
                    st.success('Results uploaded successfully')

    def render(self):
        st.title('Monetization results builder')

        self.display_existing_experiments()
        self.display_metrics_config()
        self.run_calculations()
        self.display_htm_tables()
        self.upload_results()


if __name__ == '__main__':
    if 'app_instance' not in st.session_state:
        st.session_state['app_instance'] = StreamlitApp()
    st.session_state['app_instance'].render()
