import pandas as pd
import re
import yaml


class DF_Processor:
    def __init__(self, csv_path, yaml_path) -> None:
        with open(yaml_path, 'r') as file:
            self._metric_config = yaml.safe_load(file)
        self._df = pd.read_csv(csv_path)
        self._column_groups = None
    
    def __call__(self) -> None:
        return self._df

    def is_date_series(self, series):
        date_patterns = [
            r'^\d{2}[-/\.]\d{2}[-/\.]\d{2}$',   # any by two digits
            r'^\d{4}[-/\.]\d{2}[-/\.]\d{2}$',   # YYYY-MM-DD, YYYY/MM/DD, YYYY.MM.DD
            r'^\d{2}[-/\.]\d{2}[-/\.]\d{4}$',   # DD-MM-YYYY, DD/MM/YYYY, DD.MM.YYYY
            r'^\d{2}[-/\.]\d{2}[-/\.]\d{2}$',   # MM-DD-YY, MM/DD/YY, MM.DD.YY
            r'^\d{2}[-/\.]\d{2}[-/\.]\d{4}$',   # MM-DD-YYYY, MM/DD/YYYY, MM.DD.YYYY
            r'^\d{4}$',                         # YYYY
            r'^\d{2} \w{3} \d{4}$',             # DD MMM YYYY
            r'^\d{2}-\w{3}-\d{4}$',             # DD-MMM-YYYY
            r'^\w{3} \d{2}, \d{4}$',            # MMM DD, YYYY
            r'^\w{3} \d{2} \d{4}$',             # MMM DD YYYY
            r'^\d{2} \w{3} \d{2}$',             # DD MMM YY
            r'^\w{3}-\d{2}$',                   # MMM-YY
            r'^\d{2} \w{3}$',                   # DD MMM
            r'^\d{2} \w{3} \d{4} \d{2}:\d{2}$', # DD MMM YYYY HH:MM
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$', # YYYY-MM-DD HH:MM
            r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}$', # YYYY/MM/DD HH:MM
            r'^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'  # DD-MM-YYYY HH:MM
        ]
        
        if series.dtype == 'object':
            for value in series.dropna().unique():
                if not any(re.match(pattern, str(value)) for pattern in date_patterns):
                    # value doesn't match
                    return False
            # find good column
            return True
        # wrong type
        return False

    def categorize_columns(self):
        # print(self._df)
        # print(self._df.dt)
        # print(self._df.dt[0])
        date_column = None
        for column in self._df.columns:
            try:
                if self.is_date_series(self._df[column]):
                    date_column = column
                break
            except:
                continue

        groups = {
            "date cohort": [],
            "variation": []
        }

        if date_column:
            groups["date cohort"].append(date_column)

        for column in self._df.columns:
            if self._df[column].dtype == int and set(self._df[column].unique()).issuperset({1, 2}):
                groups["variation"].append(column)
                break
        return groups

    def process(self):
        self._column_groups = self.categorize_columns()

        return self._column_groups

    @property
    def column_groups(self):
        if self._column_groups is None:
            self.process()

        return self._column_groups
    
    @property
    def metric_config(self):
        return self._metric_config
