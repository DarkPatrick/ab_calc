import numpy as np
import scipy.stats as stats
import scipy.special as special
from statsmodels.stats.power import TTestIndPower
import pandas as pd
import math
import uuid

from df_processing import DF_Processor

class Stats:
    def __init__(self) -> None:
        self._effect_size = None
        self._lift = None
        self._power = 0.8
        self._alpha = 0.05
        self._mde = None
        self._calc_session = None

    def calc_stats(self, mean_0, mean_1, var_0, var_1, len_0, len_1, alpha=None, required_power=None, pvalue=None, calc_mean=False):
        if math.isnan(mean_0) or math.isnan(mean_1) or math.isnan(len_0) or math.isnan(len_1):
            return {"pvalue": 1, "power": 0, 
                "cohen_d": 0, "sample_size": 0, 
                "enough": False,
                "ci": [np.array([0, 0])],
                "prob_b_beats_a": 0}
        if alpha is None:
            alpha = self._alpha
        if required_power is None:
            required_power = self._power

        std = np.sqrt(var_0 / len_0 + var_1 / len_1)
        mean_abs = abs(mean_1 - mean_0)
        mean = mean_1 - mean_0
        sd = np.sqrt((var_0 * len_0 + var_1 * len_1) / (len_0 + len_1 - 2))

        if pvalue is None:
            pvalue = stats.norm.cdf(x=0, loc=mean_abs, scale=std) * 2
        elif not calc_mean:
            std_corrected = np.abs(special.nrdtrisd(0, pvalue / 2, mean_abs))
            sd *= 1 + (std_corrected - std) / std
            std = std_corrected
        else:
            mean_abs = special.nrdtrimn(pvalue / 2, std, 0)
            mean = mean_abs
            if mean_0 > mean_1:
                mean *= -1

        cohen_d = mean_abs / sd
        bound_value = special.nrdtrimn(alpha / 2, std, 0)
        power = 1 - (stats.norm.cdf(x=bound_value, loc=mean_abs, scale=std) - 
                    stats.norm.cdf(x=-bound_value, loc=mean_abs, scale=std))
        analysis = TTestIndPower()
        try:
            sample_size = analysis.solve_power(cohen_d, power=required_power, nobs1=None, alpha=alpha)
        except:
            sample_size = math.inf

        # calculate mean and variance of two beta distribution
        # print("MEAN, LEN", mean_1, len_1)
        alpha_a = round(mean_0 * len_0) + 1
        alpha_b = round(mean_1 * len_1) + 1
        beta_a = len_0 - alpha_a + 2
        beta_b = len_1 - alpha_b + 2
        mean_beta_a = alpha_a / (alpha_a + beta_a)
        mean_beta_b = alpha_b / (alpha_b + beta_b)
        var_beta_a = (alpha_a * beta_a) / (np.power(alpha_a + beta_a, 2) * (alpha_a + beta_a + 1))
        var_beta_b = (alpha_b * beta_b) / (np.power(alpha_b + beta_b, 2) * (alpha_b + beta_b + 1))

        # using Central limit theorem instead of simulations to get precise results
        # print("var_beta_a=", var_beta_a, 'var_beta_b=', var_beta_b)
        normal_std = np.sqrt(var_beta_a + var_beta_b)
        normal_mean = mean_beta_a - mean_beta_b
        # probability that b beats a
        conv_prob_b_beats_a = stats.norm.cdf(x=0, loc=normal_mean, scale=normal_std)

        return {"pvalue": pvalue, "power": power, 
                "cohen_d": cohen_d, "sample_size": np.ceil(sample_size), 
                "enough": sample_size <= min(len_0, len_1),
                "ci": [np.array([stats.norm.ppf(alpha / 2, mean, std), 
                    stats.norm.ppf(1 - alpha / 2, mean, std)])],
                "prob_b_beats_a": conv_prob_b_beats_a}

    def evaluate_metrics(self, df: DF_Processor):
        data = df()
        column_groups = df.process()
        # print(column_groups)
        metric_config = df.metric_config
        date_column = column_groups["date cohort"][0]
        variation_column = column_groups["variation"][0]

        control = data[data[variation_column] == 1]
        test_variations = data[data[variation_column] != 1]

        results = {
            'cohort_date': [],
            'metric': [],
            'test_variation': [],
            'pvalue': [],
            'prob_b_beats_a': [],
            'ci_lower': [],
            'ci_upper': [],
            'test': [],
            'control': [],
            'mean_diff': [],
            'mean_diff, %': []
        }

        stat_results = {
            'cohort_date': [],
            'metric': [],
            'test_variation': [],
            'control': [],
            'test': [],
            'diff': []
        }

        for cohort_date in data[date_column].unique():
            control = data[(data[date_column] == cohort_date) & (data['variation'] == 1)]
            test_variations = data[(data[date_column] == cohort_date) & (data['variation'] != 1)]
            
            for variation in test_variations['variation'].unique():
                test = test_variations[test_variations['variation'] == variation]

                for metric in metric_config.keys():
                    if metric == "STATS" or metric_config[metric]["mean"] not in data.columns:
                        continue
                    mean_0 = control[metric_config[metric]["mean"]].values[0]
                    mean_1 = test[metric_config[metric]["mean"]].values[0]
                    results['control'].append(mean_0)
                    results['test'].append(mean_1)
                    results['mean_diff, %'].append((mean_1 - mean_0) / mean_0 * 100)
                    if "percent" in metric_config[metric] and metric_config[metric]["percent"]:
                        mean_0 /= 100
                        mean_1 /= 100
                    if "distribution" in metric_config[metric] and metric_config[metric]["distribution"] == 'bernoulli':
                        var_0 = mean_0 * (1 - mean_0)
                        var_1 = mean_1 * (1 - mean_1)
                    elif "distribution" in metric_config[metric] and metric_config[metric]["distribution"] == 'poisson':
                        var_0 = mean_0 - metric_config[metric]["truncated"]
                        var_1 = mean_1 - metric_config[metric]["truncated"]
                    else:
                        var_0 = control[metric_config[metric]["var"]].values[0]
                        var_1 = test[metric_config[metric]["var"]].values[0]
                    len_0 = control[metric_config[metric]["len"]].values[0]
                    len_1 = test[metric_config[metric]["len"]].values[0]
                    stats_result = self.calc_stats(mean_0, mean_1, var_0, var_1, len_0, len_1)
                    results['cohort_date'].append(cohort_date)
                    results['metric'].append(metric_config[metric]["title"])
                    results['test_variation'].append(variation)
                    results['pvalue'].append(stats_result['pvalue'])
                    results['prob_b_beats_a'].append(stats_result['prob_b_beats_a'])
                    if "percent" in metric_config[metric] and metric_config[metric]["percent"]:
                        results['ci_lower'].append(stats_result['ci'][0][0] * 100)
                        results['ci_upper'].append(stats_result['ci'][0][1] * 100)
                        results['mean_diff'].append((mean_1 - mean_0) * 100)
                    else:
                        results['ci_lower'].append(stats_result['ci'][0][0])
                        results['ci_upper'].append(stats_result['ci'][0][1])
                        results['mean_diff'].append((mean_1 - mean_0))

                for metric in metric_config["STATS"].keys():
                    if metric not in data.columns:
                        continue
                    mean_0 = control[metric].values[0]
                    mean_1 = test[metric].values[0]
                    stat_results['cohort_date'].append(cohort_date)
                    stat_results['metric'].append(metric_config["STATS"][metric])
                    stat_results['test_variation'].append(variation)
                    stat_results['control'].append(mean_0)
                    stat_results['test'].append(mean_1)
                    stat_results['diff'].append((mean_1 - mean_0) / mean_0 * 100)

        results_df = pd.DataFrame(results)
        # results_df['cohort_date'] = pd.to_datetime(results_df['cohort_date'], format='%d/%m/%y').dt.strftime("%Y-%m-%d")
        results_df['cohort_date'] = pd.to_datetime(results_df['cohort_date']).dt.strftime("%Y-%m-%d")
        stat_results_df = pd.DataFrame(stat_results)
        # stat_results_df['cohort_date'] = pd.to_datetime(stat_results_df['cohort_date'], format='%d/%m/%y').dt.strftime("%Y-%m-%d")
        stat_results_df['cohort_date'] = pd.to_datetime(stat_results_df['cohort_date']).dt.strftime("%Y-%m-%d")

        self._calc_session = str(uuid.uuid4())

        return results_df, stat_results_df

    def create_summary_table(self, df, stats=False):
        latest_date = df['cohort_date'].max()
        latest_df = df[df['cohort_date'] == latest_date]
        metrics = latest_df['metric'].unique()
        variations = sorted(latest_df['test_variation'].unique())

        full_table = pd.DataFrame(columns=metrics, index=["control"])
        for metric in metrics:
            full_table.loc['control', metric] = latest_df.loc[latest_df["metric"] == metric, "control"].values[0]
        
        for var in variations:
            if stats:
                new_table = pd.DataFrame(columns=metrics, index=[f'variation {var}', 'diff, %'])
            else:
                new_table = pd.DataFrame(columns=metrics, index=[f'variation {var}', 'diff, %', 'pvalue'])
                # new_table = pd.DataFrame(columns=metrics, index=[f'variation {var}', 'diff, %', 'pvalue', 'cumulatives'])
            for metric in metrics:
                metric_data = latest_df[latest_df['metric'] == metric]
                var_data = metric_data[metric_data['test_variation'] == var]
                control_value = metric_data[metric_data['test_variation'] == variations[0]]['control'].values[0]
                var_data = metric_data[metric_data['test_variation'] == var]
                if not var_data.empty:
                    new_table.loc[f'variation {var}', metric] = var_data['test'].values[0]
                    if stats:
                        new_table.loc['diff, %', metric] = var_data['diff'].values[0]
                        # new_table.loc["pvalue", metric] = 1
                    else:
                        new_table.loc['diff, %', metric] = var_data['mean_diff, %'].values[0]
                        new_table.loc["pvalue", metric] = var_data['pvalue'].values[0]
            full_table = pd.concat([full_table, new_table], sort=False)
        if not stats:
            new_table = pd.DataFrame(columns=metrics, index=[f'cumulatives'])
            full_table = pd.concat([full_table, new_table], sort=False)

        return full_table
