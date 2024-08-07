import matplotlib.pyplot as plt

class PlotBuilder:
    def __init__(self, path) -> None:
        self._path = path
        return

    def save_plots(self, results_df):
        plot_linewidth = 4
        plt.rcParams.update({'font.size': 18})
        for metric in results_df['metric'].unique():
            subset = results_df[results_df['metric'] == metric]
            
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 16), gridspec_kw={'height_ratios': [2, 2]}, sharex=True)
            
            # Upper part: p-values without confidence intervals
            for variation in subset['test_variation'].unique():
                variation_subset = subset[subset['test_variation'] == variation]
                ax1.plot(variation_subset['cohort_date'], variation_subset['pvalue'], marker='o', label=f'Variation {variation}', linewidth=plot_linewidth)
                # ax1.plot(variation_subset['cohort_date'], variation_subset['prob_b_beats_a'], marker='o', label=f'Variation {variation}', linewidth=plot_linewidth)
            ax1.axhline(y=0.05, color='r', linestyle='--', label='alpha = 0.05', linewidth=plot_linewidth)
            ax1.set_ylabel('Cumulative P-value')
            ax1.set_title(f'P-values for {metric} by Cohort Date')
            ax1.legend()
            ax1.grid(True)

            colors = plt.cm.get_cmap('tab10', len(subset['test_variation'].unique()))
            index = 0
            
            # Lower part: difference in metrics with confidence intervals
            for variation in subset['test_variation'].unique():
                variation_subset = subset[subset['test_variation'] == variation]
                mean_diff = variation_subset['mean_diff']
                ci_lower = variation_subset['ci_lower']
                ci_upper = variation_subset['ci_upper']
                color = colors(index)
                ax2.plot(variation_subset['cohort_date'], mean_diff, marker='o', label=f'Variation {variation}', linewidth=plot_linewidth)
                # ax2.vlines(x=variation_subset['cohort_date'], ymin=ci_lower, ymax=ci_upper, color='blue', linewidth=plot_linewidth)
                ax2.fill_between(variation_subset['cohort_date'], ci_lower, ci_upper, color=color, alpha=0.2)
                index += 1
            
            ax2.axhline(y=0, color='r', linestyle='--', label='Zero Difference', linewidth=plot_linewidth)
            ax2.set_ylabel('Cumulative Difference in Metric')
            ax2.legend()
            ax2.grid(True)

            fig.autofmt_xdate()

            # Common X-axis and title for the lower part
            # fig.text(0.5, 0.5, 'Cohort Date', ha='center')
            # fig.text(0.5, 0.07, f'Means difference for {metric} by Date', ha='center')
            
            # Adjust layout to avoid overlap
            plt.tight_layout(rect=[0, 0.08, 1, 0.95])

            # Rotate x-axis labels to avoid overlap
            # plt.setp(ax2.get_xticklabels(), rotation=15, ha='right')
            
            # Save plot as PNG
            plt.savefig(f'{self._path}{metric}_pvalues_diff_confidence_intervals.png')
            plt.close()
