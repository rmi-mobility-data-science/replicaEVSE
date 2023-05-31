# module to house functions for creating plots like 
# load curves etc. 

import matplotlib.pyplot as plt
import numpy as np

def plot_simulated_load(df_sub, fig_name):
    fig, axes = plt.subplots(ncols=2, figsize=(10,5), sharey=True)
    for ax, day in zip(axes, ['thursday', 'saturday']):
        locations = df_sub.location.unique().copy()[::-1]
        max_load = 0
        base = [0]*len(set(df_sub.hour))
        for idx, loc in enumerate(locations):
            temp = df_sub.loc[(df_sub.weekday==day) & (df_sub.location==loc)].copy().sort_values(by='hour')
            load = list(np.array(base) + temp.load_kW)
            ax.grid(axis='y', which='major', alpha=0.3)
            ax.fill_between(x=list(temp.hour)+list(temp.hour)[::-1],
                        y1=load+base[::-1], 
                        label=locations[idx].replace('_',' ').title(),
                        alpha=1)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            
            base = load
            max_temp = np.max(base)
            max_load = np.max([max_temp, max_load])
        # ax.title(day.title())

    if day == 'saturday':
        plt.legend(loc='upper left', frameon=False)
    axes[0].set_title('Thursday')
    axes[1].set_title('Saturday')
    axes[0].set_ylabel('Load (kW)')
    axes[0].set_xlabel('Hour')
    axes[1].set_xlabel('Hour')

    fig.suptitle('Simulated Load for LDVs in Spokane County, 2022', 
                fontsize=16,
                y=1.03)
    plt.subplots_adjust(wspace=0.05)
    plt.savefig('../visualizations/simulated_load_LDV_spokane_2022.png', dpi=300, bbox_inches='tight')
    plt.show()
    return fig, axes

