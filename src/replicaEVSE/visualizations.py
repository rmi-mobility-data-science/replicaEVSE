# module to house functions for creating plots like 
# load curves etc. 

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

def _plot_simulated_load(df_sub, fig_name):
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

def add_generic_location(all: pd.DataFrame):
    """Add generic location column to dataframe."""
    
    df = all.copy()
    df['location'] = df.charge_type.copy()
    keep_values = ['single_family_home', 'multi_family_home', 'public']
    home_values = ['public', 'work']

    # Set values not in the keep list to 'work'
    df['location'] = df['location'].where(df['location'].isin(keep_values), 'work')
    df['location'] = df['location'].where(df['location'].isin(home_values), 'home')

    return df

def extract_load_curve(df, locations=None):
    if locations is None:
        locations = ['home', 'public', 'work',]
    df_list_thu = []
    df_list_sat = []

    for loc in locations:
        df_list_thu.append(df[(df['location'] == loc) & (df['weekday'] == 'thursday')].groupby('hour')['load_kW'].sum().to_list())
        df_list_sat.append(df[(df['location'] == loc) & (df['weekday'] == 'saturday')].groupby('hour')['load_kW'].sum().to_list())
    
    return df_list_thu, df_list_sat


def make_load_curve_plot(df_list_thu: list, df_list_sat: list, locations: list=None, loc_label='Spokane') -> tuple:
    """Create load curve plot including HDV and MDV as well as charging
    locations.

    Args:
        df_list_thu (list): list of lists of load curves for thursday
        df_list_sat (list): list of lists of load curves for saturday
        locations (list, optional): charging location labels. Defaults to None.

    Returns:
        tuple: fig, axes
    """
    if locations is None:
        locations = ['home', 'public', 'work',]
    
    fig, axes = plt.subplots(ncols=2, figsize=(10,5), sharey=True) 
    labels = [s.title() for s in locations] + ['HDV', 'MDV']
    axes[0].stackplot(np.arange(24), df_list_thu, labels=labels)
    axes[1].stackplot(np.arange(24), df_list_sat, labels=labels)
    axes[1].legend(loc='upper left', frameon=False)
    axes[0].set_title('Thursday')
    axes[1].set_title('Saturday')
    axes[0].set_ylabel('Load (MW)')
    axes[0].set_xlabel('Hour')
    axes[1].set_xlabel('Hour')
    fig.suptitle(f'Simulated Load for {loc_label} County, 2021', 
                fontsize=16,
                y=1.03)
    plt.subplots_adjust(wspace=0.05)
    for ax in axes:
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.set_xlim(0, 23)
        ax.grid(axis='y', which='major', alpha=0.3)

    return fig, axes
