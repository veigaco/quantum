import pprint
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

cleanmin = lambda x: max(float(x), 1)
short_float = lambda x: '%.3f' % x
date_fmt = '%Y-%m-%d'

def clean_idx(df, s):
    'Utility clean up functions'
    dfidx = df.index.dropna()
    df = df.loc[dfidx].copy()
    rows = df[df.index.str.contains(s) == True]
    if len(rows) > 0:
        idx = df[df.index.str.contains(s) == True].index
        df = df.drop(idx, axis=0)
    return df

def clean_nas(df):
    cols = df.count().sort_values()[df.count().sort_values() < 1].index.tolist()
    df = df.drop(cols, axis=1)
    df.fillna(method='pad', inplace=True)
    df.fillna(method='bfill', inplace=True)
    df = df.applymap(cleanmin)
    return df

def port_metrics(px, rec):
    px.sort_index(inplace=True)
    returns = px[rec.index.tolist()].pct_change().dropna()
    mean_daily_returns = returns.mean()
    cov_matrix = returns.cov()
    weights = np.asarray(rec.values)
    mult = len(returns.index)
    port_return = np.dot(mean_daily_returns.values, weights) * mult
    port_risk = np.sqrt(np.dot(weights.T, np.dot(cov_matrix.values, weights))) * np.sqrt(mult)
    return port_return[0], port_risk[0][0]

def calc_port_performance(arr, weights):
    return np.cumprod(np.sum(arr * weights, axis=1) + 1)

# cleans small positions in long-short portfolios based on minimum weight criteria
def filter_ls(alloc, pos_filter):
    last_alloc = alloc[-1:].T
    last_alloc.columns = ['Allocation']
    return last_alloc[last_alloc.abs().values > pos_filter]

def compound(df):
    pct = df.pct_change() + 1
    pct.iloc[0] = 1
    return pct.cumprod()

def sect_group_stats(recommend, col):
    re_group = recommend.groupby(by=col)
    print("Total % Allocation {0:.2f}".format(recommend.Allocation.sum() * 100));
    sector_cols = ['Sector Weight', 'Avg Position']
    sector_df = pd.DataFrame([], index=pd.unique(recommend[col]), columns=sector_cols)
    sector_df[sector_df.columns[0]] = re_group.sum()
    sector_df[sector_df.columns[1]] = re_group.mean()
    return sector_df

def plot_two_series(tsa, tsb, label1, label2, xlabel, ylabel, title, fig=(4,3)):
    ax = tsa.plot(fontsize='small', figsize=fig)
    tsb.plot(ax=ax)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(loc='best', fontsize='small')
    ax.set_title(title, fontsize='small')
    
def plot_chart_grid(consol_px, port_df, w_col, lookback, tickers, cols=5):
    nbr_charts = len(sorted(tickers));
    fig, ax = plt.subplots(nbr_charts // cols, cols, figsize=(14,10), sharex=True)
    for i, axi in enumerate(ax.flat):
        co = tickers[i]
        df_range = compound(consol_px[co][-lookback:])
        weight = port_df.loc[co][w_col]
        color = '-g' if weight > 0 else '-r'
        axi.plot(df_range.index.to_datetime().tolist(), df_range.values, color, label=df_range.name)
        axi.legend(fontsize='small')
        axi.xaxis.set_major_locator(mdates.MonthLocator())
        axi.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        axi.yaxis.set_ticks([0.75, 1.25], minor=True)
        
def get_trading(port_bal, consol_px, tickers, lookback, alloc, recommend):
    last_px = consol_px[tickers][-1:]
    trading_cols = ['Name', 'Price', 'Allocation', 'Dollar Value', 'Shares']
    trading_df = pd.DataFrame([], index=alloc.index, columns=trading_cols)
    trading_df['Name'] = recommend['Company']
    trading_df['Price'] = last_px.T
    trading_df['Allocation'] = recommend['Allocation']
    trading_df['Dollar Value'] = trading_df['Allocation'] * port_bal
    trading_df['Shares'] = trading_df['Dollar Value'] / trading_df['Price']
    trading_df = trading_df.astype({'Dollar Value':np.int, 'Shares':np.int})
    return trading_df.sort_index()

def get_sector_trading(port_bal, consol_px, tickers, lookback, alloc):
    last_px = consol_px[tickers][-1:]
    trading_cols = ['Price', 'Allocation', 'Dollar Value', 'Shares']
    trading_df = pd.DataFrame([], index=alloc.index, columns=trading_cols)
    trading_df['Price'] = last_px.T
    trading_df['Allocation'] = alloc['Allocation']
    trading_df['Dollar Value'] = trading_df['Allocation'] * port_bal
    trading_df['Shares'] = trading_df['Dollar Value'] / trading_df['Price']
    trading_df = trading_df.astype({'Dollar Value':np.int, 'Shares':np.int})
    return trading_df.sort_index()

def summary_stats(consol_px, tickers, lookback, alloc, recommend, trading_df):
    last_px = consol_px[tickers][-1:]
    ret, risk = port_metrics(consol_px[tickers][-lookback:], alloc)
    date = last_px.index.to_datetime().strftime('%Y-%m-%d')[0]
    stats_map = {
        "Date": date,
        "Positions": len(recommend.index),
        "Long": len(recommend[recommend['Allocation'] > 0]),
        "Short": len(recommend[recommend['Allocation'] < 0 ]),
        "Long Exposure": recommend[recommend['Allocation'] > 0]['Allocation'].sum(),
        "Short Exposure": recommend[recommend['Allocation'] < 0]['Allocation'].sum(),
        "Net Dollar Exposure":trading_df['Dollar Value'].sum(),
        "Total Allocation":(trading_df['Allocation'].sum() * 100),
        "Return (Ann)": ret,
        "Risk (Vol)": risk,
        "Sharpe Ratio": ret / risk}
    return stats_map