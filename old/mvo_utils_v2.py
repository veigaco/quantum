from math import *
from datetime import datetime, date, time, timedelta
from time import sleep
import pandas_datareader.data as web
import numpy as np
import pandas as pd
import cvxpy as cvx
import re, os
import matplotlib.pyplot as plt
import fix_yahoo_finance as yf

component_path = "./sector_components/"
pricing_path = "./pricing/"
LT_PRICING_PATH = "./pricing/lt_pricing"

#variables for data download
frame = 20 #for limiting the range of optimizations, 1 year
hist_window = frame * 5 #for historical pricing

date_fmt = '%Y-%m-%d'
start_date = datetime.now() - timedelta(hist_window)
start_date = start_date.strftime(date_fmt)
sleep_time = 5

sector_tickers_map = {}
companies = pd.DataFrame([])

def last_allocation(alloc, min_weight):
    last_alloc = alloc[-1:].T
    last_alloc.columns = ['Allocation']
    last_alloc = last_alloc[last_alloc[last_alloc.columns[0]] > min_weight]
    return last_alloc

# Portfolio utils
p_template = "{0} Return: {1:.2f}, StdDev: {2:.2f}, Sharpe: {3:.2f}"

def calc_port_performance(arr, weights):
    return np.cumprod(np.sum(arr * weights, axis=1) + 1)

def date_rules(date_range, tgt_date_str, freq):
    #return a list of dates
    tgt_dt = datetime.strptime(tgt_date_str, date_fmt)
    return date_range[:date_range.index(tgt_dt)+1][::-freq]

def date_intervals(df, freq):
    return df.resample(freq, closed='left', label='left').mean() #using pandas

# This calculates the variance of a time series, not a portfolio
# RENAME TO ts_metrics
def portfolio_metrics(name, pdf):
    timespan = len(pdf.index)
    ret = (pdf.pct_change().mean() * timespan).values[0]
    std = (pdf.pct_change().std() * sqrt(timespan)).values[0]
    if log: print(p_template.format(name, ret, std, ret / std))
    return ret, std, ret / std

# new port metrics
def port_metrics(px, rec):
    # this is supposed to be the righ way to calculate the portfolio risk
    px.sort_index(inplace=True)
    returns = px[rec.index.tolist()].pct_change().dropna()
    mean_daily_returns = returns.mean()
    cov_matrix = returns.cov()
    weights = np.asarray(rec.values)
    mult = len(returns.index)
    port_return = np.dot(mean_daily_returns.values, weights) * mult
    port_risk = np.sqrt(np.dot(weights.T, np.dot(cov_matrix.values, weights))) * np.sqrt(mult)
    return port_return[0], port_risk[0][0]

# Mean variance optimization
def get_mean_variance(rets):
    w_len = rets.shape[1] # number of columns
    eq_weights = np.asarray([1/w_len for _ in range(w_len)]) #default weights
    mu = rets.mean(); std_dev = rets.std(); cov_matrix = rets.cov()
    return w_len, eq_weights, mu.values, std_dev, cov_matrix.values

def get_mvo_allocations(n, mu_ret, cov_mtrx, min_sum, max_sum, min_w, max_w, gamma_val):
    mu = mu_ret.T; Sigma = cov_mtrx; w = cvx.Variable(n)
    gamma = cvx.Parameter(sign='positive')
    ret = mu.T * w; risk = cvx.quad_form(w, Sigma)
    prob = cvx.Problem(cvx.Maximize(ret - gamma*risk), 
        [cvx.sum_entries(w) >= min_sum, cvx.sum_entries(w) <= max_sum, 
         w > min_w, w < max_w])
    gamma.value = gamma_val; prob.solve()
    if prob.status == 'optimal': return w.value

def get_weights(px, as_of, lb, min_sum, max_sum, min_w, max_w, gamma):
    lb_rets = px.sort_index().pct_change().dropna().tail(lb) # capture the last lb days going back
    # the below line make the old and new version return the same results, new implementation is more accurate
    # lb_rets = px.sort_index().pct_change(); lb_rets.iloc[0] = 0
    n, weights, mu_ret, std_dev, cov_mtrx = get_mean_variance(lb_rets)
    np_weights = np.array(get_mvo_allocations(n, mu_ret, cov_mtrx, min_sum, max_sum, min_w, max_w, gamma))
    lb_weights = pd.DataFrame(np_weights.T, index=[as_of], columns=lb_rets.columns)
    return lb_rets, lb_weights

def recommend_allocs(px, frame, lb, as_of, min_sum, max_sum, min_w, max_w, gamma):
    px = clean_nas(px); px_portion = px[-frame:].copy()
    returns, alloc = get_weights(px_portion, as_of, lb, min_sum, max_sum, min_w, max_w, gamma)
    port_perf = calc_port_performance(returns.values, alloc.values)
    pdf = pd.DataFrame(port_perf, index=returns.index, columns=["Quantum"])
    return px_portion, returns, alloc, pdf

# Sector analytics

def sect_group_stats(recommend, col):
    re_group = recommend.groupby(by=col)
    print("Total % Allocation {0:.2f}".format(recommend.Allocation.sum() * 100));
    sector_cols = ['Sector Weight', 'Avg Position']
    sector_df = pd.DataFrame([], index=pd.unique(recommend[col]), columns=sector_cols)
    sector_df[sector_df.columns[0]] = re_group.sum()
    sector_df[sector_df.columns[1]] = re_group.mean()
    return sector_df

# DOWNLOAD / LOAD Utility Methods

# Load component from ETF holding CSVs
col_names = ['Symbol','Company', 'Weight']
def load_components(cos, pattern, cols, idxcol, sectors, srows=1):
    flist = os.listdir(component_path)
    files = [f for f in flist if f.startswith(pattern)]
    for s in sectors:
        fname = component_path + pattern + s.lower() + '.csv'
        df = pd.read_csv(fname, skiprows=srows, index_col=idxcol, usecols=cols)
        df.index.name = col_names[0]
        df.columns = col_names[1:]
        df = clean_idx(df, ' ')
        df['ETF'] = s
        sector_tickers_map[s] = df.index.tolist()
        cos = cos.append(df)
    return cos

# Load pricing from hard drive
# PENDIND: implement path speficic pricing
def load_pricing(f, idx_col):
    fname = pricing_path + f
    px = pd.read_csv(fname, index_col=idx_col, parse_dates=True)
    px.sort_index(ascending=True, inplace=True)
    if log: print("Loaded pricing for {}, with shape {}".format(f, px.shape))
    return px

# Util function to load components for different benchmarks
def load_consol_px(ticker_map, tm_key):
    consol_px = pd.DataFrame([])
    for key in ticker_map[tm_key]:
        px = load_pricing(key + '-hold-pricing.csv', 'Date').copy()
        ccols = set(consol_px.columns.tolist())
        newcols = set(px.columns.tolist())
        consol_px = consol_px.merge(
            px[list(newcols.difference(ccols))], 
            left_index=True, 
            right_index=True, 
            how='outer')
    return consol_px

# Downloads pricing on all components for each ETF
def get_pricing(fname, ticker_list, start_date):
    if log: print("Getting pricing for:", fname, start_date)
    #px = web.DataReader(ticker_list,data_source='yahoo',start=start_date)['Adj Close']
    px = yf.download(ticker_list, start=start_date)['Adj Close']
    if isinstance(px, pd.DataFrame)==False: 
        px=px.to_frame()
        px.columns=ticker_list
    px.sort_index(ascending=True, inplace=True)
    px.to_csv(pricing_path + fname)
    return px

# Exception safe downloader
def get_safe_pricing(fname, ticker_list, s_date):
    while True:
        try:
            get_pricing(fname, ticker_list, s_date); break
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)

#For each ETF downloads 
def refresh_components(etfs):
    while len(etfs) > 0: 
        val = etfs[-1]; 
        tickers = sector_tickers_map[val] # for individual components
        get_safe_pricing(val + '-hold-pricing.csv', tickers, start_date)
        etfs.pop()

# Plots two series
def plot_two_series(tsa, tsb, label1, label2, xlabel, ylabel, title):
    ax = tsa.plot(fontsize='small', figsize=(4,3))
    tsb.plot(ax=ax)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.legend(loc='best', fontsize='small')
    ax.set_title(title, fontsize='small')

# CLEAN UTILITIES

def compound(df):
    pct = df.pct_change() + 1
    pct.iloc[0] = 1
    return pct.cumprod()

def clean_load(pattern, idxcol, cols, col_names, s, srows=0):
    fname = component_path + pattern + s.lower() + '.csv'
    df = pd.read_csv(fname, skiprows=srows, index_col=idxcol, usecols=cols)
    df.index.name = col_names[0]
    df.columns = col_names[1:]
    return df

def clean_nas(df):
    cols = df.count().sort_values()[df.count().sort_values() < 1].index.tolist()
    df = df.drop(cols, axis=1)
    df.fillna(method='pad', inplace=True)
    df.fillna(method='bfill', inplace=True)
    df = df.applymap(cleanmin)
    return df

def clean_idx(df, s):
    'Utility clean up functions'
    dfidx = df.index.dropna()
    df = df.loc[dfidx].copy()
    rows = df[df.index.str.contains(s) == True]
    if len(rows) > 0:
        idx = df[df.index.str.contains(s) == True].index
        df = df.drop(idx, axis=0)
    return df