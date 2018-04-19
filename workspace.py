import numpy as np
import pandas as pd
import json, re, os
import pandas_datareader.data as web
import fix_yahoo_finance as yf
import cvxpy as cvx
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from mvo_utils_v3 import *
from datetime import datetime, date, time, timedelta
from math import *
from time import sleep

class OptWorkspace:
    'Global variables holder for simple optimizations parameters'

    min_len = 3
    pos_filter = 0.01  # cleaning variables
    
    defaultGammas = np.logspace(0, 3, num=100)  # 1 to 1000 range
    globalVarsFmt = "Global Variables\n" \
        "Refresh Pricing: {}\n" + "Historical Time Window: {}\n" + "Lookback: {}\n" \
        + "Rebalance Frequency: {}\n" + "Net Exposure: {}\n" \
        + "Leverage: {}\n" + "Weights: min {} to max {}\n" + "Universe: {}\n"
    
    PRICING_PATH = "./pricing/"
    LT_PRICING_PATH = "./pricing/lt"
    COMPONENT_PATH = "./components/"
    BACKTEST_PATH = './backtests/'
    HISTREC_PATH = "./recommendations/"
    CONFIG_PATH = './config/'
    paths = [CONFIG_PATH, PRICING_PATH, LT_PRICING_PATH, COMPONENT_PATH, BACKTEST_PATH, HISTREC_PATH]
    
    dateFormat = '%Y-%m-%d'
    sleepTime = 5
    
    sector_tickers_map = {}
    companies = pd.DataFrame([])
    
    def __init__(self, lookback, netExposure, leverage, minWeight, maxWeight, 
                 universe='spy-sectors', activeETF=None, etfOnly=False, historicWindow = 60):
        self.lookback = lookback
        self.netExposure = netExposure
        self.leverage = leverage
        self.minWeight = minWeight
        self.maxWeight = maxWeight
        self.universe = universe
        self.activeETF = activeETF
        self.etfOnly = etfOnly
        # default settings
        self.log = True
        self.refreshPricing = True
        self.historicWindow = historicWindow
        self.rebalanceFrequency = 'W-' + datetime.now().strftime('%a') # today's date
        self.gammaVals = OptWorkspace.defaultGammas # 1 to 1000 range, 10 steps
        self.startDate = (datetime.now() - timedelta(self.historicWindow)).strftime(OptWorkspace.dateFormat)

        # creates all directories
        for p in OptWorkspace.paths: 
            if not os.path.exists(p): os.makedirs(p)
        
        # loads the ticker map
        with open(OptWorkspace.CONFIG_PATH + 'ticker_map.json', 'r') as fp: self.ticker_map = json.load(fp)
        with open(OptWorkspace.CONFIG_PATH + 'config.json', 'r') as fp: self.config = json.load(fp)
    
    def print_global_vars(self):
        'Explicit function to review global vars'
        print(self.globalVarsFmt.format(
            self.refreshPricing, 
            self.historicWindow, 
            self.lookback, 
            self.rebalanceFrequency, 
            self.netExposure, 
            self.leverage,
            self.minWeight,
            self.maxWeight,
            self.universe))

    def get_pricing(self, fname, ticker_list, columns='Adj Close'):
        'Load pricing from hard drive'
        if log: print("Getting pricing for:", fname, self.startDate)
        px = yf.download(ticker_list, self.startDate, as_panel=False)[columns]
        if isinstance(px, pd.DataFrame)==False: 
            px=px.to_frame()
            px.columns=ticker_list
        px.sort_index(ascending=True, inplace=True)
        px.to_csv(OptWorkspace.PRICING_PATH + fname)
        return px

    def load_px(self, fileName=None, indexCol='Date'):
        'Load pricing from drive'
        fileName = self.universe + '.csv' if fileName == None else fileName
        fname = OptWorkspace.PRICING_PATH + fileName
        px = pd.read_csv(fname, index_col=indexCol, parse_dates=True)
        px.sort_index(ascending=True, inplace=True)
        if self.log: print("Loaded pricing for {}, with shape {}".format(fileName, px.shape))
        return px

    def load_companies(self):
        'Load components from holding files'
        col_names = ['Symbol','Company', 'Weight']
        pattern = self.config[self.universe]['hold_format']
        cols = self.config[self.universe]['hold_cols']
        idxcol = self.config[self.universe]['idx_col']
        srows = self.config[self.universe]['skiprows']
        sectors = self.ticker_map[self.universe]
        self.companies = pd.DataFrame([])        
        flist = os.listdir(OptWorkspace.COMPONENT_PATH)
        files = [f for f in flist if f.startswith(pattern)]
        for s in sectors:
            fname = OptWorkspace.COMPONENT_PATH + pattern + s.lower() + '.csv'
            df = pd.read_csv(fname, skiprows=srows, index_col=idxcol, usecols=cols)
            df.index.name = col_names[0]
            df.columns = col_names[1:]
            df = clean_idx(df, ' ')
            df['ETF'] = s
            self.sector_tickers_map[s] = df.index.tolist()
            self.companies = self.companies.append(df)
        if (self.activeETF != None): self.companies = self.companies[companies['ETF'] == activeETF] # filter by selected ETF
        if log: print("Companies loaded:", self.companies.shape)    
        return self.companies    
    
    def drop_companies(self, tickers):
        remove = list(set(tickers).intersection(self.companies.index.tolist()))
        if len(remove) > 0: self.companies.drop(remove, axis=0, inplace=True)
        return self.companies
    
    def refresh_sector_px(self):
        'Downloads sector prices'
        self.get_pricing(self.universe + '.csv', self.ticker_map[self.universe])

    def refresh_component_px(self):
        'Gets pricing for all components of each ETF'
        etfs = self.ticker_map[self.universe]
        while len(etfs) > 0: 
            val = etfs[-1]; 
            tickers = self.sector_tickers_map[val] # for individual components
            self.get_pricing(val + '-hold-pricing.csv', tickers)
            etfs.pop()
    
    def load_universe_px(self):
        'Utility function to load components for different benchmarks'
        universe_px = pd.DataFrame([])
        for key in self.ticker_map[self.universe]:
            px = self.load_px(key + '-hold-pricing.csv', 'Date').copy()
            ccols = set(universe_px.columns.tolist())
            newcols = set(px.columns.tolist())
            universe_px = universe_px.merge(
                px[list(newcols.difference(ccols))], 
                left_index=True, right_index=True, how='outer')
        return clean_nas(universe_px)
    
    def ls_recommend_allocs(self, px, gamma_val):
        'Generate best portfolio allocations'
        px_portion = px[-self.lookback:] # subselect the period to optimize; frame and lb were duplicatives
        returns, alloc = self.ls_get_weights(px_portion, gamma_val) # subselect the period to optimize
        port_perf = calc_port_performance(returns.values, alloc.values)
        pdf = pd.DataFrame(port_perf, index=returns.index, columns=["Quantum"])
        return px_portion, returns, alloc, pdf
    
    # cleans small positions in long-short portfolios based on minimum weight criteria
    def filter_ls(rec, pos_filter):
        long = rec[(rec > pos_filter).values]; short = rec[(rec < -pos_filter).values]
        return long.append(short)

    def ls_get_weights(self, px, gamma_val):
        'Optimization for position weights'
        lb_rets = px.sort_index().pct_change().fillna(0) # capture the last lb days going back
        n = lb_rets.shape[1]; mu = lb_rets.mean().values.T; Sigma = lb_rets.cov().values
        w = cvx.Variable(n)
        gamma = cvx.Parameter(sign='positive')
        Lmax = cvx.Parameter()
        ret = mu.T * w; risk = cvx.quad_form(w, Sigma)
        lo_obj = cvx.Maximize(ret - gamma*risk)
        ls_const = [cvx.sum_entries(w) == self.netExposure, cvx.norm(w, 1) < Lmax, w <= self.maxWeight, w >= self.minWeight]
        prob = cvx.Problem(lo_obj, ls_const)
        gamma.value = gamma_val; Lmax.value = self.leverage
        prob.solve()
        weights = w.value if prob.status == 'optimal' else np.zeros((n, 1))
        np_weights = np.array(weights)
        lb_weights = pd.DataFrame(np_weights.T, index=[lb_rets.index[-1]], columns=lb_rets.columns)
        return lb_rets, lb_weights

    def quick_gamma(self, glist, px):
        'Calculation best gamma risk parameter'
        if len(glist) <= 1: 
            mid_g = glist[0]
            mid_sr = self.get_sr_for_opt(px, mid_g)
            return mid_g, mid_sr
        else:
            mid = len(glist) // 2; left = glist[:mid]; right = glist[mid:]
            mid_l = left[len(left) // 2]; mid_r = right[len(right) // 2]
            left_sr = self.get_sr_for_opt(px, mid_l)
            right_sr = self.get_sr_for_opt(px, mid_r)
            sublist = left if left_sr > right_sr else right
            return self.quick_gamma(sublist, px)

    def get_sr_for_opt(self, px, gamma_val):
        'Returns portfolio sharpe for a given optimization'
        px_p, _, alloc, pdf = self.ls_recommend_allocs(px, gamma_val)
        rec = filter_ls(alloc, 0.01)
        ret, risk = port_metrics(px_p, rec)
        return ret / risk
    
    def save_recommendation(self, trading_df):
        path = OptWorkspace.HISTREC_PATH
        pre_name = path + "portrec_" + self.universe + '_'
        fname = datetime.now().strftime(pre_name + date_fmt + r'_%H-%M') + '.csv'
        trading_df.to_csv(fname)
        print('Saved portfolio rebalance:', fname)