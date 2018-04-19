import numpy as np
import pandas as pd
import json, re, os
import pandas_datareader.data as web
import fix_yahoo_finance as yf
import cvxpy as cvx
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, date, time, timedelta
from math import *
from time import sleep

# Load pricing from hard drive
def get_pricing(fname, ticker_list, columns='Adj Close'):
    while True:
        try:
            if log: print("Getting pricing for:", fname, self.startDate)
            px = yf.download(ticker_list, opt_env.startDate, as_panel=False)[columns]
            if isinstance(px, pd.DataFrame)==False: 
                px = px.to_frame()
                px.columns = ticker_list
            px.sort_index(ascending=True, inplace=True)
            px.to_csv(OptWorkspace.PRICING_PATH + fname)
            return px
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, self.sleepTime))
            sleep(self.sleepTime)

# load pricing from drive
def load_pricing(self, fileName, indexCol):
    fname = OptWorkspace.PRICING_PATH + fileName
    px = pd.read_csv(fileName, index_col=indexCol, parse_dates=True)
    px.sort_index(ascending=True, inplace=True)
    if self.log: print("Loaded pricing for {}, with shape {}".format(fileName, px.shape))
    self.px = px
    return self.px

# Util function to load components for different benchmarks
def load_consol_px(self, tm_key):
    consol_px = pd.DataFrame([])
    for key in self.ticker_map[tm_key]:
        px = self.load_pricing(key + '-hold-pricing.csv', 'Date').copy()
        ccols = set(consol_px.columns.tolist())
        newcols = set(px.columns.tolist())
        consol_px = self.consol_px.merge(
            px[list(newcols.difference(ccols))],
            left_index=True, right_index=True, how='outer'
        )
    self.px = consol_px # overwrites samller pricing set
    return self.px

# Load component from ETF holding CSVs
def load_components(self):
    self.companies = pd.DataFrame([])
    col_names = ['Symbol','Company', 'Weight']
    pattern = config[self.universe]['hold_format']
    cols = config[self.universe]['hold_cols']
    idxcol = config[self.universe]['idx_col']
    sectors = config[self.universe]['fname']
    srows = config[self.universe]['skiprows']
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
    return self.companies

def get_etfs(self):
    return self.ticker_map[self.universe]

#For each ETF downloads 
def refresh_components(self):
    etfs = self.get_etfs()
    while len(etfs) > 0: 
        val = etfs[-1]; 
        tickers = self.sector_tickers_map[val] # for individual components
        get_pricing(val + '-hold-pricing.csv', tickers)
        etfs.pop()