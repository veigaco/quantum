
# coding: utf-8

# In[1]:

import pandas as pd
import os
from math import *
import re
from datetime import datetime, date, time, timedelta
import fix_yahoo_finance as yf
import matplotlib.pyplot as plt
import numpy as np
from time import sleep
from MPT_utils import * 
import cvxpy as cvx
import numpy as np


# In[2]:

#universe = ['spy','ark'] 
universe = ['spy']
#universe = ['ark']

#variables for data download
hist_window = 100 #for historical pricing
date_fmt = '%Y-%m-%d'
start_date = datetime.now() - timedelta(hist_window)
start_date = start_date.strftime(date_fmt)
sleep_time = 5
lb = 55; min_gross=0.5; max_gross=1; min_w=0; max_w=0.05 # default optimization vars
refresh_pricing = False
min_weight=-0.01 ## minimo peso para portafolio filtrado 

config = {'spy' : {
            'benchmark': 'spy',
            'skiprows': 1,
            'columns': ['Symbol', 'Company Name']},
        'ark' : {
            'benchmark': 'ark' ,
            'skiprows': 0,
            'columns': ['ticker','company']}}

pricing_path = "./pricing/"
component_path = "./sector_components/"
## Script que obtiene los datos de las fuentes:
# 1. Sector components   : Contiene la información de los ETF sectors y sus componentes
# 2. Pricing            : Contiene la informacion historica de precios de cada uno de los componentes
# 3. Optimizacion de Markowitz
# 4. Seleccion de subportafolio
# 5. Rebalanceo


# 1. Sector Components

# In[3]:

### 1. Sector components:
# de donde se extrae esta info, con que frecuencia se actualiza?

## 1.1 descargar componentes del server:

## 1.2 cargar los sector componentes para cada benchmark, cargar cada ETF sector para cada benchmark
# y cada company para cada ETF sectior y limpiarlos

companies_universe= pd.DataFrame({}) ### contiene todas las compañias del etf sector para cada benchmark
sector_etf = {}   ### contiene cada etf sector para cada benchmark
ticker_map= {}    ### contiene todas las compañias para cada etf sector 

for k in universe:
    #print(config[k])
    companies,benchmark_etf_map,sector_tickers_map=load_companies(benchmark=config[k]['benchmark'],skiprows=config[k]['skiprows'],columns=config[k]['columns']) ## companies del benchmark spy
    print("Companies loaded:",config[k]['benchmark'], companies.shape)
    companies_universe=companies_universe.append(companies)
    sector_etf.update(benchmark_etf_map)
    ticker_map.update(sector_tickers_map)


# 2 Pricing

# In[4]:


# 2. Pricing            : Contiene la informacion historica de precios de cada uno de los componentes

## 2.1 descargar componentes del server:

if refresh_pricing==True:
    ### refresh components
    etfs=[ [i,j,ticker_map[j]] for i in universe for j in sector_etf[i] ]
    for i in etfs:
        get_pricing(output_name=pricing_path+i[0]+"/"+i[1]+"-hold-pricing.csv",
        ticker_list=i[2],start_date=start_date)
    ## refresh etf sectors 
    sectors=[[i,sector_etf[i]] for i in universe]
    for j in sectors:
        get_pricing(output_name=pricing_path+j[0]+"/"+j[0]+'-sectors.csv', 
        ticker_list=j[1], start_date=start_date)
    # refresh benchmarks
    benchmarks=[i for i in universe]
    #benchmarks
    for j in benchmarks:
        print(j)
        get_pricing(output_name=pricing_path+ j +'/'+j+'.csv', ticker_list=j, start_date=start_date)


### consol_px contiene los precios historicos de cada compañia
consol_px=load_consol_px(universe)
consol_px=consol_px[sorted(consol_px.columns.tolist())]
print("consol_px"+str(consol_px.shape))
### crear consol_px 

    
# #### Limpiar las compañias que tienen valores nulos en los ultimos 3 dias y valores nulos en toda su serie
consol_px,cols=clean_nas(consol_px) ## limpia de aquellos valores que no tienen valores en su serie o que por alguna razon tienen NAN en los ultimos dias
print("se limpio consol_px"+str(consol_px.shape))
print("se borraron:")
print(cols)


# Removing tickers for M&A targets and diff class shares of same co.
remove_tickers = ['CSRA', 'DPS', 'UAA', 'DISCK', 'JUNO', 'XL', 'WELL', 'BKNG', 'SNI','EVGN'] # example: two M&A targets, diff share classes...
remove = list(set(remove_tickers).intersection(consol_px.columns.tolist()))
if len(remove) > 0:
    consol_px.drop(remove, axis=1, inplace=True)

consol_px.to_csv("consol_mpt_opt.csv")
print("consol_px final: "+str(consol_px.shape))


# 3. Optimización

# In[5]:

# 3. Optimizacion

px_portion = consol_px[-abs(hist_window):].copy() ## tomar los ultimos hist_window rows de la tabla de precios
px_portion= px_portion.sort_index().pct_change(); px_portion.iloc[0] = 0 ## calcular returns
return_vec =px_portion.loc[:px_portion.tail(1).index[0]].tail(lb)#.dropna()

### grafico de comportamientos de returns
plot_returns(return_vec)


# In[6]:


# Portfolio optimization with leverage limit.
weights =np.asarray([1/len(return_vec.columns) for _ in range(len(return_vec.columns))])
mu = return_vec.mean().values ## vector de return mean
n = len(mu) ## numero de compañias
name = return_vec.mean().index.values.tolist() ## nombre de las compañias
Sigma =  return_vec.cov().values  ## covarianza de los retornos

w = cvx.Variable(n) ## variable a optimizar
gamma = cvx.Parameter(sign='positive') ## aversion al riesgo
ret = mu.T*w   # returns
risk = cvx.quad_form(w, Sigma)
# Portfolio optimization with leverage limit.
Lmax = cvx.Parameter()
prob = cvx.Problem(cvx.Maximize(ret - gamma*risk), 
               [cvx.sum_entries(w) == 1, 
                cvx.norm(w, 1) <= Lmax])
# Compute trade-off curve for each leverage limit.

L_vals = [1,1.5, 2, 4]
SAMPLES = 100
w_ = []
risk_data = np.zeros((len(L_vals), SAMPLES))
ret_data = np.zeros((len(L_vals), SAMPLES))
sharpe= np.zeros((len(L_vals), SAMPLES))
gamma_vals = np.logspace(-2, 3, num=SAMPLES)
for k, L_val in enumerate(L_vals):
    for i in range(SAMPLES):
        Lmax.value = L_val
        gamma.value = gamma_vals[i]
        prob.solve()
        if prob.status == 'optimal': 
            #weights =[j[0] for j in w.value.tolist()]
            risk_data[k, i] = sqrt(risk.value)
            ret_data[k,i]= ret.value
            #w_.append(weights)
            sharpe[k,i]=ret.value/sqrt(risk.value)
        if prob.status != 'optimal': 
            print("No optimo para "+str(gamma_vals[i]))




# Plot trade-off curves for each leverage limit.
for idx, L_val in enumerate(L_vals):
    plt.plot(risk_data[idx,:][risk_data[idx,:]>0], ret_data[idx,:][ret_data[idx,:]>0], label=r"$L^{\max}$ = %f.2" % L_val)

for w_val in w_vals:
    w.value = w_val
    plt.plot(sqrt(risk.value), ret.value, 'bs')

plt.xlabel('Standard deviation')
plt.ylabel('Return')
plt.legend(loc='lower right')
plt.show()




w_vals=[]
L_vals = [2]
ret_data_u=[]
risk_data_u=[]
# Portfolio optimization with a leverage limit and a bound on risk.
prob = cvx.Problem(cvx.Maximize(ret), 
              [cvx.sum_entries(w) == 1, 
               cvx.norm(w, 1) <= Lmax,
               risk <= .001])
# Compute solution for different leverage limits.
for k, L_val in enumerate(L_vals):
    Lmax.value = L_val
    prob.solve()
    w_vals.append( w.value )
    ret_data_u.append(sqrt(risk.value))
    risk_data_u.append(ret.value)

# Plot bar graph of holdings for different leverage limits.
colors = ['b', 'g', 'r']
indices = np.argsort(mu.flatten())

for idx, L_val in enumerate(L_vals):
     plt.bar(np.arange(1,n+1) + 0.25*idx - 0.375, w_vals[idx][indices], color=colors[idx], 
             label=r"$L^{\max}$ = %d" % L_val, width = 0.25)

plt.ylabel(r"$w_i$", fontsize=16)
plt.xlabel(r"$i$", fontsize=16)
plt.xlim([1-0.375, 10+.375])
plt.xticks(np.arange(1,n+1))
plt.show()

from scipy import stats
stats.describe(w_vals[0])
plt.hist(w_vals[0], bins=np.arange(-.2, .2, 0.01))
plt.title("distribucion de w")
plt.show()

sum(w_vals[0])
sum(w_vals[0][w_vals[0]>0].tolist()[0])
sum(w_vals[0][w_vals[0]<0].tolist()[0])


recommend =pd.DataFrame(w_vals[0],index=name,columns=["w"])


j=1
n=10
short_names=recommend[recommend["w"]<0].index.tolist()[1:n]
fig = plt.figure(figsize=(15,5))
fig.subplots_adjust(left=0.2, wspace=0.6)
for i in short_names:
    ax=fig.add_subplot(2,5,j) 
    ax.set_title(i)
    ax.plot(consol_px[i].values)
    j=j+1
    #print(i)

plt.show()


j=1
n=10
long_names=recommend[recommend["w"]>0].index.tolist()[1:n]
fig = plt.figure(figsize=(15,5))
fig.subplots_adjust(left=0.2, wspace=0.6)
for i in long_names:
    ax=fig.add_subplot(2,5,j) 
    ax.set_title(i)
    ax.plot(consol_px[i].values)
    j=j+1
    #print(i)

plt.show()



# Outputs for trade execution
px = consol_px[recommend.index][-lb:]
date = pd.to_datetime(px[-1:].index[0])#.strftime(date_fmt)

trading_cols = ['Price', 'Allocation', 'Dollar Value', 'Shares']
trading_df = pd.DataFrame([], index=recommend.index, columns=trading_cols)

port_bal = 231000; 
alloc = recommend["w"].values
pvalue = (port_bal * alloc)
shares = (port_bal * alloc) / px[-1:]
#shares.apply(lambda x: round(x, 6)).T.sort_index()

#trading_df['Company'] = pd.DataFrame(recommend.index)
trading_df['Price'] = px[-1:].T
trading_df['Allocation'] = recommend['w']
trading_df['Dollar Value'] = trading_df['Allocation'] * port_bal
trading_df['Shares'] = trading_df['Dollar Value'] / trading_df['Price']

#trading_df = trading_df.astype({'Dollar Value':np.int, 'Shares':np.int})
trading_df.sort_index()


### subportfolio con short y long
'''
corre=return_vec.corr().values
names= return_vec.corr().index.tolist()
names_un=[]

for i in range(len(corre)):
    for j in range(len(corre)):
        if abs(corre[i,j])<.0005:
            names_un.append(names[i])
            names_un.append(names[j])

names_un=list(set(names_un))
len(names_un)

new=return_vec[names_un]

weights =np.asarray([1/len(new.columns) for _ in range(len(new.columns))])
mu = new.mean().values ## vector de return mean
n = len(mu) ## numero de compañias
name = new.mean().index.values.tolist() ## nombre de las compañias
Sigma =  new.cov().values  ## covarianza de los retornos
# Long only portfolio optimization.
w = cvx.Variable(n) ## variable a optimizar
gamma = cvx.Parameter(sign='positive') ## aversion al riesgo
ret = mu.T*w   # returns
risk = cvx.quad_form(w, Sigma) # risk
Lmax = cvx.Parameter()

w_vals=[]
L_vals = [2]
# Portfolio optimization with a leverage limit and a bound on risk.
prob = cvx.Problem(cvx.Maximize(ret), 
              [cvx.sum_entries(w) == 1, 
               cvx.norm(w, 1) <= Lmax,
               risk <= 2])
# Compute solution for different leverage limits.
for k, L_val in enumerate(L_vals):
    Lmax.value = L_val
    prob.solve()
    w_vals.append( w.value )

# Plot bar graph of holdings for different leverage limits.
colors = ['b', 'g', 'r']
indices = np.argsort(mu.flatten())

for idx, L_val in enumerate(L_vals):
     plt.bar(np.arange(1,n+1) + 0.25*idx - 0.375, w_vals[idx][indices], color=colors[idx], 
             label=r"$L^{\max}$ = %d" % L_val, width = 0.25)

plt.ylabel(r"$w_i$", fontsize=16)
plt.xlabel(r"$i$", fontsize=16)
plt.xlim([1-0.375, 10+.375])
plt.xticks(np.arange(1,n+1))
plt.show()





w_u=[]
for i in w_vals[0]:
    print(i[0])
    if abs(i.tolist()[0][0])<.05 and abs(i.tolist()[0][0])>.01:
        w_u.append(i.tolist()[0][0])
    #Portafolio filtrado con minimos pesos
    portfolio_opt = pd.DataFrame(w_,index=name,columns=["Allocation"])
    return_subportfolio,risk_subportfolio,sharpe_subportfolio,weights_subportfolio=get_subportfolio(portfolio_opt,min_weight,px_portion)
    risk_data_sub[i] = risk_subportfolio
    ret_data_sub[i]= return_subportfolio
    w_sub.append(weights_subportfolio)
    sharpe_sub[i]=sharpe_subportfolio

'''




