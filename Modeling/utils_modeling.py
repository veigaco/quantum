import sys
sys.path.append('/home/ubuntu/quantum/Infrastructure')
from Schema import *
from mvo_utils import *
sys.path.append('home/ubuntu/quantum/Modeling')

def get_data(Table):
    '''Funcion que obtiene los datos de Tabla de la base de datos'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql = select([Table]).select_from(Table)
    result=s.execute(sql)
    df = pd.DataFrame(result.fetchall())
    df.columns = result.keys() 
    s.close()
    return df

def get_price_ts(tick):
    '''Funcion que obtiene los precios de tick(nombre del instrumento)'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql = select([Time_series]).where(Time_series.ticker==tick).select_from(Time_series)
    result=s.execute(sql)
    df_ = pd.DataFrame(result.fetchall()).drop_duplicates()
    df_.columns = result.keys() 
    df1=df_[["instrument_id","ticker","date_"]].drop_duplicates().set_index("date_")
    df2=df_[["date_","category","value"]].drop_duplicates()
    df2=df2.groupby(["date_","category"]).first().reset_index()
    df2=df2.set_index("date_")
    df2=df2.pivot(columns="category")["value"]
    df=pd.merge(df1, df2, left_index=True, right_index=True)
    df.sort_index(inplace=True)
    df=df[df.columns[2]]#["value"]
    df.columns=tick
    df2=pd.DataFrame(df)
    df2.columns=[tick]
    s.close()
    return df2



### transformations

def pct_change_annual(df):
    '''Compute percent change from year ago''' ## time series is trimestral
    df.sort_index()
    t=((df/ df.shift(4))-1)*100
    return t

def pct_change_m(df2,m):
    '''Compute percent change from m moths ago'''
    df2.sort_index()
    t=(df2.shift(m)/df2)-1  #m=-3
    t=t.dropna()
    return t

def diff_annual(df):
    '''Compute difference from year ago'''
    df.sort_index()
    t=(df - df.shift(4))
    return t

def pct_3avgyr(data2):
    df_final=pd.DataFrame(columns=["pct_avg3yr"],index=data2.index)
    aux=data2.groupby(data2.index.year).mean()
    for idx,row in data2.iterrows():
        #print(idx.year)
        actual_year=idx.year
        avg=aux[aux.index.isin([actual_year-1,actual_year-2,actual_year-3])].mean()[0] ## avg 3 year
        pct=row[0]/avg
        #print(pct)
        df_final.loc[idx]=pct
    df_final.columns=data2.columns
    return df_final


def z_score(df2):
    macro=df2.columns.tolist()[0]
    mean=df2.mean()  # Scale the data*
    std=df2.std()
    df3=pd.DataFrame(index=df2.index,
                    columns=df2.columns)
    df3[df2.columns.tolist()[0]]=(df2-mean)/std
    return df3

def pct_gdp(df):
    df1=get_price_ts("GDP")
    df2=get_ts_qs(df1)
    df_output=pd.concat([df,df2],axis=1)
    df_output[df.columns.tolist()[0]]=df_output[df.columns.tolist()[0]]/df_output["GDP"] #+"_GDP"
    df3=pd.DataFrame(df_output[df.columns.tolist()[0]])
    return df3#+"_GDP"
    
def get_ts_qs(test):
    test1=test.resample("QS").mean()#ffill()
    return test1

def get_ts_qs_transf(df2,transfor):#macro,
    '''Compute monthly time series with transformation from name macro'''
    macro=df2.columns.tolist()[0]
    if transfor=="Percen Change One Year Ago":
        df3=pct_change_annual(df2) 
    elif transfor=="Percen Change Three Months Ago":
        df3=pct_change_m(df2,-1)
    elif transfor=="Divided by 100 and difference One Year Ago":
        df3=diff_annual(df2/100)
    elif transfor=="Divided by 100":
        df3=df2/100
    elif transfor=="X/3Yr avg":
        df3=pct_3avgyr(df2)
    elif transfor=='As % of Nominal GDP':
        df3=pct_gdp(df2)
    else:
        df3=df2

    if macro!="SP500": #(macro!="GDP")&(
        df4=z_score(df3)
    else:
        df4=df3.copy()
    return df4








'''def get_ts_ms(df1):
    t=pd.DataFrame(df1.groupby(by=[df1.index.month,df1.index.year]).count().unstack().sum())
    df_final=pd.DataFrame()
    for idx,row in t.iterrows():
        n_rows_by_y=row[0] # number of rows by year ,4,12,365
        year=idx[1] ## year
        if n_rows_by_y>=12: ## mean by month
            aux=df1[df1.index.year==year]
            df2=aux.resample('MS', label='left').mean().fillna(method='ffill')
        else:# interpolate:
            df2=df1.resample('MS', label='left')
            df2=df2.interpolate(method='linear')
            df2=df2[df2.index.year==year]
        df_final=pd.concat([df_final,df2])
    df_final.sort_index()
    df_final=df_final.dropna()
    return df_final'''
##  estimacion by mont
