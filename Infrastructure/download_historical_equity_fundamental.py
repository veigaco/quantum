import pandas as pd
from datetime import datetime,date,timedelta
import time
from sqlalchemy.orm import sessionmaker#,relationship, backref
from sqlalchemy import create_engine,select
from Schema import *
from mvo_utils import *
import glob
Path_stats="./temp_data_stats/"

def get_data_sf(data_request):
    #count=0
    while True:
        try:
            #count=count+1
            summary = data_request ; break
            #if count==100:
            #    break;
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)
            #if count==100:
            #    break;
    return summary


def get_summary(name):
    try:
        summary=get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'?p='+name))
        summaryf=summary[1]
        summaryf.columns=["Metric","Value"]
        summaryf["Category"]="Summary";summaryf["Table"]="Summary"
        summaryf["Period"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        summaryf["Period"]=summaryf["Period"].astype(str)
        summaryf["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        summaryf["instrument"]=name
        summaryf["data_vendor"]="Yahoo_Finance"
        summaryf.to_csv(Path_stats+"summary_"+name+".csv")
    except:
        print("No se existe summary para "+name)
        pass
    #return summaryf

def get_stat(name):
    try:
        statistics = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/key-statistics?p='+name))
        statisticsf=pd.DataFrame()
        for i in range(len(statistics)):
            table_aux=statistics[i]
            if i==0:
                table_aux["Table"]="Valuation Measures"
            elif (6>i>0):
                table_aux["Table"]="Financial Highlights"
            else:
                table_aux["Table"]="Trading Information"
            statisticsf=pd.concat([statisticsf,table_aux], axis=0)
        statisticsf.columns=["Metric","Value","Table"]
        statisticsf["Category"]="Statistics"
        statisticsf["Period"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        statisticsf["Period"]=statisticsf["Period"].astype(str)
        statisticsf["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        statisticsf["instrument"]=name
        statisticsf["data_vendor"]="Yahoo_Finance"
        statisticsf.to_csv(Path_stats+"statistics_"+name+".csv")
    except:
        print("No se existe statistics para "+name)
        pass
    #return statisticsf

def get_fin(name):
    try:
        financial = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/financials?p='+name,header =0))
        financial2 = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/balance-sheet?p='+name,header =0))
        financial3 = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/cash-flow?p='+name,header =0))
        financial=financial[0].dropna(thresh=3)
        financial=financial.melt(id_vars=[financial.columns[0]])
        financial.columns=["Metric","Period","Value"]
        financial["Category"]="Financial"
        financial["Table"]="Income Statement"
        financial["Period"]=pd.to_datetime(financial['Period'], format='%m/%d/%Y')
        financial["Period"]=financial["Period"].astype(str)
        financial["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        financial2=financial2[0].dropna(thresh=3)
        financial2=financial2.melt(id_vars=[financial2.columns[0]])
        financial2.columns=["Metric","Period","Value"]
        financial2["Category"]="Financial"
        financial2["Table"]="Balance Sheet"
        financial2["Period"]=pd.to_datetime(financial2['Period'], format='%m/%d/%Y')
        financial2["Period"]=financial2["Period"].astype(str)
        financial2["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        financial3=financial3[0].dropna(thresh=3)
        financial3=financial3.melt(id_vars=[financial3.columns[0]])
        financial3.columns=["Metric","Period","Value"]
        financial3["Category"]="Financial"
        financial3["Table"]="Cash Flow"
        financial3["Period"]=pd.to_datetime(financial3['Period'], format='%m/%d/%Y')
        financial3["Period"]=financial3["Period"].astype(str)
        financial3["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        fin=pd.concat([financial,financial2,financial3])
        fin["instrument"]=name
        fin["data_vendor"]="Yahoo_Finance"
        fin.to_csv(Path_stats+"financial_"+name+".csv")
    except:
        print("No se existe financials para "+name)
        pass
    #return pd.concat([financial,financial2,financial3])
#get_fin("SBUX").head(3)

def get_analysis(name):
    try:
        analysis = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/analysis?p='+name))
        analysisf=pd.DataFrame()
        for i in range(len(analysis)-1):
            analysis1=analysis[i].melt(id_vars=[analysis[i].columns[0]])
            analysis1["Table"]=analysis1.columns[0]
            analysis1.columns=["Metric","Period","Value","Table"]
            analysis1["Category"]="Analysis"
            analysis1["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            analysisf=pd.concat([analysisf,analysis1])
        analysis1=analysis[5]#.melt(id_vars=[analysis[i].columns[0]])
        analysis1=analysis1.melt(id_vars=analysis1.columns[0])
        analysis1["Table"]=analysis1.columns[0]
        analysis1.columns=["Period","Metric","Value","Table"]
        analysis1["Category"]="Analysis"
        analysis1["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        analysisf=pd.concat([analysisf,analysis1])
        analysisf["instrument"]=name
        analysisf["data_vendor"]="Yahoo_Finance"
        analysisf.to_csv(Path_stats+"analysis_"+name+".csv")
    except:
        print("No se existe analysis para "+name)
        pass
    #return analysisf


def get_holder_ef(name):
    try:
        holders = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/holders?p='+name))
        mholders=holders[0]
        mholders["Category"]="Holders"
        mholders.columns=["Value","Metric","Category"]
        mholders["Table"]="holders(breakdown)"
        mholders["Period"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d')
        mholders["Period"]=mholders["Period"].astype(str)
        mholders["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        mholders["instrument"]=name
        mholders["data_vendor"]="Yahoo_Finance"
        mholders.to_csv(Path_stats+"holder_ef_"+name+".csv")
    except:
        print("No se existe holder_ef para "+name)
        pass
    #return mholders
def get_management(name):
    try:
        holders = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/holders?p='+name))
        managemenet=holders[1]
        managemenet["data_vendor"]="Yahoo_Finance"
        managemenet["instrument"]=name
        managemenet.to_csv(Path_stats+"management_"+name+".csv")
    except:
        print("No se existe management para "+name)
        pass
        
def get_holder(name):
    try:
        holders = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/holders?p='+name))
        holders=pd.concat([holders[2],holders[3]])
        holders["data_vendor"]="Yahoo_Finance"
        holders["instrument"]=name
        holders=holders[holders.Value.notnull()]
        holders.index = range(1,len(holders)+1)
        holders.to_csv(Path_stats+"holders_"+name+".csv")
    except:
        print("No se existe holders para "+name)
        pass

def get_profile(name):
    try:
        profile = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name+'/profile?p='+name))
        profile=profile[0]
        profile=profile.fillna(0)
        profile["data_vendor"]="Yahoo_Finance"
        profile["instrument"]=name
        profile.to_csv(Path_stats+"profile_"+name+".csv")
    except:
        print("No se existe profile para "+name)
        pass
    
def get_id_instrument(name):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    sql1 = s.query(Instrument).filter(Instrument.name.endswith(name)).first()
    id_=sql1.id
    s.close()
    return id_

def get_id_data_vendor(name):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    sql1 = s.query(Data_vendors).filter(Data_vendors.name.endswith(name)).first()
    id_=sql1.id
    s.close()
    return id_

def add_up_equityf(cat,tab):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    sql1 = s.query(Equity_fundamentals).filter(and_(Equity_fundamentals.Category==cat,Equity_fundamentals.Table==tab)).first()
    if sql1 is None: 
        aux=Equity_fundamentals(Category=cat,Table=tab)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else:
        id_=sql1.id
        s.close()  
    return id_

def get_equities_fundamental(name):
    get_summary(name)
    #print("summary descargada para "+name)
    get_stat(name)
    #print("stats descargada para "+name)
    get_fin(name)
    #print("financials descargada para "+name)
    get_analysis(name)
    #print("analysis descargada para "+name)
    get_holder_ef(name)
    get_management(name)
    get_holder(name)
    get_profile(name)
    print("equities_fundamentales descargadas para "+name)
    sleep(10)

def upload_ef_metrics():
    summary_files=glob.glob(Path_stats+"summary_*.csv")
    stats_files=glob.glob(Path_stats+"statistics_*.csv")
    fin_files=glob.glob(Path_stats+"financial_*.csv")
    analysis_files=glob.glob(Path_stats+"analysis_*.csv")
    holder_ef_files=glob.glob(Path_stats+"holder_ef_*.csv")
    summary_=pd.DataFrame()
    statistics_=pd.DataFrame()
    financials_=pd.DataFrame()
    analysis_=pd.DataFrame()
    holder_ef_=pd.DataFrame()
    for i in summary_files:
        summary_=pd.concat([summary_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    for i in stats_files:
        statistics_=pd.concat([statistics_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    for i in fin_files:
        financials_=pd.concat([financials_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    for i in analysis_files:
        analysis_=pd.concat([analysis_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    for i in holder_ef_files:
        holder_ef_=pd.concat([holder_ef_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    equities_fundamental=pd.concat([summary_,
                                statistics_,
                                financials_,
                                analysis_,
                                holder_ef_])
    equities_fundamental.head()
    equities_fundamental.index = range(1,len(equities_fundamental)+1)
    ## equities fund
    equities_fundamental1=equities_fundamental[["Category","Table"]].drop_duplicates()
    #equities_fundamental1.apply(add_db,axis=1)
    equities_fundamental1.to_sql(con=engine, name='equity_fundamental', if_exists='append',index=False)
    print("equities_fundamentales agregadada")
    #metrics
    metrics=equities_fundamental[["instrument","Category","Table","Period","Metric","Value","refreshed_at","data_vendor"]]
    metrics=metrics[metrics.Value.notnull()]
    metrics["instrument_id"]=metrics["instrument"].apply(get_id_instrument)
    metrics["data_vendor_id"]=metrics["data_vendor"].apply(get_id_data_vendor)
    metrics["equity_fundamental_id"]=metrics[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)
    #metrics.apply(add_metric,axis=1)
    metrics=metrics[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]]
    metrics.to_sql(con=engine, name='metrics', if_exists='append',index=False)
    print("metrics agregadada")
    
def upload_management():
    man_files=glob.glob(Path_stats+"management_*.csv")
    management_=pd.DataFrame()
    for i in man_files:
        management_=pd.concat([management_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    management_.index = range(1,len(management_)+1)
    management_=management_.drop_duplicates()
    management_["Shares"]=management_["Shares"].astype(str)
    management_["date_reported"]=management_["Date Reported"].apply(lambda x:datetime.strptime(x,'%b\t%d,\t%Y').strftime('%Y-%m-%d %H:%M:%S'))
    management_["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    management_["instrument_id"]=management_["instrument"].apply(get_id_instrument)
    management_["data_vendor_id"]=management_["data_vendor"].apply(get_id_data_vendor)
    management_=management_[["instrument_id","data_vendor_id","Name","Shares","date_reported","refreshed_at"]]
    management_.to_sql(con=engine, name='management', if_exists='append',index=False)
    print("management agregadada para")

def upload_holders():
    holders_files=glob.glob(Path_stats+"holders_*.csv")
    holders_=pd.DataFrame()
    for i in holders_files:
        holders_=pd.concat([holders_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    holders_.index = range(1,len(holders_)+1)
    holders_=holders_.drop_duplicates()
    holders_["Shares"]=holders_["Shares"].astype(str)
    holders_["Value"]=holders_["Value"].astype(str)
    holders_["date_reported"]=holders_["Date Reported"].apply(lambda x: datetime.strptime(x,'%b\t%d,\t%Y').strftime('%Y-%m-%d %H:%M:%S'))
    holders_["percent_out"]=holders_['% Out']
    holders_["instrument_id"]=holders_["instrument"].apply(get_id_instrument)
    holders_["data_vendor_id"]=holders_["data_vendor"].apply(get_id_data_vendor)
    holders_["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    holders_=holders_[["instrument_id","data_vendor_id","Holder","Shares","date_reported","percent_out","Value","refreshed_at"]]
    holders_.to_sql(con=engine, name='holders', if_exists='append',index=False)
    print("holders agregadada")
    
def upload_profile():
    profile_files=glob.glob(Path_stats+"profile_*.csv")
    profile_=pd.DataFrame()
    for i in profile_files:
        profile_=pd.concat([profile_,pd.read_csv(i,index_col=[0],encoding='latin1')])
    profile_.index = range(1,len(profile_)+1)
    profile_=profile_.drop_duplicates()
    profile_["instrument_id"]=profile_["instrument"].apply(get_id_instrument)
    profile_["data_vendor_id"]=profile_["data_vendor"].apply(get_id_data_vendor)
    profile_["year_born"]=profile_["Year Born"]
    profile_["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    profile_=profile_[["instrument_id","Name","Title","Pay","Exercised","year_born","data_vendor_id","refreshed_at"]]
    profile_.to_sql(con=engine, name='profile', if_exists='append',index=False)
    print("profile agregadada")


instrument=get_instrument()
instrument=instrument[instrument.instrument_type_id==5]#.head(20)
instrument["name"]=instrument["name"].replace('WIKI/' ,'', inplace=False, regex=True)
#instrument=instrument["name"]
print("Instrument downloaded")
instrument["name"].apply(lambda x:get_equities_fundamental(x)) ## obtiene los archivos de stats para cada equity
print("equities fundamental downloaded")
print("upload db")
upload_ef_metrics()
upload_management()
upload_holders()
upload_profile()

