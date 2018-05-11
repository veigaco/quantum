import pandas as pd
from datetime import datetime,date,timedelta
import time
from sqlalchemy.orm import sessionmaker#,relationship, backref
from sqlalchemy import create_engine,select
from Schema import *
from mvo_utils import *
from numpy import nan
import glob
Path_stats="./temp_data_stats/"

def get_data_sf(data_request):
    count=0
    while True:
        try:
            count=count+1
            summary = data_request ; break
            if count==limit:
                break;
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)
            if count==limit:
                break;
    return summary


def get_summary(name):
    try:
        name1=name.replace('WIKI/' ,'')
        summaryf=get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'?p='+name1))[1]
        summaryf.columns=["Metric","Value"]
        summaryf["Category"]="Summary";summaryf["Table"]="Summary"
        summaryf["Period"]=str(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
        summaryf["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        summaryf["instrument_id"]=get_id_instrument(name)
        summaryf["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        summaryf["equity_fundamental_id"]=summaryf[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)
        summaryf[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]].to_csv(Path_stats+"summary_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe summary para "+name)
        pass
    

def get_stat(name):
    try:
        name1=name.replace('WIKI/' ,'')
        statistics = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/key-statistics?p='+name1))
        statisticsf=pd.DataFrame()
        for i in range(len(statistics)):
            table_aux=statistics[i]
            if i==0:
                table_aux["Table"]="Valuation Measures"
            elif (6>=i>0):
                table_aux["Table"]="Financial Highlights"
            else:
                table_aux["Table"]="Trading Information"
            statisticsf=pd.concat([statisticsf,table_aux], axis=0)
        statisticsf.columns=["Metric","Value","Table"]
        statisticsf["Category"]="Statistics"
        statisticsf["Period"]=str(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
        statisticsf["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        statisticsf["instrument_id"]=get_id_instrument(name)
        statisticsf["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        statisticsf["equity_fundamental_id"]=statisticsf[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)
        statisticsf[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]].to_csv(Path_stats+"statistics_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe statistics para "+name)
        pass
    #return statisticsf

def get_fin(name):
    try:
        name1=name.replace('WIKI/' ,'')
        financial1 = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/financials?p='+name1,header =0))[0].dropna(thresh=3)
        financial1=financial1.melt(id_vars=[financial1.columns[0]])
        financial1.columns=["Metric","Period","Value"]
        financial1["Table"]="Income Statement"
        financial2 = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/balance-sheet?p='+name1,header =0))[0].dropna(thresh=3)
        financial2=financial2.melt(id_vars=[financial2.columns[0]])
        financial2.columns=["Metric","Period","Value"]
        financial2["Table"]="Balance Sheet"
        financial3 = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/cash-flow?p='+name1,header =0))[0].dropna(thresh=3)
        financial3=financial3.melt(id_vars=[financial3.columns[0]])
        financial3.columns=["Metric","Period","Value"]
        financial3["Table"]="Cash Flow"
        financial=pd.concat([financial1,financial2,financial3])
        financial["Category"]="Financial"
        financial["Period"]=pd.to_datetime(financial['Period'], format='%m/%d/%Y')
        financial["Period"]=financial["Period"].astype(str)
        financial["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        #fin["instrument"]=name
        #fin["data_vendor"]="Yahoo_Finance"
        financial["instrument_id"]=get_id_instrument(name)
        financial["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        financial["equity_fundamental_id"]=financial[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)
        financial[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]].to_csv(Path_stats+"financial_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe financials para "+name)
        pass
    #return pd.concat([financial,financial2,financial3])
#get_fin("SBUX").head(3)

def get_analysis(name):
    try:
        name1=name.replace('WIKI/' ,'')
        analysis = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/analysis?p='+name1))
        analysisf=pd.DataFrame()
        for i in range(len(analysis)):
            analysis1=analysis[i].melt(id_vars=[analysis[i].columns[0]])
            analysis1["Table"]=analysis1.columns[0]
            if analysis1.columns[0]=="Growth Estimates":
                analysis1.columns=["Period","Metric","Value","Table"]
            else:
                analysis1.columns=["Metric","Period","Value","Table"]
            analysis1["Category"]="Analysis"
            analysis1["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            analysisf=pd.concat([analysisf,analysis1])
        #analysis1=analysis[5]#.melt(id_vars=[analysis[i].columns[0]])
        #analysis1=analysis1.melt(id_vars=analysis1.columns[0])
        #analysis1["Table"]=analysis1.columns[0]
        #analysis1.columns=["Period","Metric","Value","Table"]
        #analysis1["Category"]="Analysis"
        #analysis1["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        #analysisf=pd.concat([analysisf,analysis1])
        analysisf["instrument_id"]=get_id_instrument(name)
        analysisf["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        analysisf["equity_fundamental_id"]=analysisf[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)
        #analysisf["instrument"]=name
        #analysisf["data_vendor"]="Yahoo_Finance"
        analysisf[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]].to_csv(Path_stats+"analysis_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe analysis para "+name)
        pass
    #return analysisf


def get_holder_ef(name):
    try:
        name1=name.replace('WIKI/' ,'')
        mholders = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/holders?p='+name1))[0]
        #mholders=holders[0]
        mholders["Category"]="Holders"
        mholders.columns=["Value","Metric","Category"]
        mholders["Table"]="holders(breakdown)"
        mholders["Period"]=str(datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d'))
        mholders["refreshed_at"]=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        mholders["instrument_id"]=get_id_instrument(name)
        mholders["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        mholders["equity_fundamental_id"]=mholders[["Category","Table"]].apply(lambda x :add_up_equityf(x["Category"],x["Table"]), axis=1)  
        mholders[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]].to_csv(Path_stats+"holder_ef_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe holder_ef para "+name)
        pass
    #return mholders

def get_management(name):
    try:
        name1=name.replace('WIKI/' ,'')
        tot = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/holders?p='+name1))
        if len(tot)==4:
            management_=tot[1]
            #managemenet["data_vendor"]="Yahoo_Finance"
            #managemenet["instrument"]=name
            management_["Shares"]=management_["Shares"].astype(str)
            management_["date_reported"]=management_["Date Reported"].apply(lambda x:datetime.strptime(x,'%b\t%d,\t%Y').strftime('%Y-%m-%d %H:%M:%S'))
            management_["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            management_["instrument_id"]=get_id_instrument(name)
            management_["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
            management_=management_[["instrument_id","data_vendor_id","Name","Shares","date_reported","refreshed_at"]]
            management_.columns=["instrument_id","data_vendor_id","name","shares","date_reported","refreshed_at"]
            management_.to_csv(Path_stats+"management_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe management para "+name)
        pass


def get_holder(name):
    try:
        name1=name.replace('WIKI/' ,'')
        holders = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/holders?p='+name1))
        if len(holders)==3:
            holders=pd.concat([holders[1],holders[2]])
        else:
            holders=pd.concat([holders[2],holders[3]])
        holders=holders[holders.Value.notnull()]
        holders["Shares"]=holders["Shares"].astype(str)
        holders["Value"]=holders["Value"].astype(str)
        holders["date_reported"]=holders["Date Reported"].apply(lambda x: datetime.strptime(x,'%b\t%d,\t%Y').strftime('%Y-%m-%d %H:%M:%S'))
        holders["percent_out"]=holders['% Out']
        holders["instrument_id"]=get_id_instrument(name)
        holders["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        holders["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        holders=holders[["instrument_id","data_vendor_id","Holder","Shares","date_reported","percent_out","Value","refreshed_at"]]
        holders.to_csv(Path_stats+"holders_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe holders para "+name)
        pass

def get_profile(name):
    try:
        name1=name.replace('WIKI/' ,'')
        profile = get_data_sf(pd.read_html('https://finance.yahoo.com/quote/'+name1+'/profile?p='+name1))
        profile=profile[0]
        #profile=profile.fillna(0)
        profile=profile.fillna(value=nan, inplace=True)
        profile["instrument_id"]=get_id_instrument(name)
        profile["data_vendor_id"]=get_id_data_vendor("Yahoo_Finance")
        profile["year_born"]=profile["Year Born"]
        profile["refreshed_at"]=refreshed_at=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        profile=profile[["instrument_id","Name","Title","Pay","Exercised","year_born","data_vendor_id","refreshed_at"]]
        profile.to_csv(Path_stats+"profile_"+name1+".csv")
    except Exception as err:
        print("Error: {0}".format(err))
        print("No se existe profile para "+name)
        pass

def get_id_instrument(name):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    sql1 = s.query(Instrument).filter(Instrument.name==name).first()
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

def get_data_from_files(concept):
    files=glob.glob(Path_stats+str(concept)+"_*.csv")
    df=pd.DataFrame()
    cont=0
    for i in files:
        cont+=1
        aux=pd.read_csv(i,index_col=[0],encoding='latin1')
        print(str(cont)+" con " + str(aux.shape))
        df=pd.concat([df,aux])
    df.index = range(1,len(df)+1)
    return df

def get_metrics(id):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    subq = s.query(
        Metrics.id,
        Metrics.instrument_id,
        func.max(Metrics.refreshed_at).label('refreshed_at')
    ).filter(Metrics.instrument_id==id).group_by(Metrics.id).subquery('t2')
    query = s.query(Metrics).join(
        subq,
        and_(
            Metrics.id == subq.c.id,
            Metrics.instrument_id == subq.c.instrument_id,
            Metrics.refreshed_at == subq.c.refreshed_at
        )
    )
    result=s.execute(query)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    instrument.columns=[instrument.columns.tolist()[i].replace("metrics_","") for i in range(len(instrument.columns.tolist()))]
    s.close()
    return instrument

def get_manag(id):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    subq = s.query(
        Management.id,
        Management.instrument_id,
        func.max(Management.refreshed_at).label('refreshed_at')
    ).filter(Management.instrument_id==id).group_by(Management.id).subquery('t2')
    query = s.query(Management).join(
        subq,
        and_(
            Management.id == subq.c.id,
            Management.instrument_id == subq.c.instrument_id,
            Management.refreshed_at == subq.c.refreshed_at
        )
    )
    result=s.execute(query)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    instrument.columns=[instrument.columns.tolist()[i].replace("management_","") for i in range(len(instrument.columns.tolist()))]
    s.close()
    return instrument


def get_holddb(id):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    subq = s.query(
        Holders.id,
        Holders.instrument_id,
        func.max(Holders.refreshed_at).label('refreshed_at')
    ).filter(Holders.instrument_id==id).group_by(Holders.id).subquery('t2')
    query = s.query(Holders).join(
        subq,
        and_(
            Holders.id == subq.c.id,
            Holders.instrument_id == subq.c.instrument_id,
            Holders.refreshed_at == subq.c.refreshed_at
        )
    )
    result=s.execute(query)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    instrument.columns=[instrument.columns.tolist()[i].replace("holders_","") for i in range(len(instrument.columns.tolist()))]
    s.close()
    return instrument


def get_profiledb(id):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()  
    subq = s.query(
        Profile.id,
        Profile.instrument_id,
        func.max(Profile.refreshed_at).label('refreshed_at')
    ).filter(Profile.instrument_id==id).group_by(Profile.id).subquery('t2')
    query = s.query(Profile).join(
        subq,
        and_(
            Profile.id == subq.c.id,
            Profile.instrument_id == subq.c.instrument_id,
            Profile.refreshed_at == subq.c.refreshed_at
        )
    )
    result=s.execute(query)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    instrument.columns=[instrument.columns.tolist()[i].replace("profile_","") for i in range(len(instrument.columns.tolist()))]
    s.close()
    return instrument

from sqlalchemy import func, and_




def upload_ef_metrics():
    summary_=get_data_from_files("summary")
    statistics_=get_data_from_files("statistics")
    financials_=get_data_from_files("financial")
    analysis_=get_data_from_files("analysis")
    holder_ef_=get_data_from_files("holder_ef")
    equities_fundamental=pd.concat([summary_,statistics_,financials_,analysis_,holder_ef_])
    equities_fundamental.index = range(1,len(equities_fundamental)+1)
    metrics=equities_fundamental[["instrument_id","equity_fundamental_id","data_vendor_id","refreshed_at","Period","Metric","Value"]]
    inst=metrics.instrument_id.unique()
    for i in inst:
        aux=metrics[metrics.instrument_id==i].drop_duplicates()#[["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric","Value"]]
        aux.fillna(value=nan, inplace=True)
        aux=aux[aux.Value.notnull()]
        try:
            db=get_metrics(int(i))
            db.fillna(value=nan, inplace=True)
            db=db[db.Value.notnull()]
            db=db[["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric","Value"]].drop_duplicates()
            result=pd.merge(aux,db,on=["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric"],how="outer")
            result["flag"]=result["Value_x"]==result["Value_y"]
            result1=result[result.flag==False]
            if len(result1)>0:
                result1=result1[["instrument_id","equity_fundamental_id","Period","data_vendor_id","refreshed_at","Metric","Value_x"]]
                result1.columns=["instrument_id","equity_fundamental_id","Period","data_vendor_id","refreshed_at","Metric","Value"]
                result1.to_sql(con=engine, name='metrics', if_exists='append',index=False)
                print("datos nuevos escritos ")
            else:
                print("no hay datos nuevos")
        except Exception as err:
            print("Error: {0}".format(err))
            aux.to_sql(con=engine, name='metrics', if_exists='append',index=False)
            print("agregado a la bd")
    #metrics.to_sql(con=engine, name='metrics', if_exists='append',index=False)
    print("metrics agregadada")
    #return metrics


df = pd.DataFrame(columns=["id","name", "val"], data=[[1,"hola",1]])
df2 = pd.DataFrame(columns=["id","name", "val"], data=[[1,"hola1",2]])

def upload_management():
    management_=get_data_from_files("management")
    management_.index = range(1,len(management_)+1)
    inst=management_.instrument_id.unique()
    for i in inst:
        aux=management_[management_.instrument_id==i].drop_duplicates()#[["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric","Value"]]
        aux.fillna(value=nan, inplace=True)
        aux.columns=['instrument_id', 'data_vendor_id', 'name', 'shares', 'date_reported','refreshed_at']
        aux["shares"]=aux["shares"].astype(str)
        aux["date_reported"]=pd.to_datetime(aux["date_reported"])
        try:
            db=get_manag(int(i))
            db.fillna(value=nan, inplace=True)
            #db=db[db.Value.notnull()]
            db=db[["instrument_id","data_vendor_id","name","shares","date_reported"]].drop_duplicates()
            result=pd.merge(aux,db,on=["instrument_id","data_vendor_id","name"],how="outer")
            result["flag1"]=result["shares_x"]==result["shares_y"]
            result["flag2"]=result["date_reported_x"]==result["date_reported_y"]
            result1=result[(result.flag1==False)|(result.flag2==False)]
            if len(result1)>0:
                result1=result1[["instrument_id","data_vendor_id","name","refreshed_at","date_reported_x","shares_x"]]
                result1.columns=["instrument_id","data_vendor_id","name","refreshed_at","date_reported","shares"]
                result1.to_sql(con=engine, name='management', if_exists='append',index=False)
                print("datos nuevos escritos ")
            else:
                print("no hay datos nuevos")
        except Exception as err:
            print("Error: {0}".format(err))
            aux.to_sql(con=engine, name='management', if_exists='append',index=False)
            print("agregado a la bd")
    #management_.to_sql(con=engine, name='management', if_exists='append',index=False)
    print("management agregadada para")

def upload_holders():
    holders_=get_data_from_files("holders")
    holders_.index = range(1,len(holders_)+1)
    inst=holders_.instrument_id.unique()
    for i in inst:
        aux=holders_[holders_.instrument_id==i].drop_duplicates()#[["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric","Value"]]
        aux.fillna(value=nan, inplace=True)
        aux.columns=['instrument_id', 'data_vendor_id', 'holder', 'shares', 'date_reported','percent_out', 'value', 'refreshed_at']
        aux["shares"]=aux["shares"].astype(str)
        aux["date_reported"]=pd.to_datetime(aux["date_reported"])
        aux["percent_out"]=aux["percent_out"].astype(str)
        aux["value"]=aux["value"].astype(str)
        try:
            db=get_holddb(int(i))
            db.fillna(value=nan, inplace=True)
            #db=db[db.Value.notnull()]
            db=db[["instrument_id","data_vendor_id","holder","shares","date_reported","percent_out","value"]].drop_duplicates()
            result=pd.merge(aux,db,on=["instrument_id","data_vendor_id","holder"],how="outer")
            result["flag1"]=result["shares_x"]==result["shares_y"]
            result["flag2"]=result["date_reported_x"]==result["date_reported_y"]
            result["flag3"]=result["percent_out_x"]==result["percent_out_y"]
            result["flag4"]=result["value_x"]==result["value_y"]
            result1=result[(result.flag1==False)|(result.flag2==False)|(result.flag3==False)|(result.flag4==False)]
            if len(result1)>0:
                result1=result1[["instrument_id","data_vendor_id","holder","shares_x","date_reported_x","percent_out_x","value_x","refreshed_at"]]
                result1.columns=["instrument_id","data_vendor_id","holder","shares","date_reported","percent_out","value","refreshed_at"]
                result1.to_sql(con=engine, name='holders', if_exists='append',index=False)
                print("datos nuevos escritos ")
            else:
                print("no hay datos nuevos")
        except Exception as err:
            print("Error: {0}".format(err))
            aux.to_sql(con=engine, name='holders', if_exists='append',index=False)
            print("agregado a la bd")
    #holders_.to_sql(con=engine, name='holders', if_exists='append',index=False)
    print("holders agregadada")


def upload_profile():
    profile_=get_data_from_files("profile")
    profile_.index = range(1,len(profile_)+1)
    inst=profile_.instrument_id.unique()
    for i in inst:
        aux=profile_[profile_.instrument_id==i].drop_duplicates()#[["instrument_id","equity_fundamental_id","Period","data_vendor_id","Metric","Value"]]
        #aux.fillna(value=nan, inplace=True)
        aux.columns=['instrument_id', 'name', 'title', 'pay', 'exercised', 'year_born','data_vendor_id', 'refreshed_at']        #aux["shares"]=aux["shares"].astype(str)
        aux["title"]=aux["title"].astype(str)
        aux["pay"]=aux["pay"].astype(str)
        aux["exercised"]=aux["exercised"].astype(str)
        try:
            db=get_profiledb(int(i))
            db.fillna(value=nan, inplace=True)
            #db=db[db.Value.notnull()]
            db=db[['instrument_id', 'data_vendor_id', 'name', 'title', 'pay','exercised', 'year_born']].drop_duplicates()
            db["exercised"]=db["exercised"].astype(str)
            result=pd.merge(aux,db,on=["instrument_id","data_vendor_id","name"],how="outer")
            result["flag1"]=result["title_x"]==result["title_y"]
            result["flag2"]=result["pay_x"]==result["pay_y"]
            result["flag3"]=result["exercised_x"]==result["exercised_y"]
            result["flag4"]=result["year_born_x"]==result["year_born_y"]
            result1=result[(result.flag1==False)|(result.flag2==False)|(result.flag3==False)|(result.flag4==False)]
            if len(result1)>0:
                result1=result1[['instrument_id', 'name', 'title_x', 'pay_x', 'exercised_x', 'year_born_x','data_vendor_id', 'refreshed_at']]
                result1.columns=['instrument_id', 'name', 'title', 'pay', 'exercised', 'year_born','data_vendor_id', 'refreshed_at']
                result1.to_sql(con=engine, name='profile', if_exists='append',index=False)
                print("datos nuevos escritos ")
            else:
                print("no hay datos nuevos")
        except Exception as err:
            print("Error: {0}".format(err))
            aux.to_sql(con=engine, name='profile', if_exists='append',index=False)
            print("agregado a la bd")
    #profile_.to_sql(con=engine, name='profile', if_exists='append',index=False)
    print("profile agregadada")



instrument=get_instrument()
instrument=instrument[instrument.instrument_type_id==5]#.head(4)
#instrument=instrument["name"]
print("Instrument downloaded")
instrument["name"].apply(lambda x:get_equities_fundamental(x)) ## obtiene los archivos de stats para cada equity
print("equities fundamental downloaded")
print("upload db")
upload_ef_metrics()
upload_management()
upload_holders()
upload_profile()


