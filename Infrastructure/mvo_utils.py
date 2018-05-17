import pandas as pd
from pandas.io import sql
import urllib.request as urllib2
import io, pkgutil
from datetime import datetime,date,timedelta
#import datetime
import json
import time
from sqlalchemy.orm import sessionmaker,relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func,select,join,update,and_
import glob
from time import sleep
import os, shutil
from Schema import *
import fix_yahoo_finance as yf
import pandas_datareader.data as web

sleep_time = 5 ## waiting time for data request
Path_save="./temp_data/" ## directory where data is temporarily saved
anios = 10 # number of years that are used to download the etf
start_date = datetime.now() - timedelta(365*anios)
start_date = start_date.strftime('%Y-%m-%d')
limit=3 ## Limits of requests to the server for daily download. (number of attemps)

## conect to database
engine = create_engine('mysql+pymysql://quantum_user:Qu4ntum_u$3r@localhost/securities_master_database',pool_size=0, max_overflow=-1)


## add data
def add_up_Table(name_,Table):
    '''Function that adds or updates Table with name_'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Table).filter(Table.name==name_).first()
    if sql1 is None: # If "name_" did not sexist, it creates
        aux=Table(name=name_)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: #if name_ exists extract id
        id_=sql1.id
        s.close()  
    return id_


def add_up_data_vendors(name_,website_url_):
    '''Function that adds or updates Data_vendors with name_ and website_url_'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    #sql1=select([Data_vendors]).where(Data_vendors.name==name_) ## verify that there is the data vendor
    sql1 = s.query(Data_vendors).filter(Data_vendors.name==name_).first()
    if sql1 is None:
        aux=Data_vendors(name=name_,website_url=website_url_,created_date=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else:
        id_=sql1.id
        s.close()   
    return id_

def add_up_instrument(id_1,id_2,id_3,id_4,name1,type1,desc):
    '''Function that adds or updates Instrument with id_1, id_2, id_3, id_4, name1, type1, desc'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Instrument).filter(and_(Instrument.name==name1,Instrument.industry_id==id_1,Instrument.instrument_type_id==id_2,Instrument.sector_id==id_3,Instrument.data_vendor_id==id_4,Instrument.type==type1,Instrument.description==desc)).first()
    if sql1 is None: 
        aux=Instrument(industry_id=id_1,instrument_type_id=id_2,sector_id=id_3,data_vendor_id=id_4,name=name1,type=type1,description=desc)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: 
        id_=sql1.id
        s.close()
    return id_

def get_id_instrument(name1):
    '''Function that gets the ID of Instrument name1'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Instrument).filter(Instrument.name==name1).first()
    id_=sql1.id
    s.close()
    return id_

def add_asset(universe1,b_ac,type1,country1,index1,etf1,name1,etf_id):
    '''Function that adds or update asset_classes with universe1,b_ac,type1,country1,index1,etf1,name1 and etf_id'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Asset_classes).filter(and_(Asset_classes.name==name1,Asset_classes.universe==universe1)).first()
    if sql1 is None: 
        aux=Asset_classes(universe=universe1,broad_asset_class=b_ac,type=type1,country=country1,index=index1,etf=etf1,name=name1,etf_instrument_id=etf_id)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: 
        id_=sql1.id
        s.close()
    return id_

##################################


def get_instrument():
    '''Function that makes the query to the db to obtain the list of instruments that are going to be downloaded'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql = select([Instrument]).select_from(Instrument)
    result=s.execute(sql)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    s.close()
    return instrument


def update_data_vendor(name_,website_url_):
    '''Function that updates the table of data vendors'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql=update(Data_vendors).where(Data_vendors.name==name_).values({"last_updated_date":datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')})
    s.execute(sql)
    s.commit()
    s.close()


def update_instrument_db(df_instrument,row):
    '''function that updates the instrument table with data from datasources'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql=update(Instrument).where(Instrument.id==int(row["id"])).values({"refresh_at":datetime.strptime(df_instrument["refreshed_at"][0], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S'),
        "newest_available_date":datetime.strptime(df_instrument["newest_available_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
        "oldest_available_date":datetime.strptime(df_instrument["oldest_available_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
        "start_date":datetime.strptime(df_instrument["start_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
        "end_date":datetime.strptime(df_instrument["end_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
        "frequency":df_instrument["frequency"][0]})
    s.execute(sql)
    s.commit()
    s.close()



def get_data_quandl(row):
    #/Applications/Python\ 3.6/Install\ Certificates.command
    '''Function using the Quandl API data and metadata and download the data for each instrument
     for the two types of quandl data sources: datasets and datatables'''
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print("Getting pricing for:", row["name"])
    name_="Quandl"
    website_url_="https://www.quandl.com/"
    api_call_head1= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
    #api_call_head1="https://www.quandl.com/api/v3/datasets/WIKI/AAPL.json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
    data1 = urllib2.urlopen("%s" % (api_call_head1)).read()
    json_data = data1.decode('utf8')#.replace("'", '"')
    json_dataset= json.loads(json_data)["dataset"]
    df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","oldest_available_date","start_date","end_date","frequency"])
    df_instrument.loc[0] = [json_dataset["refreshed_at"],json_dataset["newest_available_date"],json_dataset["oldest_available_date"],json_dataset["start_date"],json_dataset["end_date"],json_dataset["frequency"]]
    df = pd.DataFrame(data=json_dataset["data"],columns=json_dataset["column_names"])
    df["ticker"]=row["name"]
    name_date=df.columns[0]   ## get column name from date
    df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
    df = df[df.value.notnull()]  ## filter NAN values from value column
    df.columns = ['ticker','date_', "category",'value'] ## rename columns
    df["instrument_id"]=row["id"]
    df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
    df.to_csv(Path_save+"df_"+row["name"].replace("/","")+".csv") ## save local
    #df_instrument.to_csv(Path_save+"df_instrument_"+row["database_name"]+"_"+row["name"]+".csv") ## save local
    update_data_vendor(name_,website_url_)
    update_instrument_db(df_instrument,row) ## update database (table instrument with dates, text,etc)
    #return df,df_instrument



def get_data_yahoo(row):
    '''Function using the Yahoo API price data for the specified instruments'''
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print("Getting pricing for:", row["name"])
    name_="Yahoo_Finance"
    website_url_="https://finance.yahoo.com/"
    df = yf.download([row["name"]], start=start_date)#['Adj Close']
    df=df.reset_index()
    df.sort_index(ascending=True, inplace=True)
    df["ticker"]=row["name"]
    name_date=df.columns[0]   ## get column name from date
    df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
    df = df[df.value.notnull()]  ## filter NAN values from value column
    df.columns = ['ticker','date_', "category",'value'] ## rename columns
    df["instrument_id"]=row["id"]
    df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
    df.to_csv(Path_save+row["name"]+".csv") ## save local
    df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","oldest_available_date","start_date","end_date","frequency"])
    df_instrument.loc[0] = [datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),max(df.date_).strftime('%Y-%m-%d'),min(df.date_).strftime('%Y-%m-%d'),min(df.date_).strftime('%Y-%m-%d'),max(df.date_).strftime('%Y-%m-%d'),"daily"]
    update_data_vendor(name_,website_url_)
    update_instrument_db(df_instrument,row) ## update database (table instrument with dates, text,etc)


# Exception safe downloader
def get_safe_data_quandl(row):
    '''Function that guarantees the download of the quandl font'''
    while True:
        try:
            get_data_quandl(row) ; break
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)


def get_safe_data_yahoo(row):
    '''Function that guarantees the download of the Yahoo font'''
    while True:
        try:
            get_data_yahoo(row); break
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)

def download_data(instrument):
    '''Function that downloads the historical prices of each instrument'''
    for index, row in instrument.iterrows():
        if row["data_vendor_id"]==add_up_data_vendors("Quandl","https://www.quandl.com/"):
            get_safe_data_quandl(row)
        else:
            get_safe_data_yahoo(row)



def write_db():
    '''Function that goes up to the db of the historical price data stored locally'''
    all_files=glob.glob(Path_save+"*.csv")
    for i in all_files:
        print(i)
        df=pd.read_csv(i)[["ticker","date_","category","value","instrument_id"]]
        df.to_sql(con=engine, name='time_series', if_exists='append',index=False) 

def delete_files_dir():
    '''Function that deletes all temporary files '''
    folder = "./temp_data"
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)

##################################



def update_instrument_db_daily(df_instrument,row):
    '''function that updates the instrument table with data from datasources'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql=update(Instrument).where(Instrument.id==int(row["id"])).values({"refresh_at":datetime.strptime(df_instrument["refreshed_at"][0], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S'),
        "newest_available_date":datetime.strptime(df_instrument["newest_available_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
        "end_date":datetime.strptime(df_instrument["end_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S')})
    s.execute(sql)
    s.commit()
    s.close()

def get_data_quandl_daily(row,last_date,today):
    '''Function using the Quandl API data and metadata and download daily data for each instrument
     for the two types of quandl data sources: datasets and datatables'''
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print("Getting pricing for:", row["name"])
    name_="Quandl"
    website_url_="https://www.quandl.com/"
    if row["type"]=="datasets": #&start_date=2014-01-01&end_date=2014-12-31
        api_call_head1= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?start_date="+str(last_date)+"&end_date="+str(today)+"&api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        data1 = urllib2.urlopen("%s" % (api_call_head1)).read()
        json_data = data1.decode('utf8')#.replace("'", '"')
        json_dataset= json.loads(json_data)["dataset"]
        df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","end_date"])
        df_instrument.loc[0] = [json_dataset["refreshed_at"],json_dataset["newest_available_date"],json_dataset["end_date"]]
        df = pd.DataFrame(data=json_dataset["data"],columns=json_dataset["column_names"])
        if len(df)>0:
            df["ticker"]=row["name"]
            name_date=df.columns[0]   ## get column name from date
            df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
            df = df[df.value.notnull()]  ## filter NAN values from value column
            df.columns = ['ticker','date_', "category",'value'] ## rename columns
            df["instrument_id"]=row["id"]
            df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
            df.to_csv(Path_save+"df_"+row["name"].replace("/","")+".csv") ## save local
            #df_instrument.to_csv(Path_save+"df_instrument_"+row["database_name"]+"_"+row["name"]+".csv") ## save local
            update_data_vendor(name_,website_url_)
            update_instrument_db_daily(df_instrument,row) ## update database (table instrument with dates, text,etc)
    else:
        start = last_date#datetime.strptime(str(last_date), "%Y-%m-%d")
        end = today#datetime.strptime(str(today), "%Y-%m-%d")
        date_generated = [start + timedelta(days=x) for x in range(0, (end-start).days)]
        dates=[]
        for date in date_generated:
            dates.append(date.strftime("%Y-%m-%d"))
        myString = ",".join(dates)
        api_call_data= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?date="+myString+"&api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        #api_call_data= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?start_date="+str(last_date)+"&end_date="+str(today)+"&api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        api_call_meta= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+"/metadata.json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        data1 = urllib2.urlopen("%s" % (api_call_data)).read()
        meta = urllib2.urlopen("%s" % (api_call_meta)).read()
        json_data = data1.decode('utf8').replace("'", '"')
        json_meta = meta.decode('utf8').replace("'", '"')
        json_dataset= json.loads(json_data)["datatable"]
        json_meta= json.loads(json_meta)
        columns=[json_dataset["columns"][i]["name"] for i in range(len(json_dataset["columns"]))]
        cols=[json_dataset["columns"][i]["name"] for i in range(len(json_dataset["columns"]))]
        if "date" in cols: 
            cols.remove("date")
        cols = ["date"]+cols
        df = pd.DataFrame(data=json_dataset["data"],columns=columns)
        df = df[cols]
        if len(df)>0:
            df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","end_date"])
            df_instrument.loc[0] = [json_meta["datatable"]["status"]["refreshed_at"],max(df.date),max(df.date)]
            name_date=df.columns[0]   ## get column name from date
            df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
            df = df[df.value.notnull()]  ## filter NAN values from value column
            df.columns = ['ticker','date_', 'category','value'] ## rename columns
            df["instrument_id"]=row["id"]
            df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
            df.to_csv(Path_save+"df_"+row["name"].replace("/","")+".csv") ## save local
            #df_instrument.to_csv(Path_save+"df_instrument_"+row["database_name"]+"_"+row["name"]+".csv") ##save local
            update_data_vendor(name_,website_url_)
            update_instrument_db_daily(df_instrument,row) ## update database (table instrument with dates, text,etc)
    
    #return df,df_instrument

def get_data_yahoo_daily(row,last_date,today):
    '''Function that downloads the daily prices of Yahoo'''
    print("Getting pricing for:", row["name"])
    name_="Yahoo_Finance"
    website_url_="https://finance.yahoo.com/"
    df = yf.download([row["name"]], start=last_date)#['Adj Close']
    df=df.reset_index()
    df.sort_index(ascending=True, inplace=True)
    df["ticker"]=row["name"]
    name_date=df.columns[0]   ## get column name from date
    df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
    df = df[df.value.notnull()]  ## filter NAN values from value column
    df.columns = ['ticker','date_', "category",'value'] ## rename columns
    df["instrument_id"]=row["id"]
    df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
    if len(df)>0:
        df.to_csv(Path_save+row["name"]+".csv") ## save local
        df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","end_date"])
        df_instrument.loc[0] = [datetime.fromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),max(df.date_).strftime('%Y-%m-%d'),max(df.date_).strftime('%Y-%m-%d')]
        update_data_vendor(name_,website_url_)
        update_instrument_db_daily(df_instrument,row) ## update database (table instrument with dates, text,etc)

# Exception safe downloader

def get_safe_data_quandl_daily(row,last_date,today):
    '''Function that guarantees the daily download of the quandl font'''
    count=0
    while True:
        try:
            count=count+1
            get_data_quandl_daily(row,last_date,today) ; break
            if count==limit:
                break;
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)
            if count==limit:
                break;


def get_safe_data_yahoo_daily(row,last_date,today):
    '''Function that guarantees the daily download of the Yahoo Finance source'''
    count=0
    while True:
        try:
            count=count+1
            get_data_yahoo_daily(row,last_date,today) ; break
            if count==limit:
                break;
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)
            if count==limit:
                break;

def download_data_daily(instrument):
    '''Function that downloads the daily prices of each instrument'''
    for index, row in instrument.iterrows():
        last_date=(row["end_date"]+timedelta(days=1)).date()
        print(index)
        today=(datetime.fromtimestamp(time.time())+ timedelta(days=1)).date()
        if row["data_vendor_id"]==add_up_data_vendors("Quandl","https://www.quandl.com/"):
            if last_date is pd.NaT: 
                get_safe_data_quandl(row)
            else:
                get_safe_data_quandl_daily(row,last_date,today)
        else:
            if last_date is pd.NaT:
                get_safe_data_yahoo(row)
            else:
                get_safe_data_yahoo_daily(row,last_date,today)


