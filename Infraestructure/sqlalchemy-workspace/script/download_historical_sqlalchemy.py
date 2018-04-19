#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
import pandas as pd
from pandas.io import sql
import urllib.request as urllib2
import io, pkgutil
from datetime import datetime,date,timedelta
import json
import time
from sqlalchemy.orm import sessionmaker,relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func,select,join,update
import glob
from time import sleep
import os, shutil
from Schema import *
import fix_yahoo_finance as yf
import pandas_datareader.data as web


sleep_time = 5
Path_save="./temp_data/" ## directorio donde se guardan temporalmente los datos
anios = 10 # numero de años que se usan para descargar los etf
start_date = datetime.now() - timedelta(365*anios)
start_date = start_date.strftime('%Y-%m-%d')

# Connect to the database
engine = create_engine('mysql+pymysql://quantum_user:Qu4ntum_u$3r@localhost/securities_master_database')


def get_instrument():
    '''Funcion que hace el query a la db para obtener el listado de instruments que se van a descargar'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    #j = join(Instrument, Instrument_type,
    #     Instrument.instrument_type_id == Instrument_type.id)
    #sql = select([Instrument,Instrument_type]).select_from(j)
    sql = select([Instrument]).select_from(Instrument)
    result=s.execute(sql)
    instrument = pd.DataFrame(result.fetchall())
    instrument.columns = result.keys()
    #connection.close()
    s.close()
    return instrument


def add_up_data_vendors(name_,website_url_):
    '''Funcion que agrega o actualiza Data_vendors con name_ y website_url_'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    #sql1=select([Data_vendors]).where(Data_vendors.name==name_) ## verifica que exista el data vendor
    sql1 = s.query(Data_vendors).filter(Data_vendors.name==name_).first()
    if sql1 is None: # si "name_" existe , entonces extare id
        aux=Data_vendors(name=name_,website_url=website_url_,created_date=datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: # si no existe el data vendor , lo agrega
        id_=sql1.id
        s.close()   
    return id_


def update_data_vendor(name_,website_url_):
    '''Funcion que actualiza la tabla de data vendors'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql=update(Data_vendors).where(Data_vendors.name==name_).values({"last_updated_date":datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')})
    s.execute(sql)
    s.commit()
    s.close()


def update_instrument_db(df_instrument,row):
    '''funcion que actualiza la tabla instrument con datos provenientes de quandl'''
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
    '''Funcion que usa la API de Quandl los datos y metadatos y descarga los datos para cada instrumento
    para los dos tipos de fuentes de datos de quandl : datasets y datatables'''
    # Load the JSON to a Python list & dump it back out as formatted JSON
    print("Getting pricing for:", row["name"])
    name_="Quandl"
    website_url_="https://www.quandl.com/"
    if row["type"]=="datasets":
        api_call_head1= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        data1 = urllib2.urlopen("%s" % (api_call_head1)).read()
        json_data = data1.decode('utf8').replace("'", '"')
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
    else:
        api_call_data= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+".json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        api_call_meta= "https://www.quandl.com/api/v3/"+row["type"]+"/"+row["name"]+"/metadata.json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        data1 = urllib2.urlopen("%s" % (api_call_data)).read()
        meta = urllib2.urlopen("%s" % (api_call_meta)).read()
        json_data = data1.decode('utf8').replace("'", '"')
        json_meta = meta.decode('utf8').replace("'", '"')
        json_dataset= json.loads(json_data)["datatable"]
        json_meta= json.loads(json_meta)
        columns=[json_dataset["columns"][i]["name"] for i in range(len(json_dataset["columns"]))]
        cols = [columns[1]]+[columns[0]]+columns[2:]
        df = pd.DataFrame(data=json_dataset["data"],columns=columns)
        df = df[cols]
        df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","oldest_available_date","start_date","end_date","frequency"])
        df_instrument.loc[0] = [json_meta["datatable"]["status"]["refreshed_at"],max(df.date),min(df.date),min(df.date),max(df.date),json_meta["datatable"]["status"]["update_frequency"]]
        name_date=df.columns[0]   ## get column name from date
        df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
        df = df[df.value.notnull()]  ## filter NAN values from value column
        df.columns = ['ticker','date_', 'category','value'] ## rename columns
        df["instrument_id"]=row["id"]
        df["data_vendor_id"]=add_up_data_vendors(name_,website_url_)
        df.to_csv(Path_save+"df_"+row["name"].replace("/","")+".csv") ## save local
        #df_instrument.to_csv(Path_save+"df_instrument_"+row["database_name"]+"_"+row["name"]+".csv") ##save local
        update_data_vendor(name_,website_url_)
        update_instrument_db(df_instrument,row) ## update database (table instrument with dates, text,etc)
    #return df,df_instrument



def get_data_yahoo(row):
    '''Funcion que usa la API de Yahoo los datos y metadatos y descarga los datos para cada instrumento
    para los dos tipos de fuentes de datos de quandl : datasets y datatables'''
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
    '''Funcion que garantiza la descarga de la fuente quandl'''
    while True:
        try:
            get_data_quandl(row) ; break
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)


def get_safe_data_yahoo(row):
    '''Funcion que garantiza la descarga de la fuente Yahoo'''
    while True:
        try:
            get_data_yahoo(row); break
        except Exception as err:
            print("Error: {0}, waiting to try again in {1}".format(err, sleep_time))
            sleep(sleep_time)

def download_data(instrument):
    '''Funcion que descarga los precios historicos de cada instrumento'''
    for index, row in instrument.iterrows():
        if row["data_vendor_id"]==add_up_data_vendors("Quandl","https://www.quandl.com/"):
            get_safe_data_quandl(row)
        else:
            get_safe_data_yahoo(row)



def write_db():
    '''Funcion que realiza la ingesta a la db de los datos de precios historicos guardados localmente'''
    all_files=glob.glob(Path_save+"*.csv")
    for i in all_files:
        print(i)
        df=pd.read_csv(i)[["ticker","date_","category","value","instrument_id"]]
        df.to_sql(con=engine, name='time_series', if_exists='append',index=False) 

def delete_files_dir():
    '''Funcion que borra todos los archivos temporales '''
    folder = "./temp_data"
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
        try:
            if os.path.isfile(file_path):
                os.unlink(file_path)
            #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        except Exception as e:
            print(e)


delete_files_dir() ## borrar archivos en caso de que existan
instrument=get_instrument() ## get data from instrument table from database (list of instruments)
print("Descargando instrumentos")
download_data(instrument) ## descarga de precios historicos
print("Escribiendo en dase de datos")
write_db() ## ingesta a base de datos
print("Ingesta de datos de instrumentos hecha!")



################






