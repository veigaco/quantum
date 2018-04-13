
import pandas as pd
import urllib.request as urllib2
import io, pkgutil
import pymysql
import pymysql.cursors
from datetime import datetime
import json
import time


add_ts = ("INSERT INTO time_series "
               "(id, data_vendor_id,instrument_id,ticker,category,date_,value) "
               "VALUES (%s, %s, %s, %s, %s, %s, %s)")
add_data_vendors = ("INSERT INTO data_vendors "
                "(id, name, website_url, created_date, last_updated_date) "
                "VALUES (%s, %s, %s, %s, %s)")

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='quantum_user',
                             password='Qu4ntum_u$3r',
                             db='securities_master_database',
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor)

def get_instrument():
    '''Funcion que hace el query a la db para obtener el listado de instruments que se van a descargar'''
    with connection.cursor() as cursor:
        sql = " SELECT * FROM instrument LEFT JOIN instrument_type ON instrument.instrument_type_id=instrument_type.id;"
        cursor.execute(sql)
        result = cursor.fetchall()
    instrument=pd.DataFrame(result)
    #connection.close()
    cursor.close()
    return instrument


def get_last_id(table):
    '''Funcion que consulta el ultimo id de table para continuar generando 
    el id incremental cuando se insertan nuevos rows, si table es vacio regresa 0'''
    with connection.cursor() as cursor:# Read a single record
        sql = " SELECT MAX(id) as id FROM " + table
        cursor.execute(sql)
        result = cursor.fetchmany()
        if result[0]['id']==None:
            lastid=0
            cursor.close()
        else:
            lastid=result[0]['id']
            cursor.close()
    #connection.close()
    return lastid

#get_last_id("time_series")

def push_data_vendor_db():
    '''Funcion que llena la tabla de data_vendors'''
    with connection.cursor() as cursor: 
        cursor.execute(add_data_vendors,(1,"Quandl","https://www.quandl.com/",datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')))
        connection.commit()
        cursor.close()



def get_data_quandl(row):
    '''Funcion que usa la API de Quandl los datos y metadatos y descarga los datos para cada instrumento
    para los dos tipos de fuentes de datos de quandl : datasets y datatables'''
    # Load the JSON to a Python list & dump it back out as formatted JSON
    if row["type_database"]=="datasets":
        api_call_head1= "https://www.quandl.com/api/v3/"+row["type_database"]+"/"+row["database_name"]+"/"+row["name"]+".json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        data1 = urllib2.urlopen("%s" % (api_call_head1)).read()
        json_data = data1.decode('utf8').replace("'", '"')
        json_dataset= json.loads(json_data)["dataset"]
        df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","oldest_available_date","start_date","end_date","premium","frequency"])
        df_instrument.loc[0] = [json_dataset["refreshed_at"],json_dataset["newest_available_date"],json_dataset["oldest_available_date"],json_dataset["start_date"],json_dataset["end_date"],json_dataset["premium"],json_dataset["frequency"]]
        df = pd.DataFrame(data=json_dataset["data"],columns=json_dataset["column_names"])
        df["ticker"]=row["database_name"]+"/"+row["name"]
    else:
        api_call_data= "https://www.quandl.com/api/v3/"+row["type_database"]+"/"+row["database_name"]+"/"+row["name"]+".json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
        api_call_meta= "https://www.quandl.com/api/v3/"+row["type_database"]+"/"+row["database_name"]+"/"+row["name"]+"/metadata.json?api_key=DBHVvJ6NtLZ9b2MnQ7LA"
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
        df_instrument = pd.DataFrame(columns=["refreshed_at","newest_available_date","oldest_available_date","start_date","end_date","premium","frequency"])
        if json_meta["datatable"]["premium"] is None:
            premium=False
        else:
            premium=json_meta["datatable"]["premium"] 
        df_instrument.loc[0] = [json_meta["datatable"]["status"]["refreshed_at"],max(df.date),min(df.date),min(df.date),max(df.date),premium,json_meta["datatable"]["status"]["update_frequency"]]
    return df,df_instrument




def push_ts(x, *args):
    '''funcion que escribe en la tabla time series de la base de datos'''
    with connection.cursor() as cursor: 
        cursor.execute(add_ts,(int(get_last_id("time_series")+1),
                            int(args[1]),int(args[0]),x["ticker"],x["variable"],
                            datetime.strptime(x["date"], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                            float(x["value"])))
        connection.commit()
        cursor.close()

def update_instrument_db(df_instrument,row):
    '''funcion que actualiza la tabla instrument con datos provenientes de quandl'''
    with connection.cursor() as cursor: 
        cursor.execute ("UPDATE instrument "
            "SET refresh_at=%s, newest_available_date=%s, oldest_available_date=%s, start_date=%s , end_date=%s ,premium=%s ,frequency=%s WHERE id=%s"
            , (datetime.strptime(df_instrument["refreshed_at"][0], '%Y-%m-%dT%H:%M:%S.%fZ').strftime('%Y-%m-%d %H:%M:%S'),
                datetime.strptime(df_instrument["newest_available_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                datetime.strptime(df_instrument["oldest_available_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                datetime.strptime(df_instrument["start_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'), 
                datetime.strptime(df_instrument["end_date"][0], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'),
                df_instrument["premium"][0],
                df_instrument["frequency"][0],
                int(row["id"])))
        connection.commit()
        #connection.close()
        cursor.close()



def push_data(row):
    '''funcion que descarga los datos de la fuente, los estandariza, actualiza las tablas de la base de datos'''
    #row=instrument.loc[0]
    print(row)
    df,df_instrument= get_data_quandl(row) ## get data from quandl
    data_vendor_id=1  ### aqui falta generalizar para mas data vendors , por ahora esta solo quandl
    update_instrument_db(df_instrument,row) ## update database (table instrument with dates, text,etc)
    name_date=df.columns[0]   ## get column name from date
    df=df.melt(id_vars=["ticker",name_date]) ## reshape pandasframe
    df = df[df.value.notnull()]  ## filter NAN values from value column
    df.columns = ['ticker','date', 'variable','value'] ## rename columns
    df.apply(push_ts,axis=1,args=(row["id"],data_vendor_id)) ## push data to database



push_data_vendor_db() ## add values to data_vendors table
instrument=get_instrument() ## get data from instrument table from database (list of instruments)
instrument.apply(push_data,axis=1) ## download data from quandl and update database





'''
def push_ts_equities(row1,row,data_vendor_id, update_frecuency_id):
    with connection.cursor() as cursor: 
        try:
            cursor.execute(add_ts_equities,(int(get_last_id("time_series_equities")+1),
                        data_vendor_id, update_frecuency_id, row["id"], row1["ticker"],"daily",
                        datetime.strptime(row1['date'], '%Y-%m-%d').strftime('%Y-%m-%d %H:%M:%S'), #,row["ticker"]
                        row1["open"],row1["close"],row1["high"],row1["low"],int(row1["volume"]),
                        row1["ex-dividend"],row1["split_ratio"],row1["adj_open"],row1["adj_close"],
                        row1["adj_high"],row1["adj_low"],int(row1["adj_volume"])))
            connection.commit()
        except:
            connection.rollback()
            #fallas.append([row["instrument_type.name"],row1["ticker"]])
    #return fallas
'''



