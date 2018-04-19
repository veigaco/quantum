#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
import pandas as pd
from datetime import date, datetime, timedelta
from sqlalchemy.orm import sessionmaker,relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine,Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func,select,join,update,and_
from Schema import *
import time

## pandasframe que contiene informacion inicial
path ="./"
data=pd.read_csv(path+"data.csv")



engine = create_engine('mysql+pymysql://quantum_user:Qu4ntum_u$3r@localhost/securities_master_database')


## add data
def add_up_Table(name_,Table):
    '''Funcion que agrega o actualiza Table con name_'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Table).filter(Table.name==name_).first()
    if sql1 is None: # si "name_" no sexiste , lo crea
        aux=Table(name=name_)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: # si existe extrae id
        id_=sql1.id
        s.close()
        
    return id_


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

def add_up_instrument(id_1,id_2,id_3,id_4,name1,type1,desc):
    '''Funcion que agrega o actualiza Instrument con id_1,id_2,id_3,id_4,name1,type1,desc'''
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql1 = s.query(Instrument).filter(Instrument.name==name1).first()
    if sql1 is None: # si "name_" existe , entonces extare id
        aux=Instrument(industry_id=id_1,instrument_type_id=id_2,sector_id=id_3,data_vendor_id=id_4,name=name1,type=type1,description=desc)
        s.add(aux)
        s.commit()
        id_=aux.id
        s.close()
    else: # si no existe el instrument , lo agrega
        id_=sql1.id
        s.close()
    return id_


## ingesta base datos sobre la tabla "data"
for index, row in data.iterrows():
    #print(row)
    id_1=add_up_Table(row["industry_name"],Industries) ### industries
    id_2=add_up_Table(row["instrument_name"],Instrument_type) ### instrument_type
    id_3=add_up_Table(row["sector_name"],Sectors) ### sectors
    id_4=add_up_data_vendors(row["data_vendor"],row["website_vendor"])
    add_up_instrument(id_1,id_2,id_3,id_4,row["name"],row["type"],row["description"])


print("Primera ingesta de datos hecha!")

