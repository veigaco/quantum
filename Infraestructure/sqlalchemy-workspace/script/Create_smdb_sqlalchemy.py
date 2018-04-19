#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
from sqlalchemy import Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func,create_engine
from sqlalchemy.orm import relationship, backref,sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from Schema import *


# SET PASSWORD = PASSWORD('Qu4ntum2018');
# CREATE DATABASE securities_master_database;
# USE securities_master_database;
# CREATE USER 'quantum_user'@'localhost' IDENTIFIED BY 'Qu4ntum_u$3r';
# GRANT ALL PRIVILEGES ON securities_master_database.* TO 'quantum_user'@'localhost';
# FLUSH PRIVILEGES;

engine = create_engine('mysql+pymysql://quantum_user:Qu4ntum_u$3r@localhost/securities_master_database')
## create database
session = sessionmaker()
session.configure(bind=engine)
Base.metadata.create_all(engine)
print("Securities Master Database created")


### Para revisar que se haya creado bien :
#SELECT DATABASE();
# SHOW TABLES;
# DESCRIBE exchanges;
# DROP TABLES time_series,time_series_equities,data_vendors,update_frecuencies,eod_option_quotes,options,listed_securities,exchanges,intrument,instrument_type,industries,sectors;
# DROP TABLES instrument,instrument_type,industries,sectors;

