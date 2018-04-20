#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
from sqlalchemy import create_engine#Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func
from sqlalchemy.orm import sessionmaker# relationship, backref,
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
# DROP TABLES time_series,eod_option_quotes,options,listed_securities,exchanges,data_vendors,instrument,instrument_type,industries,sectors,asset_classes;
# DROP TABLES time_series,eod_option_quotes,options,listed_securities,exchanges,data_vendors,instrument,instrument_type,industries,sectors,asset_classes;
# DROP TABLES data_vendors;

##SELECT * FROM (SELECT * FROM time_series ORDER BY id DESC LIMIT 50) sub ORDER BY id ASC;
