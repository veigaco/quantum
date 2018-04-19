#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
from sqlalchemy.orm import sessionmaker,relationship, backref
from sqlalchemy import create_engine,Column, DateTime, String,Float,Boolean ,Text,Integer, ForeignKey, func
from sqlalchemy.ext.declarative import declarative_base

## schema database

Base = declarative_base()
 

class Instrument_type(Base):
    __tablename__ = 'instrument_type'
    id = Column(Integer, primary_key=True)
    name= Column(String(255))

class Industries(Base):
    __tablename__ = 'industries'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))

class Sectors(Base):
    __tablename__ = 'sectors'
    id = Column(Integer, primary_key=True)
    name = Column(String(255))

class Data_vendors(Base):
    __tablename__ = 'data_vendors'
    id = Column(Integer, primary_key=True)
    name=Column(String(255))
    website_url=Column(String(255))
    created_date=Column(DateTime)
    last_updated_date=Column(DateTime)

class Instrument(Base):
    __tablename__ = 'instrument'
    id = Column(Integer, primary_key=True)
    instrument_type_id=Column(Integer,ForeignKey('instrument_type.id'))
    industry_id=Column(Integer,ForeignKey('industries.id'))
    sector_id=Column(Integer,ForeignKey('sectors.id'))
    data_vendor_id=Column(Integer,ForeignKey('data_vendors.id'))
    name=Column(String(255))
    type=Column(String(255))
    description=Column(Text)
    refresh_at=Column(DateTime)
    newest_available_date=Column(DateTime)
    oldest_available_date=Column(DateTime)
    start_date=Column(DateTime)
    end_date=Column(DateTime)
    frequency=Column(String(255))



class Time_series(Base):
    __tablename__ = 'time_series'
    id = Column(Integer, primary_key=True)
    instrument_id=Column(Integer,ForeignKey('instrument.id'))
    ticker=Column(String(255))
    category=Column(String(255))
    date_=Column(DateTime)
    value=Column(Float)

class Exchanges(Base):
    __tablename__ = 'exchanges'
    id = Column(Integer, primary_key=True)
    label=Column(String(255))
    name=Column(String(255))
    timezone_name=Column(String(255))
    currency=Column(String(255))
    market_open=Column(Float)
    market_close=Column(Float)
    trading_window_in_days=Column(Integer)


class Listed_securities(Base):
    __tablename__ = 'listed_securities'
    id = Column(Integer, primary_key=True)
    instrument_id=Column(Integer,ForeignKey('instrument.id'))
    exchange_id=Column(Integer,ForeignKey('exchanges.id'))
    symbol=Column(String(255))
    listing_start_date=Column(DateTime)
    listing_end_date=Column(DateTime)
    csi_number=Column(Integer)
    market_open=Column(Float)
    market_close=Column(Float)
    trading_window_in_days=Column(Integer)

class Options(Base):
    __tablename__ = 'options'
    id = Column(Integer, primary_key=True)
    instrument_id=Column(Integer,ForeignKey('instrument.id'))
    expiration=Column(String(255))
    type=Column(String(255))
    strike=Column(String(255))
    style=Column(String(255))


class Eod_option_quotes(Base):
    __tablename__ = 'eod_option_quotes'
    id = Column(Integer, primary_key=True)
    option_id=Column(Integer,ForeignKey('options.id'))
    date_=Column(DateTime)
    last=Column(Float)
    ask=Column(Float)
    volume=Column(Integer)
    open_interest=Column(Float)
