#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
import pandas as pd
from Schema import *
from mvo_utils import *

## pandasframe with initial information
path ="./"
data=pd.read_csv(path+"data.csv") 
asset_class=pd.read_csv(path+"asset_classes.csv")

## Load data into mysql database
for index, row in data.iterrows():
    #print(row)
    id_1=add_up_Table(row["industry_name"],Industries) ### industries
    id_2=add_up_Table(row["instrument_name"],Instrument_type) ### instrument_type
    id_3=add_up_Table(row["sector_name"],Sectors) ### sectors
    id_4=add_up_data_vendors(row["data_vendor"],row["website_vendor"])
    add_up_instrument(id_1,id_2,id_3,id_4,row["name"],row["type"],row["description"])

for index,row in asset_class.iterrows():
    id_1=get_id_instrument(row["ETF"])
    add_asset(row["Universe"],row["Broad_Asset_Class"],row["Type"],row["Country"],row["Index"],row["ETF"],row["Name"],id_1)

print("initial data upload finished!")

