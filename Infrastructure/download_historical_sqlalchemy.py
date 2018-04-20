#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
from Schema import *
from mvo_utils import *



delete_files_dir() ## delete files in "temp_Data"
instrument=get_instrument() ## get data from instrument table in database (list of instruments)
print("Download instrument list")
download_data(instrument) ## download historical prices
print("writing in database")
write_db() ##write prices in database
print("historical prices data upload finished!")



################






