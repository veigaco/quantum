#Sqlalchemy_version: '1.2.6'
#Python 3.6.4
from mvo_utils import *
from Schema import *


delete_files_dir()
instrument=get_instrument() ## get data from instrument table in database (list of instruments)
print("Download instrument list")
download_data_daily(instrument)
print("writing in database")
write_db()
print("daily prices data upload finished!")



