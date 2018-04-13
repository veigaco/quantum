from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode

# SET PASSWORD = PASSWORD('Qu4ntum2018');
# CREATE DATABASE securities_master_database;
# USE securities_master_database;
# CREATE USER 'quantum_user'@'localhost' IDENTIFIED BY 'Qu4ntum_u$3r';
# GRANT ALL PRIVILEGES ON securities_master_database.* TO 'quantum_user'@'localhost';
# FLUSH PRIVILEGES;

DB_NAME = 'securities_master_database'

TABLES = {}
TABLES['instrument_type'] = (
    "CREATE TABLE `instrument_type` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(255) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['industries'] = (
    "CREATE TABLE `industries` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(255) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['sectors'] = (
    "CREATE TABLE `sectors` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(255) NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['instrument'] = (
    "CREATE TABLE `instrument` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `instrument_type_id` int NOT NULL,"
    "  `industry_id` int NOT NULL,"
    "  `sector_id` int NOT NULL,"
    "  `database_name` varchar(255) NOT NULL,"
    "  `type_database` varchar(255) NOT NULL,"
    "  `name` varchar(255) NOT NULL,"
    "  `description` text NULL,"
    "  `refresh_at` datetime NULL,"
    "  `newest_available_date` datetime NULL,"
    "  `oldest_available_date` varchar(255) NULL,"
    "  `start_date` datetime NULL,"
    "  `end_date` datetime NULL,"
    "  `premium`  bool NULL,"
    "  `frequency` varchar(255) NULL,"
    "  PRIMARY KEY (`id`),"
    "  FOREIGN KEY (`instrument_type_id`) REFERENCES `instrument_type`(`id`) ON UPDATE CASCADE ,"
    "  FOREIGN KEY (`industry_id`) REFERENCES `industries`(`id`) ON UPDATE CASCADE ,"
    "  FOREIGN KEY (`sector_id`) REFERENCES `sectors`(`id`) ON UPDATE CASCADE "
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")



TABLES['time_series'] = (
    "  CREATE TABLE `time_series` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `data_vendor_id` int NOT NULL,"
    "  `instrument_id` int NOT NULL,"
    "  `ticker` varchar(255) NULL,"
    "  `category` varchar(255) NULL,"
    "  `date_` datetime NOT NULL,"
    "  `value` decimal(19,4) NULL,"
    "  PRIMARY KEY (`id`),"
    "  FOREIGN KEY (`instrument_id`) REFERENCES `instrument` (`id`) ON UPDATE CASCADE ,"
    "  FOREIGN KEY (`data_vendor_id`) REFERENCES `data_vendors` (`id`) ON UPDATE CASCADE "
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['data_vendors'] = (
    "CREATE TABLE `data_vendors` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `name` varchar(255) NOT NULL,"
    "  `website_url` varchar(255) NULL,"
    "  `created_date` datetime NOT NULL,"
    "  `last_updated_date` datetime NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")


TABLES['listed_securities'] = (
    "CREATE TABLE `listed_securities` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `instrument_id` int NOT NULL,"
    "  `exchange_id` int NOT NULL,"
    "  `symbol` varchar(255) NOT NULL,"
    "  `listing_start_date` datetime NULL,"
    "  `listing_end_date` datetime NULL,"
    "  `csi_number` int NULL,"
    "  `market_open` decimal(19,4) NULL,"
    "  `market_close` decimal(19,4) NULL,"
    "  `trading_window_in_days` bigint NULL,"
    "  PRIMARY KEY (`id`),"
    "  FOREIGN KEY (`instrument_id`) REFERENCES `instrument` (`id`) ON UPDATE CASCADE ,"
    "  FOREIGN KEY (`exchange_id`) REFERENCES `exchanges` (`id`) ON UPDATE CASCADE "
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['exchanges'] = (
    "CREATE TABLE `exchanges` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `label` varchar(255) NOT NULL,"
    "  `name` varchar(255) NOT NULL,"
    "  `timezone_name` varchar(255) NULL,"
    "  `currency` varchar(255) NULL,"
    "  `market_open` decimal(19,4) NULL,"
    "  `market_close` decimal(19,4) NULL,"
    "  `trading_window_in_days` bigint NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")


TABLES['options'] = (
    "CREATE TABLE `options` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `instrument_id` int NOT NULL,"
    "  `expiration` varchar(255) NULL,"
    "  `type` varchar(255) NULL,"
    "  `strike` varchar(255) NULL,"
    "  `style` varchar(255) NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['eod_option_quotes'] = (
    "CREATE TABLE `eod_option_quotes` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `option_id` int NOT NULL,"
    "  `date_` datetime NOT NULL,"
    "  `last` decimal(19,4) NULL,"
    "  `ask` decimal(19,4) NULL,"
    "  `volume` bigint NULL,"
    "  `open_interest` decimal(19,4) NULL,"
    "  PRIMARY KEY (`id`),"
    "  FOREIGN KEY (`option_id`) REFERENCES `options` (`id`) ON UPDATE CASCADE "
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

cnx = mysql.connector.connect(user='quantum_user',password= 'Qu4ntum_u$3r',)
cursor = cnx.cursor()



def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cnx.database = DB_NAME  
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)



for name, ddl in TABLES.iteritems():
    try:
        print("Creating table {}: ".format(name), end='')
        cursor.execute(ddl)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

cursor.close()
cnx.close()


### Para revisar que se haya creado bien :
#SELECT DATABASE();
# SHOW TABLES;
# DESCRIBE exchanges;
# DROP TABLES time_series,time_series_equities,data_vendors,update_frecuencies,eod_option_quotes,options,listed_securities,exchanges,intrument,instrument_type,industries,sectors
# DROP TABLES instrument,instrument_type,industries,sectors;


'''TABLES['update_frecuencies'] = (
    "CREATE TABLE `update_frecuencies` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `label` varchar(255) NULL,"
    "  PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")




TABLES['time_series_equities'] = (
    "  CREATE TABLE `time_series_equities` ("
    "  `id` int NOT NULL AUTO_INCREMENT,"
    "  `data_vendor_id` int NOT NULL,"
    "  `update_frecuency_id` int NULL,"
    "  `instrument_id` int NOT NULL,"
    "  `ticket` varchar(255) NULL,"
    "  `period` varchar(255) NULL,"
    "  `date_` datetime NOT NULL,"
    "  `open` decimal(19,4) NULL,"
    "  `close` decimal(19,4) NULL,"
    "  `high`  decimal(19,4) NULL,"
    "  `low`  decimal(19,4) NULL,"
    "  `volume` bigint NULL,"
    "  `ex_dividend`  decimal(19,4) NULL,"
    "  `split_ratio`   decimal(19,4) NULL,"
    "  `adj_open` decimal(19,4) NULL,"
    "  `adj_close` decimal(19,4) NULL,"
    "  `adj_high`  decimal(19,4) NULL,"
    "  `adj_low`  decimal(19,4) NULL,"
    "  `adj_volume` bigint NULL,"
    "  PRIMARY KEY (`id`),"
    "  FOREIGN KEY (`instrument_id`) REFERENCES `instrument` (`id`) ON UPDATE CASCADE ,"
    "  FOREIGN KEY (`data_vendor_id`) REFERENCES `data_vendors` (`id`) ON UPDATE CASCADE , "
    "  FOREIGN KEY (`update_frecuency_id`) REFERENCES `update_frecuencies` (`id`) ON UPDATE CASCADE "
    ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")'''