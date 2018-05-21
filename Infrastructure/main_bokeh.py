from Schema import *
from mvo_utils import *
import pandas as pd
from sqlalchemy.orm import sessionmaker#,relationship, backref
from sqlalchemy import create_engine,select
from Schema import *
import numpy as np
from bokeh.models import ColumnDataSource, Select, Slider
from bokeh.layouts import widgetbox, row, column
from bokeh.io import curdoc
from bokeh.plotting import figure, output_file, show

## pandasframe que contiene informacion inicial
engine = create_engine('mysql+pymysql://quantum_user:Qu4ntum_u$3r@localhost/securities_master_database')

def get_price_ts(tick):
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()
    sql = select([Time_series]).where(Time_series.ticker==tick).select_from(Time_series)
    result=s.execute(sql)
    df_ = pd.DataFrame(result.fetchall()).drop_duplicates()
    df_.columns = result.keys() 
    df1=df_[["id","instrument_id","ticker","date_"]].set_index("date_")
    df2=df_.pivot(index='date_', columns='category')['value'] 
    df=pd.merge(df1, df2, left_index=True, right_index=True)
    s.close()
    return df

instrument=get_instrument()

# set up initial data
dataset = 'SPY'

# prepare some data
df=get_price_ts(dataset)
price = np.array(df['Adj Close'])
price_dates = np.array(df.index, dtype=np.datetime64)
window_size = 30
window = np.ones(window_size)/float(window_size)
price_avg = np.convolve(price, window, 'same')

# output to static HTML file
#output_file("stocks.html", title="stocks.py example")

# create a new plot with a a datetime axis type
p = figure(width=700, height=350, x_axis_type="datetime",title=df.ticker[0]+" One-Month Average")
p.legend.location = "top_left"
p.grid.grid_line_alpha=0
p.xaxis.axis_label = 'Date'
p.yaxis.axis_label = 'Price'
p.ygrid.band_fill_color="olive"
p.ygrid.band_fill_alpha = 0.1

# add renderers
#p.circle(price_dates, price, size=4, color='darkgrey', alpha=0.2, legend='close')
#p.line(price_dates, price_avg, color='navy', legend='avg'
source = ColumnDataSource(data=dict(x=price_dates, y=price))
p.circle("x","y" , size=4, color='darkgrey', alpha=0.2, legend='close',source=source)
source1 = ColumnDataSource(data=dict(x=price_dates, y=price_avg))
p.line("x", "y", color='navy', legend='avg',source=source1)


# set up widgets
datasets_names = instrument.name.unique().tolist()

dataset_select = Select(value='SPY',
                        title='Select dataset:',
                        width=200,
                        options=datasets_names)


# set up callbacks

def update_dataset(attrname, old, new):
    global df

    dataset = dataset_select.value

    df=get_price_ts(dataset)
    price = np.array(df['Adj Close'])
    price_dates = np.array(df.index, dtype=np.datetime64)
    window_size = 30
    window = np.ones(window_size)/float(window_size)
    price_avg = np.convolve(price, window, 'same')


    source.data = dict(x=price_dates, y=price)
    source1.data = dict(x=price_dates, y=price_avg)
    p.title.text = df.ticker[0]+" One-Month Average"


dataset_select.on_change('value', update_dataset)


# set up layout
selects = row(dataset_select,width=220)
inputs = column(selects)

# add to document
curdoc().add_root(row(inputs, p))
curdoc().title = "test"