from functools import lru_cache

import numpy as np
import pandas as pd
from bokeh.io import curdoc
from bokeh.layouts import row, column
from bokeh.models import ColumnDataSource, Div, AutocompleteInput
from bokeh.models.widgets import Select, TableColumn, DataTable, \
    DateFormatter, Button
from bokeh.plotting import figure

from awesomplete_input import AwesompleteInput
from utils import get_ticker_to_company, predict, get_cassandra_session

"""
This scrip produces the dashboard with time series analysis.
Data is loaded from Cassandra db.
Run script from terminal: bokeh serve --show app.py

"""
import sys

sys.stdout = open('logs.log', 'w')
sys.stderr = open('logs.log', 'w')

# --- CONSTANTS -----

SESSIONS = get_cassandra_session()
TICKER_TO_COMPANY_NAME = get_ticker_to_company(SESSIONS)
COMPANIES_LIST = TICKER_TO_COMPANY_NAME.name.values.tolist()


# --- SMALL METHODS -----

@lru_cache()
def load_ticker_data(ticker):
    """
    Load data from Cassandra database for given ticker (symbol)
    """
    query = "SELECT * FROM wikiprice WHERE ticker=%s"
    data = SESSIONS.execute_async(query, [ticker])
    rows = data.result()
    data = rows._current_rows
    data['date'] = pd.to_datetime(data['date'], format="%Y-%m/%d %H:%M:%S")
    data = data.set_index('date')
    return data


def get_ticker_by_company(company_name):
    return TICKER_TO_COMPANY_NAME[TICKER_TO_COMPANY_NAME.name == company_name].ticker.iloc[0]


def update_aggregated_data(ticker_data):
    global aggregated_current_data
    ticker_original = ticker_data.resample('D').fillna('nearest')
    aggregated_current_data = {
        'Monthly': aggregate(ticker_original, 'MS'),
        'Annually': aggregate(ticker_original, 'AS'),
        'Quarterly': aggregate(ticker_original, 'QS'),
        'Daily': aggregate(ticker_original, 'D'),
        'Original': ticker_original
    }


def aggregate(data, how):
    aggregated_df = data.resample(how).mean()
    return aggregated_df


def update_company_select(selected=None):
    clean_prediction()
    company_name = company_chooser.value
    company_data = get_company_data(company_name)
    update_aggregated_data(company_data)
    update_current_data('Daily')
    update_indicator_select()


def get_company_data(company_name):
    ticker = get_ticker_by_company(company_name)
    return load_ticker_data(ticker)


def update_current_data(aggregator):
    aggregated_data = aggregated_current_data[aggregator]
    current_data.data = aggregated_data.reset_index().to_dict(orient='list')


def update_graph(current_series):
    current_series_dict = current_data.data
    series_source.data = {'date': current_series_dict['date'], 'value': current_series_dict[current_series]}
    static_series_source.data = series_source.data


def update_prediction_source(prediction):
    global current_prediction_source
    current_prediction_source.data = prediction


def add_confidence_interval(future):
    global patch_confidence_source
    patch_confidence_source.data = {
        'xs': np.append(future['date'], future['date'][::-1]),
        'ys': np.append(future['lower'], future['upper'][::-1])
    }


def clean_confidence_interval():
    global patch_confidence_source
    patch_confidence_source.data = {
        'xs': [],
        'ys': []
    }


def update_summary_stats(current_series):
    data = pd.DataFrame(current_data.data)[current_series]
    df = pd.DataFrame(data.describe()).T.round()
    df.columns = ['count', 'mean', 'std', 'min', '25%', '50%', '75%', 'max']
    summary_data.data = df.to_dict(orient='list')


def update_indicator_select():
    clean_prediction()
    current_series = indicator_chooser.value
    update_graph(current_series)
    update_summary_stats(current_series)


def clean_prediction():
    global current_prediction_source
    current_prediction_source.data = {'date': [], 'value': [], 'lower': [], 'upper': []}
    clean_confidence_interval()


# --- HANDLERS WHICH TRACK THE CHANGE OF SELECTORS -----

def company_change_handler(attrname, old, new):
    update_company_select()


def indicator_change_handler(attrname, old, new):
    update_indicator_select()


def aggregator_select_handler(attrname, old, new):
    clean_prediction()
    aggregation = aggregator_chooser.value
    update_current_data(aggregation)
    update_indicator_select()


def update_button_predict():
    wait_label.text = 'Please wait...'
    current_indicator = indicator_chooser.value
    current_aggregator = aggregator_chooser.value
    global aggregated_current_data
    current_series = aggregated_current_data[current_aggregator][current_indicator]
    future = predict(current_series)
    update_prediction_source(future)
    add_confidence_interval(future)
    wait_label.text = ''


# --- DATA SOURCES FOR INTERACTIVE WIDGETS -----
series_source = ColumnDataSource(data=dict())
static_series_source = ColumnDataSource(data=dict())
current_prediction_source = ColumnDataSource(data=dict({'date': [], 'value': [], 'lower': [], 'upper': []}))
patch_confidence_source = ColumnDataSource(data=dict({'xs': [], 'ys': []}))

current_data = ColumnDataSource(data=dict())
summary_data = ColumnDataSource(data=dict())
aggregated_current_data = {}

tools = 'pan,wheel_zoom,xbox_select,reset'

# --- COMPANY CHOOSER -----
company_label = Div(text='<h4>1 step: Choose the company</h4>', width=200)
company_chooser = AwesompleteInput(value='Facebook, Inc.', completions=COMPANIES_LIST)
# company_chooser = AutocompleteInput(value='Facebook, Inc.', completions=COMPANIES_LIST)
company_chooser.on_change('value', company_change_handler)

# --- INDICATOR CHOOSER -----
INDICATORS = ['adj_close', 'adj_high', 'adj_low', 'adj_open', 'adj_volume', 'close', 'ex_dividend', 'high', 'low',
              'openp', 'split_ratio', 'volume']
series_label = Div(text='<h4>2 step: Choose the indicator</h4>', width=200)
indicator_chooser = Select(value='adj_close', options=INDICATORS, width=151)
indicator_chooser.on_change('value', indicator_change_handler)

# --- AGGREGATOR CHOOSER -----
aggregator_label = Div(text='<h4>3 step: How to aggregate?</h4>', width=200)
AGGREGATORS = ['Daily', 'Monthly', 'Quarterly', 'Annually']
aggregator_chooser = Select(value='Daily', options=AGGREGATORS, width=151)
aggregator_chooser.on_change('value', aggregator_select_handler)

# --- DESCRIPTION TEXT -----
content_filename = 'description.html'
description = Div(text=open(content_filename).read(), render_as_text=False, width=900)

# --- TIME SERIES PLOT -----
time_series = figure(plot_width=900, plot_height=200, tools=tools, x_axis_type='datetime', active_drag="xbox_select")

# Time series line for original data
time_series.line('date', 'value', source=static_series_source)

# Time series line for prediction
time_series.line('date', 'value', source=current_prediction_source, color='red')

# Time series line for selected xrange
time_series.circle('date', 'value', size=1, source=series_source, color=None, selection_color="orange")

# Confidence interval for prediction
time_series.patch(x="xs", y="ys", fill_color="#86b5d6", fill_alpha=0.3, source=patch_confidence_source)

# --- SUMMARY TABLE -----
summary_label = Div(text='<h3>Summary statistics</h3>', width=200)
columns_summary_table = [
    TableColumn(field='count', title='count'),
    TableColumn(field='mean', title='mean'),
    TableColumn(field='std', title='std'),
    TableColumn(field='min', title='min'),
    TableColumn(field='25%', title='25%'),
    TableColumn(field='50%', title='50%'),
    TableColumn(field='75%', title='75%'),
    TableColumn(field='max', title='max')
]
summary_stat_data_table = DataTable(source=summary_data, columns=columns_summary_table, width=900, height=60)

# --- FULL ORIGINAL DATA TABLE -----
full_table_label = Div(text='<h3>Original data</h3>', width=200)
columns_original_table = [
    TableColumn(field='date', title='date', formatter=DateFormatter()),
    TableColumn(field='adj_close', title='adj_close'),
    TableColumn(field='adj_high', title='adj_high'),
    TableColumn(field='adj_low', title='adj_low'),
    TableColumn(field='adj_open', title='adj_open'),
    TableColumn(field='adj_volume', title='adj_volume'),
    TableColumn(field='close', title='close'),
    TableColumn(field='ex_dividend', title='ex_dividend'),
    TableColumn(field='high', title='high'),
    TableColumn(field='low', title='low'),
    TableColumn(field='openp', title='openp'),
    TableColumn(field='split_ratio', title='split_ratio'),
    TableColumn(field='volume', title='volume')
]

full_data_table = DataTable(source=current_data, columns=columns_original_table, width=900)

# --- PREDICTION BUTTON -----
prediction_label = Div(text='<h4>Click if you want ot see the prediction for the time series and press Enter<h4>')
wait_label = Div(text='')
button = Button(label="Predict", button_type="success")
button.on_click(update_button_predict)

# --- LAYOUT BUILDING -----

selectors = column(
    row(company_label, company_chooser),
    row(series_label, indicator_chooser),
    row(aggregator_label, aggregator_chooser)
)

main_row = row(selectors, column(button, prediction_label, wait_label))

time_series_plot = column(time_series)
layout = column(description,
                main_row,
                time_series_plot,
                column(summary_label, summary_stat_data_table),
                column(full_table_label, full_data_table))

# --- UPDATE LAYOUT AND RUN -----

update_company_select()
curdoc().add_root(layout)
curdoc().title = "Stocks"
