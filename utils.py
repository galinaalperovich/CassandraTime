import numpy as np
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from fbprophet import Prophet

KEYSPACE = "wiki_price_keyspace"
PERIODS_TO_PREDICT = {'D': 365, 'MS': 12, 'AS': 10, 'QS': 8}
CASSANDRA_USER = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'


def get_data(query, session):
    data = session.execute_async(query)
    rows = data.result()
    return rows._current_rows


def get_ticker_to_company(session):
    query = "SELECT * FROM tickers"
    tickers = get_data(query, session)
    query = "SELECT distinct ticker FROM wikiprice"
    tickers_wiki = get_data(query, session)
    return pd.merge(tickers_wiki, tickers, on='ticker')


def predict(series):
    """
    Returns prediction + lower and upper bounds for uncertainty interval 80%
    :return pandas.DataFrame
    """
    ts_log = np.log(series)
    freq = ts_log.index.freqstr
    after = get_num_lags_back(freq, series)
    df_fit = get_df_for_fit(after, ts_log)
    model = Prophet()
    print('Fitting.......')
    model.fit(df_fit)
    freq_key = [key for key in PERIODS_TO_PREDICT.keys() if key in freq][0]
    per = PERIODS_TO_PREDICT[freq_key]
    future = model.make_future_dataframe(periods=per, freq=freq_key)
    print('Prediction.......')
    forecast = model.predict(future)
    result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-per - 1:]
    result[['yhat', 'yhat_lower', 'yhat_upper']] = np.exp(result[['yhat', 'yhat_lower', 'yhat_upper']])
    result_df = {'date': result.ds, 'value': result.yhat, 'lower': result.yhat_lower, 'upper': result.yhat_upper}
    print('Prediction done!')
    return result_df


def get_df_for_fit(after, ts_log):
    ts_log_cut = ts_log[-after:]  # when there are lot of data it is long to predict
    return pd.DataFrame({'y': ts_log_cut.tolist(), 'ds': ts_log_cut.index})


def get_num_lags_back(freq, series):
    return len(series) // 4 if 'D' in freq else len(series)


def pandas_factory(column_names, rows):
    return pd.DataFrame(rows, columns=column_names)


def get_cassandra_session():
    auth_provider = get_auth_provider()
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    return session


def get_auth_provider():
    return PlainTextAuthProvider(username=CASSANDRA_USER, password=CASSANDRA_PASSWORD)
