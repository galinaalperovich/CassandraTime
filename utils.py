import numpy as np
import pandas as pd
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from fbprophet import Prophet


def get_data_by(query, session):
    data = session.execute_async(query)
    rows = data.result()
    data = rows._current_rows
    return data


def get_ticker_to_company(session):
    query = "SELECT * FROM tickers"
    tickers = get_data_by(query, session)
    query = "SELECT distinct ticker FROM wikiprice"
    tickers_wiki = get_data_by(query, session)
    return pd.merge(tickers_wiki, tickers, on='ticker')


periods = {'D': 365, 'MS': 12, 'AS': 10, 'QS': 8}


def predict(series):
    ts_log = np.log(series)
    freq = ts_log.index.freqstr
    if 'D' in freq:
        after = len(series) // 4
    else:
        after = len(series)
    ts_log_cut = ts_log[-after:]
    df = pd.DataFrame({'y': ts_log_cut.tolist(), 'ds': ts_log_cut.index})
    model = Prophet()
    print('Fitting.......')
    model.fit(df)
    freq_key = [key for key in periods.keys() if key in freq][0]
    per = periods[freq_key]
    future = model.make_future_dataframe(periods=per, freq=freq_key)
    print('Prediction.......')
    forecast = model.predict(future)
    result = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']][-per - 1:]
    result[['yhat', 'yhat_lower', 'yhat_upper']] = np.exp(result[['yhat', 'yhat_lower', 'yhat_upper']])
    result_df = {'date': result.ds, 'value': result.yhat, 'lower': result.yhat_lower, 'upper': result.yhat_upper}
    print('Prediction done!')
    return result_df


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)


def get_cassandra_session():
    KEYSPACE = "wiki_price_keyspace"
    auth_provider = PlainTextAuthProvider(username='cassandra', password='cassandra')
    cluster = Cluster(['127.0.0.1'], auth_provider=auth_provider)
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    return session


# For Cassandra username and password
# def getCredential(self):
#     return {'username': 'cassandra', 'password': 'cassandra'}


def getCredential(self):
    return {'username': '', 'password': ''}
