import datetime
import logging
import os
from datetime import datetime

import pandas as pd
import quandl
from pandas import DataFrame

from config import QUANDL_KEY

quandl.ApiConfig.api_key = QUANDL_KEY

DATA_FOLDER = 'crypto_data'

EXCHANGES = ['COINBASE', 'BITSTAMP', 'KRAKEN', 'OKCOIN']

ALTCOIN_TICKERS = ['ETH', 'XRP', 'BCH', 'LTC', 'DASH', 'XMR', 'ETC', 'XEM', 'ZEC', 'DOGE']

POLONIEX_URL_FORMAT = 'https://poloniex.com/public?command=returnChartData&currencyPair={}&start={}&end={}&period={}'
#
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

DATA_START_DATE = datetime(2011, 1, 1)
PIVOT_START_DATE = datetime(1970, 1, 1)

CANDLESTICK_PERIOD = 86400  # 1D in seconds


def get_bitcoin_prices_from_exchange(exchange, refresh=False):
    """ Get Historical Bitcoin prices from various exchanges using Quandl API.
    Please have your Quandl Key set as config.QUANDL_KEY

    :param <exchange> of type String ex. "COINBASE"
    :return Pandas.DataFramce
     """
    df = pd.DataFrame()
    if refresh or not os.path.exists('{}/btcusd-{}.csv'.format(DATA_FOLDER, exchange)):
        logging.info("Fetching Bitcoin price for Exchange=%s from Source=Quandl" % exchange)
        df = quandl.get('BCHARTS/{}USD'.format(exchange), authtoken=QUANDL_KEY)
        if not df.empty:
            logging.info("### Successfully fetched BTC prices from %s exchange!!" % exchange)
            save_data(df, 'btcusd-%s' % exchange)
    else:
        logging.debug('Already have {}'.format(exchange))
        df = pd.read_csv('{}/btcusd-{}.csv'.format(DATA_FOLDER, exchange), parse_dates=True, index_col=0)
    return df


def get_altcoin_prices_from_poloniex(altcoin_code, start_date, end_date, refresh=False):
    """ Get Historical Bitcoin prices from various exchanges using Quandl API.
       Please have your Quandl Key set as config.QUANDL_KEY

       :param <altcoin_code> of type String ex. "ETH" - Altcoin whose data we want to fetch
       :param <start_date> of type Datetime ex. datetime(..) - Starting from what date
       :param <end_date> of type Datetime ex. datetime(..) - To what date
       :param <refresh> of type bool - If existing data is already saved in CSV file, do we refresh it or use it.
       :return Pandas.DataFrame
        """
    df_altcoin_prices = pd.DataFrame()  # type: DataFrame
    alt_btc_pair = 'BTC_{}'.format(altcoin_code)
    logging.info("Now fetching historical prices for Alt-Btc pair {} from Poloniex".format(alt_btc_pair))
    df_altcoin_prices = _get_json_data_from_poloniex(alt_btc_pair, (start_date - PIVOT_START_DATE).total_seconds(),
                                                     (end_date - PIVOT_START_DATE).total_seconds(), refresh=refresh)
    if df_altcoin_prices.empty:
        logging.warn("Unable to get historical prices for Alt-Btc pair {} from Poloniex".format(alt_btc_pair))
        raise IOError("Unable to get historical prices for Alt-Btc pair {} from Poloniex".format(alt_btc_pair))
    print(df_altcoin_prices.tail())
    return df_altcoin_prices


def _merge_data_frames_for_column(name_dfs, data_frames, column_name):
    series_dict = {}
    for index in range(len(data_frames)):
        print(data_frames[index].tail())
        series_dict[name_dfs[index]] = data_frames[index][column_name]
    return pd.DataFrame(series_dict)


def get_bitcoin_prices(refresh=False):
    exchange_data = {}
    for exchange in EXCHANGES:
        btc_exchange_df = get_bitcoin_prices_from_exchange(exchange, refresh=refresh)
        exchange_data[exchange] = btc_exchange_df

    btc_usd_df = _merge_data_frames_for_column(exchange_data.keys(), exchange_data.values(), 'Weighted Price')
    btc_vol_df = _merge_data_frames_for_column(exchange_data.keys(), exchange_data.values(), 'Volume (BTC)')
    btc_usd_df['Mean'] = btc_usd_df.mean(axis=1)
    btc_usd_df['Volume'] = btc_vol_df.mean(axis=1)
    # print(btc_usd_df.tail())
    # print(btc_vol_df.tail())
    return btc_usd_df


def get_altcoin_prices(refresh=False):
    altcoin_data = {}
    for altcoin in ALTCOIN_TICKERS:
        crypto_price_df = get_altcoin_prices_from_poloniex(altcoin, DATA_START_DATE, datetime.today(),
                                                           refresh=refresh)
        altcoin_data[altcoin] = crypto_price_df

    altcoin_data['ETH'].tail()
    return altcoin_data


def _get_json_data_from_poloniex(altcoin_pair, start_date, end_date, candlestick_period=CANDLESTICK_PERIOD,
                                 refresh=False):
    '''Download and cache JSON data, return as a dataframe.'''
    df = pd.DataFrame()
    if refresh or not os.path.exists('{}/{}.csv'.format(DATA_FOLDER, altcoin_pair)):
        json_url = POLONIEX_URL_FORMAT.format(altcoin_pair, start_date, end_date, candlestick_period)
        print(json_url)
        logging.info("Fetching %s prices  from Poloniex " % altcoin_pair)
        df = pd.read_json(json_url)
        if not df.empty:
            logging.info("### Successfully fetched %s prices from Poloniex exchange!!" % altcoin_pair)
            save_data(df, '%s' % altcoin_pair)
    else:
        logging.debug('Already have {}'.format(altcoin_pair))
        df = pd.read_csv('{}/{}.csv'.format(DATA_FOLDER, altcoin_pair), parse_dates=True, index_col=2)
    df.set_index('date')
    return df


def save_data(data_frame, file_name):
    if not os.path.exists(DATA_FOLDER):
        os.makedirs(DATA_FOLDER)
    data_frame.to_csv('{}/{}.csv'.format(DATA_FOLDER, file_name))
    logging.info("Save completed for {}".format(file_name))


if __name__ == '__main__':
    get_bitcoin_prices()
    get_altcoin_prices()
