import pandas as pd
import dill
import os
from tiingo import TiingoClient
from multiprocessing import Pool
import requests
import zipfile
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta

api = 'xxxxxxxxxxxxxxxxxxx'
client = TiingoClient({'api_key': api})

class Download:
    def __init__(self, start_date, end_date):
        # settings
        self.start_date = start_date
        self.end_date = end_date

        # directory settings
        self.data_path = os.getcwd() + '/data/'
        if not os.path.exists(self.data_path):
            os.makedirs(self.data_path)
            print('data directory made')

        self.path = self.data_path + self.start_date[:4] + '/raw/'

        # date margin
        self.start_date = (datetime.strptime(self.start_date, '%Y/%m/%d') - relativedelta(months=4)).strftime('%Y/%m/%d')

    # get tickers
    def get_tiingo_supported(self, s, e):
        # date setting
        delta_year = relativedelta(years=1)
        start_date = (datetime.strptime(e, "%Y/%m/%d") - delta_year).strftime("%Y/%m/%d")
        end_date = (datetime.strptime(s, "%Y/%m/%d") + delta_year).strftime("%Y/%m/%d")

        # tiingo support tickers
        url = 'https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip'
        r = requests.get(url)
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        df = pd.read_csv(zf.open(zf.namelist()[0]))
        df = df[(df['exchange'] == 'NASDAQ') | (df['exchange'] == 'NYSE')]
        df = df[df['assetType'] == 'Stock']
        df = df[df['ticker'].str.contains("-") == False]
        # drop fifth identifier
        # https://www.investopedia.com/ask/answers/06/nasdaqfifthletter.asp
        mask = (df['ticker'].str.len() < 5)
        df = df.loc[mask]
        df['startDate'] = pd.to_datetime(df['startDate'])
        df['endDate'] = pd.to_datetime(df['endDate'])
        if start_date != '':
            mask = (df['startDate'] <= pd.to_datetime(start_date))
            df = df.loc[mask]
        if end_date != '':
            mask = (df['endDate'] >= pd.to_datetime(end_date))
            df = df.loc[mask]
        df.dropna(inplace=True)
        return df

    def get_fundamentals(self, ticker):
        f = client.get_fundamentals_statements(ticker,
                                               startDate=self.start_date,
                                               endDate=self.end_date,
                                               fmt='csv',
                                               asReported=False)
        df = pd.DataFrame(f.splitlines())
        df = df[0].str.split(',', expand=True)
        df.columns = df.iloc[0]
        df.drop(df.index[0], inplace=True)
        df.set_index(pd.DatetimeIndex(df['date']).tz_localize("UTC"), inplace=True)
        df.drop(columns=['date'], inplace=True)
        df.sort_index(inplace=True)
        df['value'] = df['value'].astype('float32')
        return df

    def download(self, ticker):
        try:
            # quarter fundamentals
            df = self.get_fundamentals(ticker)
            if df.empty:
                print('empty: ', ticker)
                return
            os.makedirs(self.path + ticker)
            with open(self.path + ticker + '/q_fundamental.pkl', "wb") as dill_file:
                dill.dump(df, dill_file)

            # meta
            d = client.get_ticker_metadata(ticker)
            df = pd.DataFrame.from_dict(d, orient='index')
            with open(self.path + ticker + '/meta.pkl', "wb") as dill_file:
                dill.dump(df, dill_file)

            # price
            df = client.get_dataframe(ticker, startDate=self.start_date, endDate=self.end_date)
            with open(self.path + ticker + '/price.pkl', "wb") as dill_file:
                dill.dump(df, dill_file)

            # daily fundamentals
            df = pd.DataFrame(client.get_fundamentals_daily(ticker, startDate=self.start_date, endDate=self.end_date))
            df.to_pickle(self.path + ticker + '/d_fundamental.pkl')
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')
            with open(self.path + ticker + '/d_fundamental.pkl', "wb") as dill_file:
                dill.dump(df, dill_file)

        except Exception as e:
            print('error: ', ticker)
            print(e)

tickers = ['AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 'INTC', 'JNJ', 'KO', 'JPM',
       'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 'UNH', 'CRM', 'VZ', 'V', 'WBA', 'WMT', 'DIS', 'DOW']

if __name__ == '__main__':
    '''
    print('DOWNLOAD')
    s = '2019/01/01'
    e = '2023/01/01'
    dates = pd.date_range(s, e, freq='Ys').format(formatter=lambda x: x.strftime('%Y/%m/%d'))
    for i in range(len(dates)-1):
        d = Download(dates[:-1][i], dates[1:][i])
        #tickers = d.get_tiingo_supported(s,e)['ticker']
        with Pool(30) as pool:
            pool.map(d.download, tickers)
        print(dates[:-1][i], dates[1:][i])

    print('FINSHED')
'''
