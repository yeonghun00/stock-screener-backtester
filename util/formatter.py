import os
import pandas as pd
from dateutil.relativedelta import relativedelta

class Format:
    def __init__(self, year='2020'):
        # settings
        self.year = year

        # ffill이 미래까지 채워버림 유의
        self.start_date = pd.to_datetime(self.year+'/01/01')
        self.end_date = pd.to_datetime(str(int(self.year)+1)+'/01/01')
        self.margined_start = self.start_date - relativedelta(months=4)

        # path setting
        self.data_path = os.getcwd() + '/data/' + self.year + '/'
        self.path_dic = {
            'raw': self.data_path + 'raw/',
            'daily': self.data_path + 'formatted/daily_fund/',
            'quarter': self.data_path + 'formatted/quarter_fund/',
            'price': self.data_path + 'formatted/price/'
        }
        self.files = [f for f in os.listdir(self.path_dic['raw']) if os.path.isdir(os.path.join(self.path_dic['raw'], f))]
        for i in self.path_dic:
            if not os.path.exists(self.path_dic[i]):
                os.makedirs(self.path_dic[i])

        print('Year: ' + year)
        self.get_price(self.start_date, self.end_date)
        print('Price complete')
        self.get_daily_fund(self.start_date, self.end_date)
        print('Daily complete')
        self.get_quarter_fund(self.start_date, self.end_date)
        print('Quarter complete')
        self.reset_raw(self.start_date, self.end_date)
        print('Raw complete')

    # get price
    def get_price(self, start, end):
        dfs_li = []
        columns_li = []
        dates = pd.date_range(start, end).tz_localize('UTC')
        margined_dates = pd.date_range(self.margined_start, end).tz_localize('UTC')
        df_files = [f + '/price.pkl' for f in self.files]

        for file in df_files:
            code_name = file[:file.find('/')]
            current_df = pd.read_pickle(self.path_dic['raw'] + file)
            if current_df.empty:
                continue
            dfs_li.append(current_df['adjClose'])
            columns_li.append(code_name)

        df = pd.concat(dfs_li, keys=columns_li, axis=1)
        df = pd.DataFrame(df, index=margined_dates)
        df = df.fillna(method="ffill")
        df = pd.DataFrame(df, index=dates)
        df.to_pickle(self.path_dic['price'] + 'adjClose' + '.pkl')

    # get daily
    def get_daily_fund(self, start, end):
        dates = pd.date_range(start, end).tz_localize('UTC')
        margined_dates = pd.date_range(self.margined_start, end).tz_localize('UTC')
        df_files = [f + '/d_fundamental.pkl' for f in self.files]
        aapl = pd.read_pickle(self.path_dic['raw'] + 'AAPL/d_fundamental.pkl')
        columns = aapl.loc[:, aapl.columns != 'date']
        columns_dic = {}

        for file in df_files:
            try:
                code_name = file[:file.find('/')]
                current_df = pd.read_pickle(self.path_dic['raw'] + file)
                if current_df.empty:
                    continue
                for column in columns:
                    if column not in columns_dic:
                        columns_dic[column] = {}
                    columns_dic[column][code_name] = current_df[column]
            except Exception as e:
                print('ERROR: ', file)
                print(e)

        for column in columns_dic:
            df = pd.concat(columns_dic[column].values(), keys=columns_dic[column].keys(), axis=1)
            df = pd.DataFrame(df, index=margined_dates)
            df = df.fillna(method="ffill")
            df = pd.DataFrame(df, index=dates)
            df.to_pickle(self.path_dic['daily'] + column + '.pkl')


    def get_quarter_fund(self, start, end):
        dates = pd.date_range(start, end).tz_localize('UTC')
        margined_dates = pd.date_range(self.margined_start, end).tz_localize('UTC')
        df_files = [f + '/q_fundamental.pkl' for f in self.files]
        columns_dic = {}
        for file in df_files:
            df = pd.read_pickle(self.path_dic['raw'] + file)
            if df.empty:
                continue
            code_name = file[:file.find('/')]
            columns = list(set(df['dataCode'].values))

            for column in columns:
                if column + '_quarter' not in columns_dic: columns_dic[column + '_quarter'] = {}
                if column + '_annual' not in columns_dic: columns_dic[column + '_annual'] = {}

                # quarter
                current_df = df.loc[df['quarter'] != '0']
                if not current_df.empty:
                    current_df = current_df.loc[current_df['dataCode'] == column]['value']
                    if current_df.index.duplicated().any() == True:
                        current_df = current_df.groupby(current_df.index).first()

                    columns_dic[column + '_quarter'][code_name] = current_df

                # annaul
                current_df = df.loc[df['quarter'] == '0']
                if not current_df.empty:
                    current_df = current_df.loc[current_df['dataCode'] == column]['value']
                    if current_df.index.duplicated().any() == True:
                        current_df = current_df.groupby(current_df.index).first()
                    columns_dic[column + '_annual'][code_name] = current_df

        for column in columns_dic:
            df = pd.concat(columns_dic[column].values(), keys=columns_dic[column].keys(), axis=1)
            df = pd.DataFrame(df, index=margined_dates)
            df = df.fillna(method="ffill")
            df = pd.DataFrame(df, index=dates)

            if 'quarter' in column:
                if not os.path.exists(self.path_dic['quarter'] + 'quarter/'):
                    os.makedirs(self.path_dic['quarter'] + 'quarter/')
                df.to_pickle(self.path_dic['quarter'] + 'quarter/' + column[:column.find('_')] + '.pkl')
            elif 'annual' in column:
                if not os.path.exists(self.path_dic['quarter'] + 'annual/'):
                    os.makedirs(self.path_dic['quarter'] + 'annual/')
                df.to_pickle(self.path_dic['quarter'] + 'annual/' + column[:column.find('_')] + '.pkl')

    def reset_raw(self, start, end):
        dates = pd.date_range(start, end).tz_localize('UTC')
        margined_dates = pd.date_range(self.margined_start, end).tz_localize('UTC')
        for folder in os.listdir(self.path_dic['raw']):

            # d_fund
            df = pd.read_pickle(self.path_dic['raw'] + folder + '/d_fundamental.pkl')
            df = pd.DataFrame(df, index=margined_dates)
            df = df.fillna(method="ffill")
            df = pd.DataFrame(df, index=dates)
            df.to_pickle(self.path_dic['raw'] + folder + '/d_fundamental.pkl')

            # q_fund
            df = pd.read_pickle(self.path_dic['raw'] + folder + '/q_fundamental.pkl')
            mask = (df.index >= dates[0]) & (df.index <= dates[-1])
            df = df.loc[mask]
            df.to_pickle(self.path_dic['raw'] + folder + '/q_fundamental.pkl')

            # price
            df = pd.read_pickle(self.path_dic['raw'] + folder + '/price.pkl')
            df = pd.DataFrame(df, index=margined_dates)
            df = df.fillna(method="ffill")
            df = pd.DataFrame(df, index=dates)
            df.to_pickle(self.path_dic['raw'] + folder + '/price.pkl')

if __name__ == '__main__':
    '''
    for i in range(2022,2024):
        f = Format(str(i))
        print('complete', i)

    print('Complete')'''