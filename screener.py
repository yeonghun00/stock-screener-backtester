import pandas as pd
import os
import time

# screener

conditions1 = {
    #'marketCap_1': [10 ** 8, 10 ** 12, '<', '<=', 'xxx', 'xxx'],
    'pbRatio_0': [1, 50, '<', '<=', 'xxx', 'xxx'],

}
conditions2 = {
    #'roe_1':[0.1, 100, '<', '<', 'Quarter' , 'xxx'],
    'piotroskiFScore_0':[5,10,'<=','<=','Quarter','xxx']
}

conditions3 = {
    'revenue_1': [.01, 10, '<', '<', 'Previous quarter', 'Recent quarter', ],
}


conditions = {
    'conditions1':conditions1,
    'conditions2':conditions2,
    'conditions3':conditions3
}

# Screen date
#screen_date = pd.to_datetime('2021-05-01').tz_localize("UTC")

class Screener:
    def __init__(self, conditions, screen_date):
        self.conditions1 = conditions['conditions1']
        self.conditions2 = conditions['conditions2']
        self.conditions3 = conditions['conditions3']
        self.screen_date = screen_date

        # path setting
        # 리스트로 비교 하기 필요
        batch_year = self.screen_date.strftime('%Y')
        data_path = os.getcwd() + '/data/' + batch_year + '/'
        self.path_dic = {
            'raw': data_path + 'raw/',
            'daily': data_path + 'formatted/daily_fund/',
            'quarter': data_path + 'formatted/quarter_fund/',
            'price': data_path + 'formatted/price/'
        }

        self.screened_codes = self.screen()

    # util
    def get_recent_date(self, df, date, prev=0):
        return sorted(set(df.loc[(df.index < date)].index), reverse=True)[prev]

    def get_recent_annaul_date(self, df, date, prev=0):
        t = df.loc[(df.index < date), 'quarter'] == '0'
        return sorted(set(df.loc[(df.index < date)][t].index), reverse=True)[prev]
        #return sorted(set((df.loc[mask, 'quarter'] == '0').index), reverse=True)[prev]

    '''
    def date2yq(self, df, date):
        q = df.loc[self.get_recent_date(df, date)]['quarter'][0]
        y = df.loc[self.get_recent_date(df, date)]['year'][0]
        return (int(y),int(q))

    def yq2date(self, df, y, q):
        return df[(df['year'] == str(y)) & (df['quarter'] == str(q))]'''

    def get_joined_data(self, code):
        if os.path.isdir(os.getcwd() + '/data/' + str(int(self.screen_date.strftime('%Y'))-1)):
            prev_df = pd.read_pickle(os.getcwd() + '/data/' + str(int(self.screen_date.strftime('%Y'))-1) + '/raw/' + code + '/q_fundamental.pkl')
            current_df = pd.read_pickle(self.path_dic['raw'] + code + '/q_fundamental.pkl')

            prev_df['id'] = prev_df['year'] + prev_df['quarter'] + prev_df['dataCode']
            current_df['id'] = current_df['year'] + current_df['quarter'] + current_df['dataCode']

            return pd.concat([prev_df, current_df]).drop_duplicates(subset ="id", keep = False)
        else:
            return pd.read_pickle(self.path_dic['raw'] + code + '/q_fundamental.pkl')

    def get_data(self, df, date, annual=False):
        #print(df)
        #print(date)
        t = df.loc[date]
        if annual:
            return t[t['quarter'] =='0']
        if df.loc[[date], 'quarter'].isin(['0']).any():
            return t[t['quarter'] == '4']
        else:
            return t

    # screen
    def screen1(self):
        codes = set(f for f in os.listdir(self.path_dic['raw']))
        for condition in self.conditions1:
            current_row = pd.read_pickle(self.path_dic['daily'] + condition[:condition.find('_')] + '.pkl').loc[self.screen_date]
            screened = 0
            if self.conditions1[condition][2] == '<' and self.conditions1[condition][3] == '<':
                screened = current_row.between(self.conditions1[condition][0], self.conditions1[condition][1], 'neither')
            elif self.conditions1[condition][2] == '<=' and self.conditions1[condition][3] == '<':
                screened = current_row.between(self.conditions1[condition][0], self.conditions1[condition][1], 'left')
            elif self.conditions1[condition][2] == '<' and self.conditions1[condition][3] == '<=':
                screened = current_row.between(self.conditions1[condition][0], self.conditions1[condition][1], 'right')
            elif self.conditions1[condition][2] == '<=' and self.conditions1[condition][3] == '<=':
                screened = current_row.between(self.conditions1[condition][0], self.conditions1[condition][1], 'both')
            codes = codes.intersection(set(screened[screened].index.values))
        return codes

    def screen2(self, codes):
        for condition in self.conditions2:
            current_row = 0
            if self.conditions2[condition][4] == 'Quarter':
                current_row = pd.read_pickle(self.path_dic['quarter'] + 'quarter/' + condition[:condition.find('_')] + '.pkl').loc[self.screen_date]
            if self.conditions2[condition][4] == 'Annual':
                current_row = pd.read_pickle(self.path_dic['quarter'] + 'annual/' + condition[:condition.find('_')] + '.pkl').loc[self.screen_date]
            screened = 0
            if self.conditions2[condition][2] == '<' and self.conditions2[condition][3] == '<':
                screened = current_row.between(self.conditions2[condition][0], self.conditions2[condition][1], 'neither')
            elif self.conditions2[condition][2] == '<=' and self.conditions2[condition][3] == '<':
                screened = current_row.between(self.conditions2[condition][0], self.conditions2[condition][1], 'left')
            elif self.conditions2[condition][2] == '<' and self.conditions2[condition][3] == '<=':
                screened = current_row.between(self.conditions2[condition][0], self.conditions2[condition][1], 'right')
            elif self.conditions2[condition][2] == '<=' and self.conditions2[condition][3] == '<=':
                screened = current_row.between(self.conditions2[condition][0], self.conditions2[condition][1], 'both')
            codes = codes.intersection(set(screened[screened].index.values))
        return codes

    def screen3(self, codes):
        result_code = []
        for code in codes:
            isin = True
            for condition in self.conditions3:
                screened = False
                try:
                    #df = pd.read_pickle(self.path_dic['raw'] + code + '/q_fundamental.pkl')
                    df = self.get_joined_data(code)

                    # 1. 데이터 부재 오류
                    if df.empty:
                        isin = False
                        break

                    # get date
                    recent_date = 0
                    if self.conditions3[condition][5] == 'Recent quarter':
                        recent_date = self.get_recent_date(df, self.screen_date)
                    # Todos
                    elif self.conditions3[condition][5] == 'Recent annual':
                        recent_date = self.get_recent_annaul_date(df, self.screen_date)

                    previous_date = 0
                    if self.conditions3[condition][4] == 'Previous quarter':
                        previous_date = self.get_recent_date(df, self.screen_date, 1)
                    # Todos
                    elif self.conditions3[condition][4] == 'Previous annual':
                        previous_date = self.get_recent_annaul_date(df, self.screen_date, 1)

                    elif self.conditions3[condition][4] == 'Previous year quarter':
                        previous_date = self.get_recent_annaul_date(df, self.screen_date)

                    # dataCode
                    dc = condition[:condition.find('_')] # condition[condition.find('_') + 1:]
                    data = self.get_data(df, recent_date)
                    recent_data = data[data['dataCode'] == dc]['value']

                    data = self.get_data(df, previous_date)
                    previous_data = data[data['dataCode'] == dc]['value']

                    # 0으로 나눔 무시
                    if recent_data[0] == 0 or previous_data[0] == 0:
                        isin = False
                        break
                    # nan으로 나눔 무시
                    if pd.isna(recent_data[0]) or pd.isna(previous_data[0]):
                        isin = False
                        break

                    if self.conditions3[condition][2] == '<' and self.conditions3[condition][3] == '<':
                        screened = self.conditions3[condition][0] < (recent_data[0] - previous_data[0]) / abs(recent_data[0]) < \
                                   self.conditions3[condition][1]
                    elif self.conditions3[condition][2] == '<=' and self.conditions3[condition][3] == '<':
                        screened = self.conditions3[condition][0] <= (recent_data[0] - previous_data[0]) / abs(recent_data[0]) < \
                                   self.conditions3[condition][1]
                    elif self.conditions3[condition][2] == '<' and self.conditions3[condition][3] == '<=':
                        screened = self.conditions3[condition][0] < (recent_data[0] - previous_data[0]) / abs(recent_data[0]) <= \
                                   self.conditions3[condition][1]
                    elif self.conditions3[condition][2] == '<=' and self.conditions3[condition][3] == '<=':
                        screened = self.conditions3[condition][0] <= (recent_data[0] - previous_data[0]) / abs(recent_data[0]) <= \
                                   self.conditions3[condition][1]

                except Exception as e:
                    isin = False
                    print(code)
                    print('ERRORED')
                    print(e)
                if screened == False:
                    isin = False
                    break

            if isin:
                result_code.append(code)

        return result_code

    def screen(self):
        filter1 = self.screen1()
        #print('Screen 1: ', len(filter1), filter1)
        filter2 = self.screen2(filter1)
        #print('Screen 2: ', len(filter2), filter2)
        self.screened_codes = self.screen3(filter2)
        #print('Screen 3: ', len(self.screened_codes), self.screened_codes)
        return self.screened_codes

    # 그냥 바이부터 홀드까지 붙여서 출력 todos
    # 다음 년도 홀드 오류
    def evaluate(self, hold_period = 1):
        if os.path.isdir(os.getcwd() + '/data/' + self.screen_date.strftime('%Y')) == False:
            return 0, 0, 0
        next_date = self.screen_date + pd.DateOffset(months=hold_period) - pd.DateOffset(days=1)
        next_year = int(next_date.strftime('%Y'))
        codes = self.screened_codes
        if int(self.screen_date.strftime('%Y')) == next_year:
            df = pd.read_pickle(self.path_dic['price'] + 'adjClose' + '.pkl')
        else:
            if os.path.isdir(os.getcwd() + '/data/' + str(next_year+1)):
                next_df = pd.read_pickle(os.getcwd() + '/data/' + str(next_year+1) + '/formatted/price/adjClose.pkl')
                current_df = pd.read_pickle(self.path_dic['price'] + 'adjClose' + '.pkl')
                df = pd.concat([current_df, next_df])
            else: return 0, 0, 0

        current = df.loc[self.screen_date, codes]
        next = df.loc[next_date][codes]
        r = (next / current).mean()
        total = len(current)
        increased = sum(next / current > 1.0)
        return r, total, increased

    # 스톱로스 양수로
    def evaluate2(self, hold_period = 1, stoploss = 0.05):
        codes = self.screened_codes
        df = pd.read_pickle(self.path_dic['price'] + 'adjClose' + '.pkl')
        next_date = self.screen_date + pd.DateOffset(months=hold_period)
        df = df.loc[self.screen_date:next_date, codes] / df.loc[self.screen_date:next_date, codes].loc[self.screen_date]
        print(df)

        print(df.iloc[-1])
        stopped = [i for i in (df<(1-stoploss)).any().index if (df<(1-stoploss)).any()[i]]
        print(df.iloc[-1].drop(stopped))
        print((len(stopped) * (1-stoploss) + df.iloc[-1].drop(stopped).mean())/(len(stopped)+1))

        r = df.iloc[-1].mean()
        total = len(df.columns)
        increased = sum(df.iloc[-1] > 1) / total
        return r, total, increased


#s = Screener(conditions, pd.to_datetime('2021-03-01').tz_localize("UTC"))


# 스크리너 3
# 과거/현재 데이터 조회 시 전 연도 데이터 오류 --> 전 데이터 붙이기 (get_joined)
# ㄴ 과거 데이터 부재 오류, 과거 년도 티커 없음 --> 없으면 스킵 (get_joined)
# 소수 종목들은 해당 column 부재 오류 --> 없으면 스킵
