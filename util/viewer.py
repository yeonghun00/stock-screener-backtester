import os
import pandas as pd
import time

start = time.time()

path = '/Users/yeonghun/PycharmProjects/pythonProject/screen_backtester_3/data/2022/raw/AAPL/'
#path = '/Users/yeonghun/PycharmProjects/pythonProject/screen_backtester_2/data/2021_2022/raw/WBA/'
#path = '/Users/yeonghun/PycharmProjects/pythonProject/screen_backtester/formatted/price/'

path = '/Users/yeonghun/PycharmProjects/pythonProject/screen_backtester_3/data/2020/formatted/daily_fund/'
files = [f for f in os.listdir(path) if 'pb' in f]

print(files)

for file in files:
    print(file)
    df = pd.read_pickle(path + file)
    print(df.to_string())

end = time.time()
print(end-start)

