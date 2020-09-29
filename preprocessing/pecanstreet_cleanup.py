import sys
import os
import os.path as path
import time
import calendar
from datetime import datetime
import math
# import numpy as np
import pandas as pd

def main(argv):

    workspace = ''

    # datasets = ['1minute_data_newyork', '1minute_data_california', '1minute_data_austin']
    datasets = ['1minute_data_california']

    # Iterate of input datasets
    for dataset in datasets:

        input_dir = workspace + 'data/pecanstreet/' + dataset + '/'

        output_dir = workspace + 'data/pecanstreet/'

        input_file_name = dataset + '.csv'

        print('Start processing ' + input_file_name + '...')
        
        input_path = input_dir + input_file_name

        df = pd.read_csv(input_path)
        
        cols = list(df.columns.values)

        df = pd.read_csv(input_path, parse_dates=[cols[1]]) 

        # df['datetime'] =  df.iloc[:, 1].str.replace('-05', '')
        # df['timestamp'] = df.iloc[:, 1].apply(lambda x:time.mktime(x.timetuple()))
        df['timestamp'] = df.iloc[:, 1].apply(lambda x:calendar.timegm(x.timetuple()))
        
        # df['timestamp'] = df['datetime'].apply(lambda x:time.mktime(datetime.strptime(x, '%Y-%m-%d %H:%M:%S').timetuple()))

        df['use'] = df.iloc[:, 2:31].sum(axis=1) + df.iloc[:, 32:67].sum(axis=1) + df.iloc[:, 69:77].sum(axis=1)

        df['gen'] = df.iloc[:, 67:69].sum(axis=1)

        df['diff'] = df['gen'] - df['use']

        df['grid'] = df.iloc[:, 31]

        # df['house_id'] = df.iloc[:, 0]

        # df = df[['datetime'] + ['use'] + ['gen'] + ['diff'] + ['grid'] + [cols[0]]]
        df = df[['timestamp'] + [cols[1]] + ['use'] + ['gen'] + ['diff'] + ['grid'] + [cols[0]]]

        df = df.sort_values(by='timestamp', ascending=True)

        # df.columns = ['datetime', 'use', 'gen', 'diff', 'grid', 'house_id']
        df.columns = ['timestamp', 'datetime', 'use', 'gen', 'diff', 'grid', 'house_id']

        # id = input_file_name.split('_')[1].split('.')[0]
        # output_path = output_dir + id + '.csv'
        output_path = output_dir + 'p_' + input_file_name
        
        df.to_csv(output_path, index=False)

        # break

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print(datetime.now()-start)