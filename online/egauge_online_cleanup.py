import csv
import sys
import os
import os.path as path
import time
from datetime import datetime
import math
# import numpy as np
import pandas as pd

def init_house_list(house_info_path):    # Init house list
    house_list = []
    with open(house_info_path) as house_info_csv_file:
        reader = csv.DictReader(house_info_csv_file)
        for row in reader:
            house_list.append(row['house_id'])
    house_info_csv_file.close()
    return house_list

def main(argv):

    workspace = ''

    dataset = 'eGauge'

    # location = 'Colorado'
    location = 'online'

    input_dir = workspace + 'data/' + dataset + '/online/energy_data/'

    output_dir = workspace + 'data/' + dataset + '/' + location + '/cleanup/'

    # Init house list
    house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
    house_list = init_house_list(house_info_path)

    # Iterate input
    for house_id in house_list:

        input_file_name = house_id + '_data.csv'

        print('Start processing ' + input_file_name + '...')
        
        input_path = input_dir + input_file_name

        df = pd.read_csv(input_path)
        
        cols = list(df.columns.values)

        df = pd.read_csv(input_path, parse_dates=[cols[0]]) 

        df['timestamp'] = df.iloc[:, 0].apply(lambda x:time.mktime(x.timetuple()))
        df['timestamp'] = df['timestamp'] - df['timestamp'] % 3600

        df['diff'] = df.iloc[:, 2] - df.iloc[:, 1]

        df = df[['timestamp'] + [cols[0]] + cols[1:3] + ['diff']]

        df = df.sort_values(by=cols[0], ascending=True)

        df.columns = ['timestamp', 'datetime', 'use', 'gen', 'diff']

        # Trim data for 2015
        # start_ts = 1420088400
        # end_ts = 1451620800

        # Trim data for 2020
        start_ts = 1577854800
        end_ts = 1596848400


        df = df.loc[ (df['timestamp'] >= start_ts) & (df['timestamp'] <= end_ts) ]

        output_path = output_dir + house_id + '.csv'
        
        df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')