import csv

import sys
import os
import os.path as path
from datetime import datetime
import math
from datetime import datetime

import pandas as pd

DEBUG = False

def log(message):
    if DEBUG:
        print(message)

def main(argv):

    workspace = ''
    
    # datasets = ['15minute_data_newyork', '15minute_data_california', '15minute_data_austin']
    datasets = ['austin']

    granularity = 3600

    for dataset in datasets:

        input_path = workspace + 'data/pecanstreet/' + dataset + '/p_15minute_data_' + dataset + '.csv'

        output_path = workspace + workspace + 'data/pecanstreet/p_1hour_data_' + dataset + '.csv'

        # init csv file header
        output_csv_header = ['timestamp', 'datetime', 'use', 'gen', 'diff', 'grid', 'house_id']
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
            writer.writeheader()
        csvfile.close()

        counter = 0

        df = pd.read_csv(input_path)
        total = len(df)

        current_ts = df['timestamp'].iloc[0]

        next_ts = current_ts + granularity

        use = {}
        gen = {}
        diff = {}
        grid = {}

        df = pd.read_csv(input_path)
        total = len(df)

        with open(output_path, 'a', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
                
            with open(input_path) as input_csv_file:

                reader = csv.DictReader(input_csv_file)

                for row in reader:
                    
                    ts = int(float(row['timestamp']))

                    # if counter == 0:
                    #     current_ts = ts
                    #     next_ts = current_ts + granularity
                    
                    counter += 1
                    process = str(counter) + '/' + str(total) + '(' + str(round(counter/total*100, 2)) + '%)'
                    print(process, end='\r')

                    if ts >= next_ts:

                        dt = str(datetime.utcfromtimestamp(current_ts)) + '-05:00'
                        log(dt)
                        # writer.writerow([current_ts, dt, grid, share, battery_remain])

                        for hid in use:
                            writer.writerow([current_ts, dt, use[hid], gen[hid], diff[hid], grid[hid], hid])

                            use[hid] = 0
                            gen[hid] = 0
                            diff[hid] = 0
                            grid[hid] = 0
                        
                        current_ts = next_ts
                        next_ts = current_ts + granularity

                    

                    if row['house_id'] in use:
                        use[row['house_id']]  += float(row['use']) if row['use'] != '' else 0
                        gen[row['house_id']]  += float(row['gen']) if row['gen'] != '' else 0
                        diff[row['house_id']] += float(row['diff']) if row['diff'] != '' else 0
                        grid[row['house_id']] += float(row['grid']) if row['grid'] != '' else 0
                    else:
                        use[row['house_id']]  = float(row['use']) if row['use'] != '' else 0
                        gen[row['house_id']]  = float(row['gen']) if row['gen'] != '' else 0
                        diff[row['house_id']] = float(row['diff']) if row['diff'] != '' else 0
                        grid[row['house_id']] = float(row['grid']) if row['grid'] != '' else 0

            input_csv_file.close()
        output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print(datetime.now()-start)