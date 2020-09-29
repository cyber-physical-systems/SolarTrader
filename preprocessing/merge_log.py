import csv

import sys
import os
import os.path as path
from datetime import datetime

import us
import pytz

import pandas as pd

DEBUG = False

def log(message):
    if DEBUG:
        print(message)

def main(argv):

    workspace = ''

    dataset = 'pecanstreet'

    location = 'newyork'
    tz = pytz.timezone('America/New_York')

    discharge_speed = '100'

    sharing_algorithms = ['FCFS', 'RR', 'SSTF', 'LSTF', 'LRF', 'SRF']

    for sharing_algorithm in sharing_algorithms:

        input_path = workspace + 'data/' + dataset + '/' + location + '/logs/' + discharge_speed + '/' + sharing_algorithm + '_Sharing_log.csv'

        output_path = workspace + 'data/' + dataset + '/' + location + '/hourly/' + sharing_algorithm + '.csv'

        print('Start merging log for ' + sharing_algorithm)

        # init csv file header
        output_csv_header = ['timestamp', 'datetime', 'grid', 'share', 'battery_remain']
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
            writer.writeheader()
        csvfile.close()

        counter = 0

        df = pd.read_csv(input_path)
        total = len(df)

        current_ts = 0

        grid = 0
        share = 0
        battery_remain = 0

        with open(output_path, 'a', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
                
            with open(input_path) as input_csv_file:
                reader = csv.DictReader(input_csv_file)
                for row in reader:
                    # Skip empty row
                    row_amount = float(row['amount'])
                    if row_amount == 0:
                        continue

                    if row_amount < 0:
                        log('Error: Negative row amount!')
                        exit()
                    
                    ts = int(float(row['timestamp']))

                    if counter == 0:
                        current_ts = ts
                    
                    counter += 1
                    # process = str(counter) + '/' + str(total) + ' (' + str(round(counter/total*100, 2)) + '%)'
                    # print(process, end='\r')

                    if ts != current_ts:

                        dt = datetime.fromtimestamp(current_ts)
                        log(dt)
                        writer.writerow([current_ts, dt, grid, share, battery_remain])

                        grid = 0
                        share = 0
                        battery_remain = 0
                        
                        current_ts = ts

                    if row['type'] == 'grid':
                        grid += row_amount
                    elif row['type'] == 'share':
                        share += row_amount
                    elif row['type'] == 'battery_remain':
                        battery_remain += row_amount

            input_csv_file.close()
        output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print(datetime.now()-start)