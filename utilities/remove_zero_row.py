import csv

import sys
import os
import os.path as path
from datetime import datetime

import pandas as pd

DEBUG = False

def log(message):
    if DEBUG:
        print(message)

def main(argv):

    workspace = ''

    dataset = 'pecanstreet'

    # location = 'newyork'
    # location = 'austin'
    location = 'california'
    # tz = pytz.timezone('America/New_York')


    input_path = workspace + 'data/' + dataset + '/' + location + '/1hour_data_california.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/p_1hour_data_california.csv'

    # init csv file header
    output_csv_header = ['timestamp', 'datetime', 'use', 'gen', 'diff', 'grid', 'house_id']
    with open(output_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
        writer.writeheader()
    csvfile.close()

    counter = 0

    df = pd.read_csv(input_path)
    total = len(df)

    with open(output_path, 'a', newline='') as output_csv_file:
        writer = csv.writer(output_csv_file)
            
        with open(input_path) as input_csv_file:
            reader = csv.DictReader(input_csv_file)
            for row in reader:
                counter += 1
                # process = str(counter) + '/' + str(total) + ' (' + str(round(counter/total*100, 2)) + '%)'
                # print(process, end='\r')
                
                if row['use'] != '0' and row['gen'] != '0' and row['diff'] != '0' and row['grid'] != '0':
                    writer.writerow([row['timestamp'], row['datetime'], row['use'], row['gen'], row['diff'], row['grid'], row['house_id']])

            input_csv_file.close()
        output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print(datetime.now()-start)