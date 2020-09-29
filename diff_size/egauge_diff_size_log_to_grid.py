import csv

import sys
import os
import os.path as path
from datetime import datetime
import math

import pandas as pd

from utility import Logger as log

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

    sharing_algorithms = ['FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF']
    # sharing_algorithms = ['LRF', 'LSTF', 'SRF', 'SSTF']

    datasets = {
        'eGauge': ['size_of_agents/10', 'size_of_agents/50', 'size_of_agents/100']
    }

    for sharing_algorithm in sharing_algorithms:
        for dataset in datasets:
            for location in datasets[dataset]:

                start_ts = 1420088400
                end_ts = 1451620800

                # Init house list 
                house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
                house_list = init_house_list(house_info_path)

                input_path = workspace + 'data/' + dataset + '/' + location + '/logs/' + sharing_algorithm + '.csv'

                # init csv file header
                output_csv_header = ['timestamp', 'datetime', 'year', 'month', 'day', 'weekday', 'hour']
                for hid in house_list:
                    output_csv_header.append(hid)
                
                grids_output_path = workspace + 'data/' + dataset + '/' + location + '/grids/' + sharing_algorithm + '.csv'

                with open(grids_output_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
                    writer.writeheader()
                csvfile.close()
                
                trade_output_path = workspace + 'data/' + dataset + '/' + location + '/trade/' + sharing_algorithm + '.csv'

                with open(trade_output_path, 'w', newline='') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
                    writer.writeheader()
                csvfile.close()

                agents = {}
                for hid in house_list:
                    agents[hid] = 0

                df = pd.read_csv(input_path)
                total = len(df)

                counter = 0
                current_ts = start_ts

                with open(grids_output_path, 'a', newline='') as grids_csv_file:
                    grids_writer = csv.writer(grids_csv_file)

                    with open(trade_output_path, 'a', newline='') as trade_csv_file:
                        trade_writer = csv.writer(trade_csv_file)
            
                        with open(input_path) as input_csv_file:
                            reader = csv.DictReader(input_csv_file)
                            for row in reader:
                                
                                # Get timestamp
                                ts = int(float(row['timestamp']))
                
                                # Increase counter
                                counter += 1
                                # log.show_process(counter, total)

                                if ts != current_ts or counter == total:
                                    if counter == total:
                                         if row['type'] == 'grid':
                                            agents[row['to']] -= row_amount
                                    if dataset == 'pecanstreet':
                                        dt = datetime.utcfromtimestamp(current_ts)
                                    elif dataset == 'eGauge':
                                        dt = datetime.fromtimestamp(current_ts)
                                    else:
                                        print('ERROR, unkown dataset!')
                                        exit()
                                    
                                    year = dt.year
                                    month = dt.month
                                    day = dt.day
                                    weekday = dt.isoweekday()
                                    hour = dt.hour
                                    
                                    row_to_write = [current_ts, str(dt), year, month, day, weekday, hour]
                                    for hid in house_list:
                                        row_to_write.append(agents[hid])
                                    grids_writer.writerow(row_to_write)
                                    
                                    for hid in house_list:
                                        agents[hid] = 0
                                    current_ts = current_ts + 3600
                                
                                row_amount = float(row['amount'])

                                if row['type'] == 'grid':
                                    agents[row['to']] -= row_amount
                        
                        input_csv_file.close()
                trade_csv_file.close()
            grids_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')