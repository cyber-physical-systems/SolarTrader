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

    datasets = {
        'eGauge': ['size_of_agents']
    }

    target_sizes = [10, 50, 100]

    for dataset in datasets:
        for location in datasets[dataset]:

            # 2015 full year for eGauge dataset

            start_ts = 1420088400
            end_ts = 1451620800

            for target_size in target_sizes:

                print(f'Start to clean up {target_size} agents data...')

                # Init house list
                house_info_path = f'{workspace}data/{dataset}/{location}/{target_size}/metadata.csv'
                house_list = init_house_list(house_info_path)

                input_path = f'{workspace}data/{dataset}/{location}/{target_size}/2015.csv'

                output_path = f'{workspace}data/{dataset}/{location}/{target_size}/grids/No Solar.csv'

                # init csv file header
                output_csv_header = ['timestamp', 'datetime', 'year', 'month', 'day', 'weekday', 'hour']
                for hid in house_list:
                    output_csv_header.append(hid)

                with open(output_path, 'w', newline='') as csvfile:
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

                with open(output_path, 'a', newline='') as output_csv_file:
                    writer = csv.writer(output_csv_file)
                        
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
                                    agents[row['house_id']] = -float(row['use'])
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
                                row_to_write = []
                                for hid in house_list:
                                    row_to_write.append(agents[hid])
                                row_to_write = [current_ts, str(dt), year, month, day, weekday, hour] + row_to_write
                                writer.writerow(row_to_write)
                                
                                for hid in house_list:
                                    agents[hid] = 0
                                current_ts = current_ts + 3600

                            # if abs(float(row['diff'])) > 1000:
                            #     log.print('outlayer appear!' + row['diff'])
                            agents[row['house_id']] = -float(row['use'])

                    input_csv_file.close()
                output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')