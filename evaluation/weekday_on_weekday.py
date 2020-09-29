import csv

import sys
import os
import os.path as path
from datetime import datetime
import math
from datetime import datetime

DEBUG = False

def log(message):
    if DEBUG == True:
        print(message)

def main(argv):

    workspace = ''

    dataset = 'pecanstreet'

    # location = 'newyork'
    # location = 'california'
    location = 'austin'

    # discharge_speed = '100'

    sharing_algorithms = ['NoSharing', 'FCFS', 'RR', 'SSTF', 'LSTF', 'LRF', 'SRF']
    # sharing_algorithms = ['origin']

    for sharing_algorithm in sharing_algorithms:

        input_path = workspace + 'data/' + dataset + '/' + location + '/hourly/' + sharing_algorithm + '.csv'

        output_path = workspace + 'data/' + dataset + '/' + location + '/wdowd/' + sharing_algorithm + '.csv'


        # init csv file header
        output_csv_header = ['weekday', 'grid_total', 'num_of_days', sharing_algorithm]
        # type: share, grid, own
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
            writer.writeheader()
        csvfile.close()

        days_amount = {}
        grid_amount = {}

        for wd in range(0, 7):
            days_amount[wd] = 0
            grid_amount[wd] = 0
                
        with open(input_path) as input_csv_file:
            reader = csv.DictReader(input_csv_file)
            for row in reader:
                ts = int(row['timestamp'])
                dt = datetime.fromtimestamp(ts)
                weekday = dt.weekday()

                days_amount[weekday] += 1
                grid_amount[weekday] += float(row['grid'])

        input_csv_file.close()

        grid_mean = {}
        with open(output_path, 'a', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
            for wd in range(0, 7):
                grid_mean[wd] = grid_amount[wd]/days_amount[wd]
                print(str(wd) + ':' + str(grid_amount[wd]) + '/' + str(days_amount[wd]) + '=' + str(grid_mean[wd]))
                writer.writerow([str(wd), abs(grid_amount[wd]), days_amount[wd], abs(grid_mean[wd])])
        output_csv_file.close()

if __name__ == "__main__":
    main(sys.argv[1:])