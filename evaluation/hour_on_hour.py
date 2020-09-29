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

    for sharing_algorithm in sharing_algorithms:

        input_path = workspace + 'data/' + dataset + '/' + location + '/hourly/' + sharing_algorithm + '.csv'

        output_path = workspace + 'data/' + dataset + '/' + location + '/hoh/' + sharing_algorithm + '.csv'

        # init csv file header
        output_csv_header = ['hour', 'grid_total', 'num_of_days', sharing_algorithm]
        # type: share, grid, own
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
            writer.writeheader()
        csvfile.close()

        days_amount = {}
        grid_amount = {}

        for h in range(0, 24):
            days_amount[h] = 0
            grid_amount[h] = 0
                
        with open(input_path) as input_csv_file:
            reader = csv.DictReader(input_csv_file)
            for row in reader:
                ts = int(row['timestamp'])
                dt = datetime.fromtimestamp(ts)
                hour = dt.time().hour

                days_amount[hour] += 1
                grid_amount[hour] += float(row['grid'])

        input_csv_file.close()

        grid_mean = {}
        with open(output_path, 'a', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
            for h in range(0, 24):
                if days_amount[h] == 0:
                    print(sharing_algorithm)
                    print(h)
                grid_mean[h] = grid_amount[h]/days_amount[h]
                print(str(h) + ':' + str(grid_amount[h]) + '/' + str(days_amount[h]) + '=' + str(grid_mean[h]))
                writer.writerow([str(h), abs(grid_amount[h]), days_amount[h], abs(grid_mean[h])])
        output_csv_file.close()

if __name__ == "__main__":
    main(sys.argv[1:])