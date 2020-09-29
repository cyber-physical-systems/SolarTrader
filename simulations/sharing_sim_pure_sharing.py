import csv

import sys
# import os
# import os.path as path
from datetime import datetime
# import math
import pandas as pd

# from utility import Logger as log

def init_house_list(house_info_path):    # Init house list
    house_list = []
    with open(house_info_path) as house_info_csv_file:
        reader = csv.DictReader(house_info_csv_file)
        for row in reader:
            house_list.append(row['house_id'])
    house_info_csv_file.close()
    return house_list

def check_battery(demand, balance):
    
    BATTERY_VOLUME = 0
    
    new_balance = demand + balance
    
    if demand == 0:
        request = 0
    # Extra gen
    elif demand > 0: 
        if new_balance > BATTERY_VOLUME:
            request = new_balance - BATTERY_VOLUME
            new_balance = BATTERY_VOLUME
        else:
            request = 0
    # Need power
    else:
        if new_balance < 0:
            request = new_balance
            new_balance = 0
        else:
            request = 0
    
    return new_balance, request


def check_battery(demands, balances):
    
    BATTERY_VOLUME = 30

    new_balances = []
    new_demands = []

    for i in range(0, len(demands)):

        new_balances[i] = demands[i] + balances[i]
    
        if demands[i] == 0:
            new_demands[i] = 0
        # Extra gen
        elif demands[i] > 0: 
            if new_balances[i] > BATTERY_VOLUME:
                new_demands[i] = new_balances[i] - BATTERY_VOLUME
                new_balances[i] = BATTERY_VOLUME
            else:
                new_demands[i] = 0
        # Need power
        else:
            if new_balances[i] < 0:
                new_demands[i] = new_balances[i]
                new_balances[i] = 0
            else:
                new_demands[i] = 0
    
    return new_balances, new_demands

def main(argv):

    workspace = ''

    datasets = {
        'pecanstreet': ['California', 'Austin', 'New York'],
        'eGauge':['Colorado']
    }

    

    for dataset in datasets:
        for location in datasets[dataset]:

            print(f'Start simulate {location} by pure sharing...')

            # Get house list
            house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
            house_list = init_house_list(house_info_path)

            # Init battery balances
            battery_balances = {}
            for hid in house_list:
                battery_balances[hid] = 0

            demands = {}
            requests = {}

            lenders = {}
            borrowers = {}

            grids = {}

            input_path = workspace + 'data/' + dataset + '/' + location + '/demands.csv'

            # Init output files headers
            df = pd.read_csv(input_path)
            source_header = list(df.columns.values)

            grids_output_path = workspace + 'data/' + dataset + '/' + location + '/grids/Pure Sharing.csv'

            with open(grids_output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=source_header)
                writer.writeheader()
            csvfile.close()
            
            trade_output_path = workspace + 'data/' + dataset + '/' + location + '/trade/Pure Sharing.csv'

            with open(trade_output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=source_header)
                writer.writeheader()
            csvfile.close()

            with open(grids_output_path, 'a', newline='') as grids_csv_file:
                grids_writer = csv.writer(grids_csv_file)

                with open(trade_output_path, 'a', newline='') as trade_csv_file:
                    trade_writer = csv.writer(trade_csv_file)
                    
                    with open(input_path) as input_csv_file:
                        reader = csv.DictReader(input_csv_file)
                        for row in reader:
                            for hid in house_list:
                                demands[hid] = float(row[hid])
                                battery_balances[hid], requests[hid] = check_battery(demands[hid], battery_balances[hid])
                                if requests[hid] > 0:
                                    lenders[hid] = requests[hid]
                                else:
                                    borrowers[hid] = requests[hid]
                            
                            requests_sum = sum(requests.values())

                            # More gen than use
                            if requests_sum > 0:
                                lenders_sum = sum(lenders.values())
                                sharing_rate = requests_sum / lenders_sum
                                for hid in house_list:
                                    if requests[hid] > 0:
                                        grids[hid] = requests[hid] * sharing_rate
                                    else:
                                        grids[hid] = 0
                            # More use than gen
                            elif requests_sum < 0:
                                borrowers_sum = sum(borrowers.values())
                                sharing_rate = requests_sum / borrowers_sum
                                for hid in house_list:
                                    if requests[hid] < 0:
                                        grids[hid] = requests[hid] * sharing_rate
                                    else:
                                        grids[hid] = 0
                            else: # requests_sum == 0
                                for hid in house_list:
                                    grids[hid] = 0
                            
                            row_for_grids = []
                            row_for_trade = []
                            
                            for field in source_header[0:7]:
                                row_for_grids.append(row[field])
                                row_for_trade.append(row[field])
                            
                            for hid in house_list:
                                row_for_grids.append(grids[hid])
                                row_for_trade.append([row[hid], grids[hid], battery_balances[hid]])
                            
                            grids_writer.writerow(row_for_grids)
                            trade_writer.writerow(row_for_trade)
                    input_csv_file.close()
                trade_csv_file.close()
            grids_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')