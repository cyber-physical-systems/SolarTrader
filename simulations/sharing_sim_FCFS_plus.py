import csv

import sys
# import os
# import os.path as path
from datetime import datetime
# import math
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

def tail(list):
    if len(list) == 0:
        return 0 
    else:
        return (max(list.keys()) + 1)

def head(list):
    if len(list) == 0:
        # return 0
        print('error') 
        exit()
    else:
        return min(list.keys())

def main(argv):

    workspace = ''

    BATTERY_VOLUME = 0

    datasets = {
        'pecanstreet': ['California', 'Austin', 'New York'],
        'eGauge':['Colorado']
    }

    for dataset in datasets:
        for location in datasets[dataset]:

            print(f'Start simulate {location} by FCFS...')

            # Get house list
            house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
            house_list = init_house_list(house_info_path)

            # Init battery balances
            battery_balances = {}
            for hid in house_list:
                battery_balances[hid] = {}

            demands = {}
            requests = {}

            lenders = {}

            inter_lenders = {}
            inter_borrowers = {}

            grids = {}

            input_path = workspace + 'data/' + dataset + '/' + location + '/demands.csv'

            # Init output files headers
            df = pd.read_csv(input_path)
            source_header = list(df.columns.values)

            grids_output_path = workspace + 'data/' + dataset + '/' + location + '/grids/FCFS.csv'

            with open(grids_output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=source_header)
                writer.writeheader()
            csvfile.close()
            
            trade_output_path = workspace + 'data/' + dataset + '/' + location + '/trade/FCFS.csv'

            with open(trade_output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=source_header)
                writer.writeheader()
            csvfile.close()

            # End Init

            with open(grids_output_path, 'a', newline='') as grids_csv_file:
                grids_writer = csv.writer(grids_csv_file)

                with open(trade_output_path, 'a', newline='') as trade_csv_file:
                    trade_writer = csv.writer(trade_csv_file)
                    
                    with open(input_path) as input_csv_file:
                        reader = csv.DictReader(input_csv_file)
                        for row in reader:
                            # print(f"{row['timestamp']}")
                            for hid in house_list:
                                demands[hid] = float(row[hid])
                                '''
                                Start Process Battery
                                '''
                                balance_sum = sum(battery_balances[hid].values())
                                if sum(battery_balances[hid].values()) > BATTERY_VOLUME:
                                    print(f"ERROR: balance out of range!")
                                    print(battery_balances)
                                    exit()

                                new_balance = demands[hid] + balance_sum
                                
                                if demands[hid] == 0:
                                    requests[hid] = 0
                                # Extra gen
                                elif demands[hid] > 0:
                                    lender_id = tail(lenders)
                                    if new_balance > BATTERY_VOLUME:
                                        # print(f'here{hid}')
                                        requests[hid] = new_balance - BATTERY_VOLUME
                                        battery_balances[hid][lender_id] = BATTERY_VOLUME - balance_sum
                                        lenders[lender_id] = {'hid': hid, 'amount': (BATTERY_VOLUME - balance_sum)}
                                    else:
                                        # print(f'there{hid}')
                                        requests[hid] = 0
                                        battery_balances[hid][lender_id] = demands[hid]
                                        lenders[lender_id] = {'hid': hid, 'amount': demands[hid]}
                                # Need power
                                else: # demands[hid] < 0
                                    # Use up all battery remaining, still need more
                                    if new_balance < 0: # demands[hid](-) + balance_sum(+) < 0
                                        requests[hid] = new_balance
                                        for lender_id in battery_balances[hid]:
                                            del lenders[lender_id]
                                        battery_balances[hid] = {}
                                    # Didn't use up all battery remaining, remove used parts in battery and update lenders queue
                                    else:
                                        # No need borrow from lenders
                                        lender_id = tail(lenders)
                                        requests[hid] = 0
                                        battery_sum = 0
                                        while True:

                                            lender_id = 88888888
                                            
                                            # print(f'current{hid}')
                                            for lid in lenders:
                                                # print(lenders)
                                                # print(lenders[lid]['hid'])
                                                if lenders[lid]['hid'] == hid:
                                                    lender_id = lid
                                                    break
                                            
                                            if lender_id == 88888888:
                                                print('ERROR no lender matched!')

                                            battery_sum += battery_balances[hid][lender_id]
                                            diff = battery_sum + demands[hid]
                                            if diff == 0:
                                                del battery_balances[hid][lender_id]
                                                del lenders[lender_id]
                                                break
                                            elif diff > 0: # battery_sum(+) + demands[hid](-) > 0
                                                battery_balances[hid][lender_id] = diff
                                                lenders[lender_id] = {'hid': hid, 'amount': diff}
                                                break
                                            else: # battery_sum(+) + demands[hid](-) < 0
                                                del battery_balances[hid][lender_id]
                                                del lenders[lender_id]
                                '''
                                End Process Battery
                                '''

                                # Build inter leaders and borrower list
                                if requests[hid] > 0:
                                    inter_lenders[hid] = requests[hid]
                                else:
                                    inter_borrowers[hid] = requests[hid]
                                
                            '''
                            Start FCFS Algorithm
                            '''
                            requests_sum = sum(requests.values())
                        
                            # More gen than use
                            if requests_sum >= 0:
                                # Inter lenders fullfilled borrowers, no need to touch battery
                                
                                # Calculate for grids
                                inter_lenders_sum = sum(inter_lenders.values())
                                sharing_rate = requests_sum / inter_lenders_sum
                                # No extra inter lender power left
                                if requests_sum == 0:
                                    # No need to draw from nor feedback to grids
                                    for hid in house_list:
                                        grids[hid] = 0
                                # Still have extra power left over inter lenders
                                else: 
                                    # Feedback extra gen grids
                                    for hid in house_list:
                                        if requests[hid] > 0:
                                            grids[hid] = requests[hid] * sharing_rate
                                        else:
                                            grids[hid] = 0
                            # More use than gen, need borrow power from lenders' battery
                            elif requests_sum < 0:
                                '''
                                requests sum is extra needs for borrowers.
                                '''
                                # Total battery storage over all lenders
                                lenders_sum = 0
                                for lid in lenders:
                                    lenders_sum += lenders[lid]['amount']
                                
                                # Diff between extra needs and battery storage
                                share_diff = requests_sum + lenders_sum
                                '''
                                Compare extra needs with battery storage.
                                '''
                                # More requests than shared, all battery used up, draw from grid
                                if share_diff < 0:
                                    # Clean lenders queue and battery balances
                                    lenders = {}
                                    for hid in house_list:
                                        battery_balances[hid] = {}
                                    # Calculate for grids
                                    borrowers_sum = sum(inter_borrowers.values())
                                    sharing_rate = share_diff / borrowers_sum
                                    for hid in house_list:
                                        # Draw from grid
                                        if requests[hid] < 0:
                                            grids[hid] = requests[hid] * sharing_rate
                                        else:
                                            grids[hid] = 0
                                # More share than requests, all borrowers are fullfilled, still have extra power left in lenders' battery
                                elif share_diff > 0:
                                    # Update lenders queue and battery balances
                                    share_sum = 0
                                    while True:
                                        # Share from head of lenders queue !!!FCFS CORE!!!


                                        lender_id = head(lenders)
                                        share_sum += lenders[lender_id]['amount']

                                        diff = share_sum + requests_sum

                                        if diff == 0:
                                            # FUK this!
                                            del battery_balances[lenders[lender_id]['hid']][lender_id]
                                            del lenders[lender_id]
                                            break
                                        elif diff > 0:
                                            battery_balances[lenders[lender_id]['hid']][lender_id] = diff
                                            lenders[lender_id] = {'hid': lenders[lender_id]['hid'], 'amount': diff}
                                            break
                                        else: # share_sum(+) + requests_sum(-) < 0
                                            del battery_balances[lenders[lender_id]['hid']][lender_id]
                                            del lenders[lender_id]
                                    # Calculate for grids
                                    # No need to draw from nor feedback to grids
                                    for hid in house_list:
                                        grids[hid] = 0
                                # Use up all batteries and just fullfilled the extra needs
                                else:
                                    # Clean lenders queue and battery balances
                                    lenders = {}
                                    for hid in house_list:
                                        battery_balances[hid] = {}
                                        # Calculate for grids
                                        # No need to draw from nor feedback to grids
                                        grids[hid] = 0
                            '''
                            End FCFS Algorithm
                            '''
                        
                            row_for_grids = []
                            row_for_trade = []
                            
                            for field in source_header[0:7]:
                                row_for_grids.append(row[field])
                                row_for_trade.append(row[field])
                            
                            for hid in house_list:
                                row_for_grids.append(grids[hid])
                                row_for_trade.append([row[hid], grids[hid], sum(battery_balances[hid].values())])
                                
                            grids_writer.writerow(row_for_grids)
                            trade_writer.writerow(row_for_trade)
                            if sum(battery_balances[hid].values()) > 0:
                                print(f"ERROR! HUGE BATTERY VOLUME on {hid} with {sum(battery_balances[hid].values())} at {row['timestamp']}")
                                print(battery_balances)
                                exit()
                    input_csv_file.close()
                trade_csv_file.close()
            grids_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')