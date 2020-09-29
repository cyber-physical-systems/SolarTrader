import csv

import sys
import os
import os.path as path
from datetime import datetime
import math

import pandas as pd

from utility import Logger as log

DEBUG = False

def log(message):
    if DEBUG:
        print(message)

def main(argv):

    workspace = ''

    # datasets = {
    #     'pecanstreet': ['California', 'Austin', 'New York'],
    #     'eGauge':['Colorado']
    # }

    datasets = {
        'pecanstreet': ['California'],
        'eGauge':['Colorado']
    }

    data_years = {
        'California': '2015', # 2015 full year for PecanStreet California dataset UTC
        'Austin': '2018', # 2018 full year for PecanStreet Austin dataset UTC
        'New York': '2019', # 2019/5/1-10/31 half year for PecanStreet New York dataset UTC
        'Colorado': '2015'  # 2015 full year for eGauge dataset
    }

    discharge_speed = '100'
    
    for dataset in datasets:
        for location in datasets[dataset]:

            print(f'Start simulate {location} by SSTF...')

            input_path = workspace + 'data/' + dataset + '/' + location + '/' + data_years[location] + '.csv'

            output_path = workspace + 'data/' + dataset + '/' + location + '/logs/SSTF.csv'

            # init csv file header
            output_csv_header = ['timestamp', 'datetime', 'from', 'to', 'amount', 'type']
            # type: share, grid, own
            with open(output_path, 'w', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
                writer.writeheader()
            csvfile.close()

            lenders = []
            borrowers = []

            discharge_rate = int(discharge_speed)

            df = pd.read_csv(input_path)
            total = len(df)

            counter = 0
            current_ts = 0

            # house_info_path = workspace + 'data/metadata.csv'
            house_info_path =workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
            discharge_rates = {}
            

            # Init charge rate list
            with open(house_info_path) as house_info_csv_file:
                reader = csv.DictReader(house_info_csv_file)
                for row in reader:
                    discharge_rates[row['house_id']] = discharge_rate
            house_info_csv_file.close()

            with open(output_path, 'a', newline='') as output_csv_file:
                writer = csv.writer(output_csv_file)
                    
                with open(input_path) as input_csv_file:
                    reader = csv.DictReader(input_csv_file)
                    for row in reader:
                        # Skip empty row
                        diff = float(row['diff'])
                        if diff == 0:
                            continue
                        
                        # Get timestamp
                        ts = int(float(row['timestamp']))

                        # Init current timestamp at beginning
                        if counter == 0:
                            current_ts = ts
                        
                        # Increase counter
                        counter += 1
                        # process = str(counter) + '/' + str(total) + '(' + str(round(counter/total*100, 2)) + '%)'
                        # print(process, end='\r')
                            
                        if ts != current_ts:
                            for bidx, borrower in enumerate(borrowers):
                                
                                if len(lenders) <= 0:
                                    log('No lender is available.')
                                    break
                                
                                # 1st. Check if can use own battery power first
                                for idx, lender in enumerate(lenders):

                                    if lender['house_id'] == borrower['house_id']:
                                        
                                        if discharge_rates[lender['house_id']] <= 0:
                                            continue

                                        # Borrow amount greater than own discharge rate
                                        if borrower['diff'] >= discharge_rates[lender['house_id']]:
                                            # Power provided by own battery is greater than discharge rate, then use discharge rate amount, keep rest for sharing
                                            if lender['diff'] > discharge_rates[lender['house_id']]:
                                                log('Use own: b>=d, l>d')

                                                borrower['diff'] -= discharge_rates[lender['house_id']]
                                                borrowers[bidx] = borrower

                                                lender['diff'] -= discharge_rates[lender['house_id']]
                                                lenders[idx] = lender

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'own'])

                                                discharge_rates[lender['house_id']] = 0

                                                break
                                            # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                            else:
                                                log('Use own: b>=d, l=<d')

                                                borrower['diff'] -= lender['diff']
                                                borrowers[bidx] = borrower
                                                
                                                lenders.remove(lender)

                                                discharge_rates[lender['house_id']] -= lender['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                        
                                        # Borrow amount less than own discharge rate
                                        else:

                                            if borrower['diff'] >= lender['diff']:
                                                log('own: b<d, b>=l')

                                                borrower['diff'] -= lender['diff']
                                                borrowers[bidx] = borrower

                                                lenders.remove(lender)
                                                
                                                discharge_rates[lender['house_id']] -= lender['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                            else:
                                                log('own: b<d, b<l')

                                                lender['diff'] -= borrower['diff']
                                                lenders[idx] = lender

                                                discharge_rates[lender['house_id']] -= borrower['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'own'])

                                                borrower['diff'] = 0
                                                borrowers[bidx] = borrower
                                    
                                    if borrower['diff'] == 0:
                                        break
                                    elif borrower['diff'] < 0:
                                        log('Error: borrowing amount is negative!')
                                
                                # 2. Borrow from other lenders

                                # if len(lenders) < 1:
                                #     log('No lender is available.')
                                #     break
                                
                                lender_index = 0

                                while borrower['diff'] > 0:

                                    # log(borrower['diff'])
                                    # log(str(lender_index) + '/' + str(len(lenders)))

                                    if lender_index < len(lenders):

                                        lender = lenders[lender_index]


                                        if discharge_rates[lender['house_id']] <= 0:
                                            lender_index += 1
                                            continue
                                        
                                        if lender['house_id'] == borrower['house_id']:
                                            lender_index += 1
                                            continue

                                        lend_amount = abs(float(lenders[lender_index]['diff']))
                                        # log(lender['diff'])

                                        # Borrow amount greater than own discharge rate
                                        if borrower['diff'] >= discharge_rates[lender['house_id']]:
                                            # Power provided by lender's battery is greater than discharge rate, then use discharge rate amount, keep rest for sharing
                                            if lender['diff'] > discharge_rates[lender['house_id']]:
                                                log('Share: b>=d, l>d')

                                                borrower['diff'] -= discharge_rates[lender['house_id']]
                                                borrowers[bidx] = borrower

                                                lender['diff'] -= discharge_rates[lender['house_id']]
                                                lenders[lender_index] = lender

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'share'])

                                                discharge_rates[lender['house_id']] = 0

                                            # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                            else:
                                                log('Share: b>=d, l=<d')

                                                borrower['diff'] -= lender['diff']
                                                borrowers[bidx] = borrower
                                                
                                                lenders.remove(lender)

                                                discharge_rates[lender['house_id']] -= lender['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                        
                                        # Borrow amount less than lender's discharge rate
                                        else:

                                            if borrower['diff'] >= lender['diff']:
                                                log('Share: b<d, b>=l')

                                                borrower['diff'] -= lender['diff']
                                                borrowers[bidx] = borrower

                                                lenders.remove(lender)
                                                
                                                discharge_rates[lender['house_id']] -= lender['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                            else:
                                                log('Share: b<d, b<l')

                                                lender['diff'] -= borrower['diff']
                                                lenders[lender_index] = lender

                                                discharge_rates[lender['house_id']] -= borrower['diff']

                                                writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'share'])

                                                borrower['diff'] = 0
                                                borrowers[bidx] = borrower
                                        lender_index += 1

                                    # No lenders available, get from grid
                                    else:
                                        log('grid')
                                        writer.writerow([current_ts, borrower['datetime'], 0, borrower['house_id'], borrower['diff'], 'grid'])
                                        borrower['diff'] = 0
                                        break

                                    if borrower['diff'] == 0:
                                        break
                                    elif borrower['diff'] < 0:
                                        log('Error: borrowing amount is negative!')
                                    
                            # Reset dicharge rate list
                            for dr in discharge_rates:
                                discharge_rates[dr] = discharge_rate

                            # Reset borrowers list
                            borrowers = []

                            # Sum up power left in batteries
                            battery_remain = 0
                            
                            for l in lenders:
                                battery_remain += l['diff']

                            if battery_remain > 0:
                                dt = datetime.fromtimestamp(current_ts)
                                writer.writerow([current_ts, dt, '', '', battery_remain, 'battery_remain'])
                            
                            current_ts = ts

                        row['diff'] = abs(diff)
                        if diff < 0:
                            borrowers.append(row)
                        else:
                            lenders.insert(0, row)
                        # log(str(counter) + ':' + str(ts))

                input_csv_file.close()
            output_csv_file.close()

if __name__ == "__main__":
    # log.debug_off()

    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')