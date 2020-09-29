import csv
import sys
from datetime import datetime
import math
import pandas as pd
from utility import Logger as log

workspace = ''

dataset = 'eGauge'

year = 2015

discharge_speed = '100'

data_years = {
    'size_of_agents/10': '2015',  # 2015 full year for eGauge dataset
    'size_of_agents/50': '2015',  # 2015 full year for eGauge dataset
    'size_of_agents/100': '2015'  # 2015 full year for eGauge dataset
}

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

def pure(location):
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

def FCFS(location):

    input_path = workspace + 'data/' + dataset + '/' + location + '/' + data_years[location] + '.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/logs/FCFS.csv'

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
                # process = str(counter) + '/' + str(total) + ' (' + str(round(counter/total*100, 2)) + '%)'
                # print(process, end='\r')
                    
                if ts != current_ts:
                    for bidx, borrower in enumerate(borrowers):
                        
                        if len(lenders) <= 0:
                            log.print('No lender is available.')
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
                                        log.print('Use own: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        lenders[idx] = lender

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'own'])

                                        discharge_rates[lender['house_id']] = 0

                                        break
                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Use own: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                
                                # Borrow amount less than own discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('own: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                    else:
                                        log.print('own: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        lenders[idx] = lender

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'own'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                            
                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                        
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
                                        log.print('Share: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        lenders[lender_index] = lender

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'share'])

                                        discharge_rates[lender['house_id']] = 0

                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Share: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                
                                # Borrow amount less than lender's discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('Share: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                    else:
                                        log.print('Share: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        lenders[lender_index] = lender

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'share'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                                lender_index += 1

                            # No lenders available, get from grid
                            else:
                                log.print('grid')
                                writer.writerow([current_ts, borrower['datetime'], 0, borrower['house_id'], borrower['diff'], 'grid'])
                                borrower['diff'] = 0
                                break

                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                            
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
                    lenders.append(row)
                # log(str(counter) + ':' + str(ts))

        input_csv_file.close()
    output_csv_file.close()

def LRF(location):
    input_path = workspace + 'data/' + dataset + '/' + location + '/' + data_years[location] + '.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/logs/LRF.csv'

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
                # process = str(counter) + '/' + str(total) + ' (' + str(round(counter/total*100, 2)) + '%)'
                # print(process, end='\r')
                    
                if ts != current_ts:
                    for bidx, borrower in enumerate(borrowers):
                        
                        if len(lenders) <= 0:
                            log.print('No lender is available.')
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
                                        log.print('Use own: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        # lenders[idx] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] < lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'own'])

                                        discharge_rates[lender['house_id']] = 0

                                        break
                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Use own: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                
                                # Borrow amount less than own discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('own: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                    else:
                                        log.print('own: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        # lenders[idx] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] < lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'own'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                            
                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                        
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

                                # lend_amount = abs(float(lenders[lender_index]['diff']))
                                # log(lender['diff'])

                                # Borrow amount greater than own discharge rate
                                if borrower['diff'] >= discharge_rates[lender['house_id']]:
                                    # Power provided by lender's battery is greater than discharge rate, then use discharge rate amount, keep rest for sharing
                                    if lender['diff'] > discharge_rates[lender['house_id']]:
                                        log.print('Share: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        # lenders[lender_index] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] < lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'share'])

                                        discharge_rates[lender['house_id']] = 0

                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Share: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                
                                # Borrow amount less than lender's discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('Share: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                    else:
                                        log.print('Share: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        # lenders[lender_index] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] < lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'share'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                                lender_index += 1

                            # No lenders available, get from grid
                            else:
                                log.print('grid')
                                writer.writerow([current_ts, borrower['datetime'], 0, borrower['house_id'], borrower['diff'], 'grid'])
                                borrower['diff'] = 0
                                break

                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                            
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

                row['diff'] = abs(float(row['diff']))
                if diff < 0:
                    borrowers.append(row)
                else:
                    lenders_len = len(lenders)
                    if lenders_len < 1:
                        lenders.append(row)
                    else:
                        for i, l in enumerate(lenders):
                            if l['diff'] < row['diff']:
                                lenders.insert(i, row)
                                break
                            else:
                                if i == (lenders_len - 1):
                                    lenders.insert(i+1, row)
                                    break
                # log(lenders)
                # log('-----------------------')
                # log(str(counter) + ':' + str(ts))

        input_csv_file.close()
    output_csv_file.close()

def RR(location):
    input_path = workspace + 'data/' + dataset + '/' + location + '/' + data_years[location] + '.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/logs/RR.csv'

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

    lender_index = 0

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
                # process = str(counter) + '/' + str(total) + ' (' + str(round(counter/total*100, 2)) + '%)'
                # print(process, end='\r')
                    
                if ts != current_ts:
                    for bidx, borrower in enumerate(borrowers):
                        
                        if len(lenders) <= 0:
                            log.print('No lender is available.')
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
                                        log.print('Use own: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        lenders[idx] = lender

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'own'])

                                        discharge_rates[lender['house_id']] = 0

                                        break
                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Use own: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                
                                # Borrow amount less than own discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('own: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'own'])
                                    else:
                                        log.print('own: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        lenders[idx] = lender

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'own'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                            
                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                        
                        # 2. Borrow from other lenders

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
                                        log.print('Share: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        lenders[lender_index] = lender

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], discharge_rates[lender['house_id']], 'share'])

                                        discharge_rates[lender['house_id']] = 0

                                    # Own battery cannot provide power greater than discharge rate, use up all and withdraw sharing
                                    else:
                                        log.print('Share: b>=d, l=<d')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower
                                        
                                        lenders.remove(lender)

                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                
                                # Borrow amount less than lender's discharge rate
                                else:

                                    if borrower['diff'] >= lender['diff']:
                                        log.print('Share: b<d, b>=l')

                                        borrower['diff'] -= lender['diff']
                                        borrowers[bidx] = borrower

                                        lenders.remove(lender)
                                        
                                        discharge_rates[lender['house_id']] -= lender['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], lender['diff'], 'share'])
                                    else:
                                        log.print('Share: b<d, b<l')

                                        lender['diff'] -= borrower['diff']
                                        lenders[lender_index] = lender

                                        discharge_rates[lender['house_id']] -= borrower['diff']

                                        writer.writerow([current_ts, borrower['datetime'], lender['house_id'], borrower['house_id'], borrower['diff'], 'share'])

                                        borrower['diff'] = 0
                                        borrowers[bidx] = borrower
                                lender_index += 1

                            # No lenders available, get from grid
                            else:
                                lender_index = 0
                                log.print('grid')
                                writer.writerow([current_ts, borrower['datetime'], 0, borrower['house_id'], borrower['diff'], 'grid'])
                                borrower['diff'] = 0
                                break

                            if borrower['diff'] == 0:
                                break
                            elif borrower['diff'] < 0:
                                log.print('Error: borrowing amount is negative!')
                            
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
                    lenders.append(row)
                # log(str(counter) + ':' + str(ts))

        input_csv_file.close()
    output_csv_file.close()

def SRF(location):
    input_path = workspace + 'data/' + dataset + '/' + location + '/' + data_years[location] + '.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/logs/SRF.csv'

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
                                        # lenders[idx] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] > lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

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
                                        # lenders[idx] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] > lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

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

                                # lend_amount = abs(float(lenders[lender_index]['diff']))
                                # log(lender['diff'])

                                # Borrow amount greater than own discharge rate
                                if borrower['diff'] >= discharge_rates[lender['house_id']]:
                                    # Power provided by lender's battery is greater than discharge rate, then use discharge rate amount, keep rest for sharing
                                    if lender['diff'] > discharge_rates[lender['house_id']]:
                                        log('Share: b>=d, l>d')

                                        borrower['diff'] -= discharge_rates[lender['house_id']]
                                        borrowers[bidx] = borrower

                                        lender['diff'] -= discharge_rates[lender['house_id']]
                                        # lenders[lender_index] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] > lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break


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
                                        # lenders[lender_index] = lender
                                        lenders.remove(lender)
                                        lenders_len = len(lenders)
                                        if lenders_len < 1:
                                            lenders.append(lender)
                                        else:
                                            for i, l in enumerate(lenders):
                                                if l['diff'] > lender['diff']:
                                                    lenders.insert(i, lender)
                                                    break
                                                else:
                                                    if i == (lenders_len - 1):
                                                        lenders.insert(i+1, lender)
                                                        break

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

                row['diff'] = abs(float(row['diff']))
                if diff < 0:
                    borrowers.append(row)
                else:
                    lenders_len = len(lenders)
                    if lenders_len < 1:
                        lenders.append(row)
                    else:
                        for i, l in enumerate(lenders):
                            if l['diff'] > row['diff']:
                                lenders.insert(i, row)
                                break
                            else:
                                if i == (lenders_len - 1):
                                    lenders.insert(i+1, row)
                                    break

        input_csv_file.close()
    output_csv_file.close()

def SSTF(location):
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

def main(argv):

    target_sizes = [10, 50, 100]

    for target_size in target_sizes:
        location = f'size_of_agents/{target_size}'

        print(f'Start simulate {target_size} by Pure Sharing...')
        pure(location)

        print(f'Start simulate {target_size} by FCFS...')
        FCFS(location)

        print(f'Start simulate {target_size} by LRF...')
        LRF(location)

        print(f'Start simulate {target_size} by RR...')
        RR(location)

        print(f'Start simulate {target_size} by SRF...')
        SRF(location)

        print(f'Start simulate {target_size} by SSTF...')
        SSTF(location)

if __name__ == "__main__":
    log.debug_off()

    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')