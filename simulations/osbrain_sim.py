import csv

import sys
import os
import os.path as path
from datetime import datetime
import time

import pandas as pd

from osbrain import run_agent
from osbrain import run_nameserver


def log_message(agent, message):
    agent.log_info('Received: %s' % message)

def main(argv):
    workspace = ''

    dataset = 'sundance'

    # location = 'california'
    location = ''

    discharge_speed = '100'

    # input_path = workspace + 'data/2015.csv'
    input_path = workspace + 'data/' + dataset + '/' + location + '/2015.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/logs/' + discharge_speed + '/FCFS_Sharing_log.csv'

    # # init csv file header
    # output_csv_header = ['timestamp', 'datetime', 'from', 'to', 'amount', 'type']
    # # type: share, grid, own
    # with open(output_path, 'w', newline='') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=output_csv_header)
    #     writer.writeheader()
    # csvfile.close()

    agent_names = []

    agents = {}
    channels = {}

    discharge_rate = int(discharge_speed)

    df = pd.read_csv(input_path)
    total = len(df)

    counter = 0
    current_ts = 0

    # house_info_path = workspace + 'data/metadata.csv'
    house_info_path =workspace + 'data/' + dataset + '/' + location + '/metadata.csv'

    ns = run_nameserver()

    print('Init agents and channels...')

    with open(house_info_path) as house_info_csv_file:
        reader = csv.DictReader(house_info_csv_file)
        for row in reader:
            counter += 1
            # print(row['hid'])
            agent_names.append(row['hid'])
            agents[row['hid']] = run_agent(row['hid'])
            channels[row['hid']] = agents[row['hid']].bind('PUB', alias=row['hid'])
            if counter == 10:
                break
    house_info_csv_file.close()

    print("Subscribe to channel...")

    for channel_name in agent_names:
        for agent_name in agent_names:
            if agent_name != channel_name:
                agents[agent_name].connect(channels[channel_name], handler=log_message)

    print("Start simulation...")

    # with open(output_path, 'a', newline='') as output_csv_file:
    #     writer = csv.writer(output_csv_file)
            
    with open(input_path) as input_csv_file:
        reader = csv.DictReader(input_csv_file)
        for row in reader:
            if row['house_id'] in agent_names:
                agents[row['house_id']].send(row['house_id'], row['diff'])
    input_csv_file.close()
    # output_csv_file.close()
    
    ns.shutdown()


if __name__ == '__main__':
    main(sys.argv[1:])