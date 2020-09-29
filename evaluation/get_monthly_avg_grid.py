import sys
import csv
from datetime import datetime
import pandas as pd

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
        'pecanstreet': ['California', 'Austin', 'New York'],
        # 'pecanstreet': ['New York']
        'eGauge':['Colorado', 'online']
        # 'eGauge':['online']
    }

    # sharing_algorithms = ['No Solar', 'No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF']
    sharing_algorithms = ['No Solar', 'No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF', 'Solar Trader']
    # sharing_algorithms = ['Solar Trader']

    algorithm_names = {
        'No Solar': 'Non-Solar',
        'Solar Trader': 'Solar Trader',
        'No Sharing': 'Non-Sharing',
        'Pure Sharing': 'Pure Sharing',
        'FCFS': 'FCFS', 
        'LRF': 'LRF',
        'LSTF': 'LSTF',
        'RR': 'RR',
        'SRF': 'SRF',
        'SSTF': 'SSTF'
        # 'Solar Trader': 'Solar Trader',
        # 'No Sharing': 'No Sharing',
        # 'Pure Sharing': 'Pure Sharing',
        # 'FCFS': 'First Come First Sharing', 
        # 'LRF': 'Largest Remaining First',
        # 'LSTF': 'Longest Sharing Time First',
        # 'RR': 'Round Robin',
        # 'SRF': 'Smallest Remaining First',
        # 'SSTF': 'Shortest Sharing Time First'
    }

    for sharing_algorithm in sharing_algorithms:
        columns = ['location', algorithm_names[sharing_algorithm]]
        output_df = pd.DataFrame(columns = columns)

        for dataset in datasets:
            for location in datasets[dataset]:

                # Get house number for current location
                house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
                house_list = init_house_list(house_info_path)
                house_number = len(house_list)

                input_path = workspace + 'data/' + dataset + '/' + location + '/monthly/' + sharing_algorithm + '.csv'

                df = pd.read_csv(input_path)

                # df[df > 0] = 0

                # print(df)

                means = df.iloc[:, 1:(1+house_number)].mean(axis=0)

                # print(means)

                output_df.loc[len(output_df)] = [location, -means.mean()]
        
        output_path = workspace + 'figures/grid/fig_data/' + sharing_algorithm + '.csv'
        output_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')