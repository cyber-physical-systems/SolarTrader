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
        'eGauge':['Colorado']
    }

    sharing_algorithms = ['No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF']

    algorithm_names = {
        'No Sharing': 'No Sharing',
        'Pure Sharing': 'Pure Sharing',
        'FCFS': 'First Come First Sharing', 
        'LRF': 'Largest Remaining First',
        'LSTF': 'Longest Sharing Time First',
        'RR': 'Round Robin',
        'SRF': 'Smallest Remaining First',
        'SSTF': 'Shortest Sharing Time First'
    }

    nosharing_means = {}
    
    for sharing_algorithm in sharing_algorithms:
        for dataset in datasets:
            for location in datasets[dataset]:

                # Get house number for current location
                house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
                house_list = init_house_list(house_info_path)
                house_number = len(house_list)

                if sharing_algorithm == 'No Sharing':
                    input_path = workspace + 'data/' + dataset + '/' + location + '/demands.csv'
                else:
                    input_path = workspace + 'data/' + dataset + '/' + location + '/grids/' + sharing_algorithm + '.csv'

                output_path = workspace + 'figures/saving_hoh/fig_data/' + location + '/' + sharing_algorithm + '.csv'

                df = pd.read_csv(input_path)

                df = df.groupby(['hour'])[house_list].sum().reset_index()
                
                if sharing_algorithm == 'No Sharing':
                    nosharing_means[location] = -df.iloc[:, 1:(1+house_number)].mean(axis=1)
                else:
                    df['mean'] = -df.iloc[:, 1:(1+house_number)].mean(axis=1)

                    df['saving'] = 1 - (df['mean']/nosharing_means[location])

                    output_df = df[['hour', 'saving']]

                    output_df.columns=['saving', algorithm_names[sharing_algorithm]]
                    
                    output_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')