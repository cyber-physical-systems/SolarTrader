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

    sharing_algorithms = ['No Solar', 'No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF', 'Solar Trader']
    # sharing_algorithms = ['No Sharing', 'Solar Trader']

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

                output_path = workspace + 'figures/grid_hoh/fig_data/' + location + '/' + sharing_algorithm + '.csv'

                df = pd.read_csv(input_path)

                df = df.groupby(['hour'])[house_list].sum().reset_index()

                df['mean'] = -df.iloc[:, 1:(1+house_number)].mean(axis=1)

                # means = -df.iloc[:, 1:(1+house_number)].mean(axis=0)

                # print(means)

                # print(df)

                # exit()
                
                # columns = ['location', algorithm_names[sharing_algorithm]]
                # output_df = pd.DataFrame(columns = columns)
                output_df = df[['hour', 'mean']]
                # output_df.rename(columns={'mean': algorithm_names[sharing_algorithm]})
                output_df.columns=['mean', algorithm_names[sharing_algorithm]]
                
                output_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')