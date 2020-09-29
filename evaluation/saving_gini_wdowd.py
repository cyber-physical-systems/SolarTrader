import sys
import csv
from datetime import datetime
import pandas as pd
import math

def init_house_list(house_info_path):    # Init house list
    house_list = []
    with open(house_info_path) as house_info_csv_file:
        reader = csv.DictReader(house_info_csv_file)
        for row in reader:
            house_list.append(row['house_id'])
    house_info_csv_file.close()
    return house_list

def gini(row):
     # Saving rate df to list
    saving_list = row.values.tolist()
    # Remove inf and NaN
    saving_list = [v for v in saving_list if not (math.isinf(v) or math.isnan(v))]
    # Move up over x-axis
    min_saving = abs(min(saving_list))
    saving_list = [saving + min_saving for saving in saving_list]

    sorted_list = sorted(saving_list)
    # print(sorted_list)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(saving_list) / 2.
    return (fair_area - area) / fair_area

def main(argv):

    workspace = ''

    datasets = {
        'pecanstreet': ['California', 'Austin', 'New York'],
        'eGauge':['Colorado']
    }

    sharing_algorithms = ['No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF', 'Solar Trader']
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

    nosharing_means = {}
    means = {}
    saving = {}

    for sharing_algorithm in sharing_algorithms:
        columns = ['weekday', algorithm_names[sharing_algorithm]]
        output_df = pd.DataFrame(columns = columns)
        output_df['weekday'] = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
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

                output_path = workspace + 'figures/saving_gini_wdowd/fig_data/' + location + '/' + sharing_algorithm + '.csv'

                df = pd.read_csv(input_path)

                if sharing_algorithm == 'No Sharing':
                    nosharing_means[location] = -df.groupby(['weekday'])[house_list].sum().reset_index()
                    # nosharing_means[location] = -nosharing_means[location].iloc[:, 1:(1+house_number)]
                else:
                    means[location] = -df.groupby(['weekday'])[house_list].sum().reset_index()
                    # means[location] = -means[location].iloc[:, 1:(1+house_number)]
                    
                    saving[location] = 1 - means[location].iloc[:, 1:(1+house_number)]/nosharing_means[location].iloc[:, 1:(1+house_number)]
                    output_df[algorithm_names[sharing_algorithm]] = saving[location].apply(gini, axis=1)
                    
                    # print(nosharing_means[location])
                    # print(means[location])
                    # print(saving[location])
                    # print(output_df)
                
                    output_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')