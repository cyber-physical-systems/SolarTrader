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

def gini(list_of_values):
    sorted_list = sorted(list_of_values)
    # print(sorted_list)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(list_of_values) / 2.
    return (fair_area - area) / fair_area

def main(argv):

    workspace = ''

    datasets = {
        'eGauge/size_of_agents':['10', '50', '100']
    }

    sharing_algorithms = ['No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF', 'Solar Trader']

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
                
                if sharing_algorithm == 'No Sharing':
                    nosharing_means[location] = -df.iloc[:, 1:(1+house_number)].mean(axis=0)
                else:
                    means[location] = -df.iloc[:, 1:(1+house_number)].mean(axis=0)
                    

                    # Calculate saving rate
                    saving = 1 - (means[location]/nosharing_means[location])
                    # Saving rate df to list
                    saving_list = saving.values.tolist()
                    # Remove inf and NaN
                    saving_list = [v for v in saving_list if not (math.isinf(v) or math.isnan(v))]
                    # Move up over x-axis
                    min_saving = abs(min(saving_list))
                    saving_list = [s + min_saving for s in saving_list] 
                    # if sharing_algorithm == 'Pure Sharing':
                    #     print(nosharing_means[location])
                    #     print(means[location])
                    #     print(saving)
                    #     print(saving_list)

                    gc = gini(saving_list)
                    
                    output_df.loc[len(output_df)] = [location, gc]
        if sharing_algorithm != 'No Sharing':
            output_path = workspace + 'figures/diff_size_gini_saving/fig_data/' + sharing_algorithm + '.csv'
            output_df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')