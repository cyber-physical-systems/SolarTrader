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

    # datasets = {
    #     'eGauge': ['size_of_agents/10', 'size_of_agents/50', 'size_of_agents/100']
    # }

    sharing_algorithms = ['No Solar', 'No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF', 'Solar Trader']
    # sharing_algorithms = ['Solar Trader']
    
    for sharing_algorithm in sharing_algorithms:
        for dataset in datasets:
            for location in datasets[dataset]:

                # Get house number for current location
                house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
                house_list = init_house_list(house_info_path)

                if sharing_algorithm == 'No Sharing':
                    input_path = workspace + 'data/' + dataset + '/' + location + '/demands.csv'
                else:
                    input_path = workspace + 'data/' + dataset + '/' + location + '/grids/' + sharing_algorithm + '.csv'

                output_path = workspace + 'data/' + dataset + '/' + location + '/monthly/' + sharing_algorithm + '.csv'

                df = pd.read_csv(input_path)

                df = df.groupby(['month'])[house_list].sum().reset_index()

                # print(df)
                
                df.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')