import sys
import csv

from datetime import datetime

def main(argv):

    workspace = ''

    locations = ['California', 'Austin', 'New York', 'Colorado', 'online']
    # locations = ['online']

    # sharing_algorithms = ['No Sharing', 'Pure Sharing', 'FCFS', 'RR', 'LRF', 'LSTF', 'SRF', 'SSTF']
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

    grids = {
             'Solar Trader': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'No Sharing': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'Pure Sharing': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'FCFS': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'LRF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'LSTF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'RR': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'SRF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'SSTF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 }}

    savings = {
             'Solar Trader': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'Pure Sharing': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'FCFS': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'LRF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'LSTF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'RR': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'SRF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 },
             'SSTF': { 'California': 0, 'Austin': 0, 'New York': 0, 'Colorado': 0 }}
    
    for sharing_algorithm in sharing_algorithms:
        input_path = workspace + 'figures/grid/fig_data/' + sharing_algorithm + '.csv'
        with open(input_path) as input_csv_file:
            reader = csv.DictReader(input_csv_file)
            for row in reader:
                grids[sharing_algorithm][row['location']] = row[algorithm_names[sharing_algorithm]]
        input_csv_file.close()

    for sharing_algorithm in savings:
        output_path = workspace + 'figures/saving/fig_data/' + sharing_algorithm + '.csv'
        with open(output_path, 'w', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
            writer.writerow(['location', algorithm_names[sharing_algorithm]])
            for location in locations:
                if float(grids[sharing_algorithm][location]) >= 0:
                    savings[sharing_algorithm][location] = 1 - (float(grids[sharing_algorithm][location]) / float(grids['No Sharing'][location]))
                else:
                    savings[sharing_algorithm][location] = - abs(float(grids[sharing_algorithm][location]) - float(grids['No Sharing'][location])) / abs(float(grids['No Sharing'][location]))

                writer.writerow([location, savings[sharing_algorithm][location]])
        output_csv_file.close()


if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')