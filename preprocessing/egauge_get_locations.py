import csv

import sys

from datetime import datetime

def main(argv):
  
    workspace = ''

    dataset = 'eGauge'

    input_path = workspace + 'data/' + dataset + '/location_mapping.csv'
    
    state_list = {}
    city_list = {}
    
    # with open(output_path, 'a', newline='') as output_csv_file:
    #     writer = csv.writer(output_csv_file)
            
    with open(input_path) as input_csv_file:
        reader = csv.reader(input_csv_file)
        for row in reader:
            state = row[4].split('/')[0]
            city = row[4].split('/')[1]

            if state not in state_list:
                state_list[state] = 1
            else:
                state_list[state] += 1
            
            if city not in city_list:
                city_list[city] = 1
            else:
                city_list[city] += 1

    input_csv_file.close()

    print(state_list)
    print(city_list)

    # output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')