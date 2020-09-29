import csv

import sys

from datetime import datetime

import pandas as pd

import os.path as path

def main(argv):
  
    workspace = ''

    dataset = 'eGauge'

    target_location = 'size_of_agents'
    target_sizes = [10, 50, 100]

    for target_size in target_sizes:

        print(f'Start to clean up {target_size} agents data...')

        input_path = workspace + 'data/' + dataset + '/location_mapping.csv'
        output_path = f'{workspace}data/{dataset}/{target_location}/{target_size}/metadata.csv'
    
        with open(output_path, 'w', newline='') as csvfile:
            csvfile.write('house_id,latitude,longitude,state_abbr,city,header\n')
        csvfile.close()

        counter = 0
    
        with open(output_path, 'a', newline='') as output_csv_file:
            writer = csv.writer(output_csv_file)
                
            with open(input_path) as input_csv_file:
                reader = csv.reader(input_csv_file)
                for row in reader:
                    house_id = row[1].split('_')[1]
                    state_abbr = row[4].split('/')[0]
                    city = row[4].split('/')[1]
                    
                    data_path = workspace + 'data/' + dataset + '/energy_data/' + house_id + '_data.csv'
                    
                    if path.exists(data_path):
                        counter += 1
                        df = pd.read_csv(data_path)
                        source_header = list(df.columns.values)
                        
                        writer.writerow([house_id, row[2], row[3], state_abbr, city, source_header])
                    
                    if counter >= target_size:
                        break

            input_csv_file.close()
        output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')