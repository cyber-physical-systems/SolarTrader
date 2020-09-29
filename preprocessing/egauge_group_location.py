import csv

import sys

from datetime import datetime

import pandas as pd

import os.path as path

def main(argv):
  
    workspace = ''

    dataset = 'eGauge'

    target_location = 'Colorado'
    target_abbr = 'CO'

    input_path = workspace + 'data/' + dataset + '/location_mapping.csv'
    output_path = workspace + 'data/' + dataset + '/' + target_location + '/metadata.csv'
    
    with open(output_path, 'w', newline='') as csvfile:
        csvfile.write('house_id,latitude,longitude,state,state_abbr,city,header\n')
    csvfile.close()
    
    with open(output_path, 'a', newline='') as output_csv_file:
        writer = csv.writer(output_csv_file)
            
        with open(input_path) as input_csv_file:
            reader = csv.reader(input_csv_file)
            for row in reader:
                house_id = row[1].split('_')[1]
                state_abbr = row[4].split('/')[0]
                city = row[4].split('/')[1]
                
                if state_abbr == target_abbr:
                    data_path = workspace + 'data/' + dataset + '/energy_data/' + house_id + '_data.csv'
                    
                    if path.exists(data_path):
                        df = pd.read_csv(data_path)
                        source_header = list(df.columns.values)
                        
                        writer.writerow([house_id, row[2], row[3], target_location, state_abbr, city, source_header])

        input_csv_file.close()
    output_csv_file.close()

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')