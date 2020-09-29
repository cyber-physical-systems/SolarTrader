import csv
import sys
import os
import os.path as path
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

    dataset = 'eGauge'

    # location = 'Colorado'
    location = 'online'

    # year = '2015'
    year = '2020'

    input_dir = workspace + 'data/' + dataset + '/' + location + '/cleanup/'

    output_dir = workspace + 'data/' + dataset + '/' + location + '/'

    counter = 0

    # Init house list
    house_info_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'
    house_list = init_house_list(house_info_path)

    # Iterate input
    for house_id in house_list:

        input_file_name = house_id + '.csv'

        print('Start processing ' + input_file_name + '...')
        
        input_path = input_dir + input_file_name

        if counter == 0:
            mdf = pd.read_csv(input_path)
            
            mdf['house_id'] = house_id

            print(mdf.shape)

        else:
            df = pd.read_csv(input_path)

            df['house_id'] = house_id

            mdf = pd.concat([mdf, df], axis=0)

            mdf = mdf.sort_values(by='timestamp', ascending=True)

        counter += 1

        # if counter == 3:
        #     break

    output_path = output_dir + year + '.csv'

    mdf = mdf[['timestamp', 'datetime', 'use', 'gen', 'diff', 'house_id']]

    print(str(counter) + ' houses are processed.')
    
    mdf.to_csv(output_path, index=False)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')