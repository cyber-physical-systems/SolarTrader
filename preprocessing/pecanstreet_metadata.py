import csv

import sys
import os
import os.path as path
from datetime import datetime
import math
from datetime import datetime

import pandas as pd

DEBUG = False

def log(message):
    if DEBUG:
        print(message)

def main(argv):
    workspace = ''

    dataset = 'pecanstreet'

    # location = 'california'
    # location = 'austin'
    location = 'newyork'

    # input_path = workspace + 'data/' + dataset + '/' + location + '/p_1hour_data_' + location + '.csv'
    input_name = '15minute_data_' + location
    input_path = workspace + 'data/' + dataset + '/' + location + '/' + input_name + '/' + input_name + '.csv'

    output_path = workspace + 'data/' + dataset + '/' + location + '/metadata.csv'

    house_list = []

    with open(input_path) as input_csv_file:
        reader = csv.DictReader(input_csv_file)
        for row in reader:
            if row['dataid'] not in house_list:
                house_list.append(row['dataid'])
    input_csv_file.close()

    print(house_list)
    # sprint(len(house_list))

    # init csv file header
    with open(output_path, 'w', newline='') as csvfile:
        csvfile.write('house_id,active_record,building_type,city,state,egauge_1min_min_time,egauge_1min_max_time,egauge_1min_data_availability,egauge_1s_min_time,egauge_1s_max_time,egauge_1s_data_availability,air1,air2,air3,airwindowunit1,aquarium1,bathroom1,bathroom2,bedroom1,bedroom2,bedroom3,bedroom4,bedroom5,battery1,car1,car2,circpump1,clotheswasher1,clotheswasher_dryg1,diningroom1,diningroom2,dishwasher1,disposal1,drye1,dryg1,freezer1,furnace1,furnace2,garage1,garage2,grid,heater1,heater2,heater3,housefan1,icemaker1,jacuzzi1,kitchen1,kitchen2,kitchenapp1,kitchenapp2,lights_plugs1,lights_plugs2,lights_plugs3,lights_plugs4,lights_plugs5,lights_plugs6,livingroom1,livingroom2,microwave1,office1,outsidelights_plugs1,outsidelights_plugs2,oven1,oven2,pool1,pool2,poollight1,poolpump1,pump1,range1,refrigerator1,refrigerator2,security1,sewerpump1,shed1,solar,solar2,sprinkler1,sumppump1,utilityroom1,venthood1,waterheater1,waterheater2,winecooler1,wellpump1,water_ert_min_time,water_ert_max_time,water_data_availability,gas_ert_min_time,gas_ert_max_time,gas_data_availability,indoor_temp_min_time,indoor_temp_max_time,indoor_temp_data_availability,date_enrolled,date_withdrawn,house_construction_year,total_square_footage,first_floor_square_footage,second_floor_square_footage,third_floor_square_footage,half_floor_square_footage,lower_level_square_footage,pv,pv_panel_direction,total_amount_of_pv,amount_of_south_facing_pv,amount_of_west_facing_pv,amount_of_east_facing_pv,number_of_nests,audit_2011,audit_2013_2014,survey_2011,survey_2012,survey_2013,survey_2014,survey_2017,survey_2019,program_579,program_baseline,program_energy_internet_demo,program_lg_appliance,program_verizon,program_ccet_group,program_civita_group,program_shines')
    csvfile.close()

    counter = 0

    origin_metadata_path = workspace + 'data/' + dataset + '/' + location + '/1minute_data_' + location + '/metadata.csv'
    
    with open(output_path, 'a', newline='') as output_csv_file:
        writer = csv.writer(output_csv_file)
            
        with open(origin_metadata_path) as input_csv_file:
            reader = csv.reader(input_csv_file)
            for row in reader:
                if row[0] in house_list:
                    # print(row)
                    counter += 1
                    writer.writerow(row)

        input_csv_file.close()
    output_csv_file.close()
    
    print(counter, '/', len(house_list))

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in", datetime.now()-start, '.')