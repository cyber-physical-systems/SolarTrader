import sys
import csv
from datetime import datetime
import pandas as pd
import os

def main(argv):

    # datasets = {
    #     'pecanstreet': ['California', 'Austin', 'New York'],
    #     'eGauge':['Colorado']
    # }

    input_dir = f'data/pecanstreet/Austin/training_log/'
    # input_dir = f'data/rewards_mean/fig_data/'

    # Iterate directory of input files
    input_files_list = os.listdir(input_dir)
    for f in input_files_list:
        input_file_name = f.split('.')[0]
        print(f'Start strip {input_file_name}...')

        input_path = f'{input_dir}{f}'

        df = pd.read_csv(input_path)
        df[input_file_name] = df['episode_reward_mean']

        output_df = df[input_file_name]

        output_df.index.names = ['episode']

        output_path = f'figures/rewards_mean/fig_data/{f}'

        output_df.to_csv(output_path)

if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')