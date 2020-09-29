import sys
from datetime import datetime

import math
import numpy as np
import pandas as pd

def mean_absolute_percentage_error(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return np.mean(np.abs((y_true - y_pred) / y_true)) * 100

def main(argv):
    groundtruth_path = 'California_test_copy.csv'
    prediction_path = 'California_test_predict_12.csv'

    groundtruth_df = pd.read_csv(groundtruth_path)
    groundtruth_df = groundtruth_df.iloc[:, 7:18]
    
    prediction_df = pd.read_csv(prediction_path)

    agents_list = prediction_df.columns.tolist()
    
    MAPE = {}
    
    for agent in agents_list:
        agent_groundtruth = groundtruth_df[agent].tolist()
        agent_prediction = prediction_df[agent].tolist()
        MAPE[agent] = mean_absolute_percentage_error(agent_groundtruth, agent_prediction)
    
    MAPE_list = list(MAPE.values())

    MAPE_list = [v for v in MAPE_list if not (math.isinf(v) or math.isnan(v))]

    MAPE_df = pd.DataFrame(MAPE_list)

    MAPE_mean = MAPE_df.mean()

    print(MAPE_mean)


if __name__ == "__main__":
    start = datetime.now()
    main(sys.argv[1:])
    print("Finished in ", datetime.now()-start, '.')