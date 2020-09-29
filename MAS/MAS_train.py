import ray
from ray import tune
from ray.tune import run_experiments
from ray.tune.registry import register_env
from ray.rllib.agents.ppo import PPOTrainer
import ray.rllib.agents.dqn as dqn
import ray.rllib.agents.ddpg as ddpg
# Import environment definition
import json
import csv
import time
from ray.rllib.agents.callbacks import DefaultCallbacks
import numpy as np
start_time = time.time()
import csv
import gym
import numpy as np

from ray.rllib.env.multi_agent_env import MultiAgentEnv

# Reward Function Coefficient
global_coefficient = {'error':50, 'diff':1,'gini':1,'var':1,'grid':1}

# Reinforcement Learning Model
# Options: PPO, APPO, PG, DDPG
rl_model = 'PPO'

AGENTS_NUMBER = 25
space_range = {
    "min": -51.246, 
    "max": 20.542
}

def init_house_list(house_info_path):  # Init house list
    house_list = []
    with open(house_info_path) as house_info_csv_file:
        reader = csv.DictReader(house_info_csv_file)
        for row in reader:
            house_list.append(row['house_id'])
    house_info_csv_file.close()
    return house_list


def gini(list_of_values):
    if min(list_of_values) < 0:
        min_value = abs(min(list_of_values))
        list_of_values = [value + min_value for value in list_of_values]
    sorted_list = sorted(list_of_values)
    # print(sorted_list)
    height, area = 0, 0
    for value in sorted_list:
        height += value
        area += height - value / 2.
    fair_area = height * len(list_of_values) / 2.
    return (fair_area - area) / fair_area

def check_battery(demand, balance):
    BATTERY_VOLUME = 0

    new_balance = demand + balance

    if demand == 0:
        request = 0
    # Extra gen
    elif demand > 0:
        if new_balance > BATTERY_VOLUME:
            request = new_balance - BATTERY_VOLUME
            new_balance = BATTERY_VOLUME
        else:
            request = 0
    # Need power
    else:
        if new_balance < 0:
            request = new_balance
            new_balance = 0
        else:
            request = 0

    return new_balance, request

class IrrigationEnv(MultiAgentEnv):
    # define the observation space and action space
    def __init__(self, return_agent_actions=False, part=False):
        self.num_agents = AGENTS_NUMBER
        self.observation_space = gym.spaces.Box(low=space_range['min'], high=space_range['max'], shape=(1,))
        self.action_space = gym.spaces.Box(low=space_range['min'], high=space_range['max'], shape=(1,))


    # reset function  return the observation, which is every agent demand
    def reset(self):
        obs = {}
        self.dones = set()
        # self.water = np.random.uniform(-20, 20)
        for i in range(self.num_agents):
            obs[i] = np.array([np.random.uniform(space_range['min'], space_range['max'])])
        self.water =np.array(obs)
        # print(self.water)
        return obs


    def cal_rewards(self, action_dict):
        global global_coefficient
        #  share price *11
        #  trade to grid *5
        #  buy from gride *20 (penalty)
        # self.gen_water = 0
        reward = 0
        obs_gen = 0
        obs_use = 0
        act_gen =0
        act_use =0
        '''
            >Cap penalty
            e.g.    observations = [10, 5, -7, 2, -13] # obs is one item in observations
                actions = [, 0, 0, ]   # act is one item in actions
            if obs > 0: # Lend
                act > 0: # remain
                    act > obs: # remain > lend ---> Error
                    act <= obs: # normal
                act == 0: # lend all # normal
                act < 0: # Error
            else obs < 0 : # Borrow
                act < 0: # Normal ----> Penalty
                    act < obs: # Error
                    act > obs: # Normal ----> Penalty
                act = 0: # Normal ----> Reward = 0
                act > 0: # Error
        '''
        # cap
        demand = self.water.tolist()
        for i in range(self.num_agents):
            if demand[i][0] > 0 :
                if action_dict[i][0] > 0 :
                    if action_dict[i][0] > demand[i][0]:
                        reward = reward - action_dict[i][0]
                if action_dict[i][0] < 0:
                    reward = reward + action_dict[i][0]

            if demand[i][0] < 0 :
                if action_dict[i][0] < 0 :
                    if action_dict[i][0] < demand[i][0]:
                        reward = reward + action_dict[i][0]
                if action_dict[i][0]:
                    reward = reward - action_dict[i][0]

        # sum reward
        '''
                 (obs+.sum - act+.sum) # lend_sum 10, 5, 15, -4, -1
                                                20, 15, 10 feedback
                   lend_sum < 0 ----> penalty
                   lend_sum >= 0 -----> reward
                (|obs-.sum| - |act-.sum|) # borrow_sum  -5, -7, -10
                    |act-.sum| ----> penalty      ?          0, 0, 0
                    borrow_sum > 0 ----> small 

                >|lend_sum - borrow_sum| ----> penalty
        '''
        for i in range(self.num_agents):
            if demand[i][0] > 0 :
                obs_gen += demand[i][0]
            elif demand[i][0] <= 0 :
                obs_use += demand[i][0]

            if action_dict[i][0] > 0:
                act_gen += action_dict[i][0]
            elif action_dict[i][0] <= 0 :
                act_use += action_dict[i][0]

        # lend_sum = abs(obs_gen - act_gen)

        if abs(obs_gen) > abs(act_gen):
            lend_sum = abs(obs_gen - act_gen)
        elif abs(obs_gen) <= abs(act_gen):
            reward = reward - global_coefficient['error'] * (abs(act_gen)-abs(obs_gen))
            lend_sum = 0

        if abs(obs_use) > abs(act_use):
            borrow_sum = abs(obs_use - act_use)
        elif abs(obs_use) <= abs(act_use):
            reward = reward - global_coefficient['error'] * (abs(act_use)-abs(obs_use))
            borrow_sum = 0

        # if (obs_gen - act_gen) < 0 :

        # reward = reward + obs_gen - act_gen
        # # penalty from grid
        # reward = reward + act_use
        
        reward = reward + global_coefficient['grid']*act_use

        reward = reward - global_coefficient['diff']*abs(lend_sum - borrow_sum)

        action_list = []
        for i in range(self.num_agents):
            action_list.append(action_dict[i][0])

        variance = np.var(action_list)
        reward = reward - global_coefficient['var']*variance

        gini_reward = gini(action_list)

        reward = reward - global_coefficient['gini']*gini_reward


        return reward


    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}

        reward = self.cal_rewards(action_dict)
        # print(action_dict)

        for i in range(self.num_agents):

            obs[i], rew[i], done[i], info[i] = np.array([self.water.tolist()[i][0]]), reward, True, {}
            # print(obs[i])

        done["__all__"] = True
        return obs, rew, done, info


# Driver code for training
# def setup_and_train():
# Create a single environment and register it
def env_creator(_):
    return IrrigationEnv()

single_env = IrrigationEnv()
env_name = "IrrigationEnv"
register_env(env_name, env_creator)

# Get environment obs, action spaces and number of agents
obs_space = single_env.observation_space
act_space = single_env.action_space
num_agents = single_env.num_agents

# Create a policy mapping
def gen_policy():
    return (None, obs_space, act_space, {})

policy_graphs = {}
for i in range(num_agents):
    policy_graphs['agent-' + str(i)] = gen_policy()

def policy_mapping_fn(agent_id):
    return 'agent-' + str(agent_id)

# Define configuration with hyperparam and training details
config = {
    "log_level": "WARN",
    "num_workers": 3,
    "num_cpus_for_driver": 1,
    "num_cpus_per_worker": 1,
    "train_batch_size": 256,
    "lr": 5e-3,
    "callbacks": DefaultCallbacks,
    "model": {"fcnet_hiddens": [8, 8]},
    "multiagent": {
        # "policies": policy_graphs,
        # "policy_mapping_fn": policy_mapping_fn,
        "policies": {
            'default_policy': (None, obs_space, act_space, {})},
        "policy_mapping_fn":
            lambda agent_id: 'default_policy',
    },
    "env": "IrrigationEnv"}

# Define experiment details
exp_name = 'my_exp_Austin_test'


exp_dict = {
    'name': exp_name,
    'run_or_experiment': rl_model,
    "stop": {
        "training_iteration": 1000
    },
    'checkpoint_freq': 100,
    "config": config,
}

# Initialize ray and run

ray.init()
tune.run(**exp_dict)
ray.shutdown()


    





