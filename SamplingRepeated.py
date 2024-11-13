import pandas as pd
import datetime
import random
import os
import glob
import re
import argparse
from Functions import *

parser = argparse.ArgumentParser()
parser.add_argument('seed', type=int)
parser.add_argument('first')
parser.add_argument('last')
args = parser.parse_args()
seed = int(args.seed)
first = int(args.first)
last = int(args.last)

random.seed(seed)

# this table has details on each city, with year, month, day, and gmt_hours
cities = pd.read_csv(f"city_details.csv", sep=";")

for n_city in range(first, last+1):
    city = cities.iloc[n_city]
    name = city['city']
    year = city['year']
    month = city['month']
    day = city['day']
    gmt_hours = city['gmt_hours']
    timezone = datetime.timezone(datetime.timedelta(hours=gmt_hours))

    os.makedirs(f'results_samplesrep/{name}', exist_ok=True)

    # dataset with all information on nodes
    nodes_detailed = pd.read_csv(f"new_datasets/{name}/nodes.csv", sep=";")
    # list of nodes
    nodes = list(nodes_detailed['stop_I'])
    n_nodes = len(nodes)

    # edges stream, ordered by the departure time of trip
    edges = pd.read_csv(f"new_datasets/{name}/edges.csv", sep=";")

    # random extraction of K, N, k
    results_available = glob.glob(f'results/{name}/result_*.txt')
    selected = random.choice(results_available)
    K = int(re.search('K\d+', selected).group()[1:])
    N = int(re.search('N\d+', selected).group()[1:])
    samples_available = glob.glob(f'results/{name}/sample_k*_K{K}_N{N}_*.txt')
    selected = random.choice(samples_available)
    k = int(re.search('k\d+', selected).group()[1:])

    requests = pd.read_csv(f"requests/{name}/requests_K{K}_N{N}.csv", sep=";")
    partition_quests = create_partition_quests(requests)

    for M in [1, 5, 10, 50, 100]:
        for num_test in range(M):

            if not os.path.exists(f'results_samplesrep/{name}/sample_k{k}_K{K}_N{N}_{num_test+1}_M{M}.txt'):

                quest_dict = extract_sample(partition_quests, k)

                timestamp_start = datetime.datetime.now()
                occupancy_matrix, crowding_vector, waiting_time, travelling_time, total_people, number_requests = calculate_statistics(quest_dict, nodes, edges)
                timestamp_end = datetime.datetime.now()
                execution_time = timestamp_end - timestamp_start

                result_summary = f"city: {name}\nnumber of people: {total_people}\nnumber of requests: {number_requests}\nsize of the sample: {k}\nseed: {seed}\nnumber of foremost path calculated: {len(quest_dict)}\nstart: {str(timestamp_start).split('.')[0]}\nend: {str(timestamp_end).split('.')[0]}\nexecution time: {str(execution_time).split('.')[0]}\naverage waiting time: {str(waiting_time / total_people)}\naverage travelling time: {str(travelling_time / total_people)}\n\noccupancy matrix: {str(occupancy_matrix)}\n\n\ncrowding vector: {str(crowding_vector)}"

                open(f'results_samplesrep/{name}/sample_k{k}_K{K}_N{N}_{num_test+1}_M{M}.txt', 'w').write(result_summary)
                