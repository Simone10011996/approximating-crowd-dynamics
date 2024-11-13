import pandas as pd
import random
import os
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('seed', type=int)
args = parser.parse_args()
seed = int(args.seed)

random.seed(seed)

def create_requests(city, K, N, nodes_list, time_minimum, time_maximum):
    filename = f'requests/{city}/requests_K{K}_N{N}.csv'
    if not os.path.exists(filename):
        
        quests = {}
        while len(quests) < K:
            start, arr = random.sample(nodes_list, 2)
            time = random.randint(time_minimum, time_maximum)
            quests[(start, arr, time)] = 1

        if N//K > 1:
            random_numbers = random.choices(range(1, N//K), k=K)
        else:
            random_numbers = [1]*K
        scalar = N / sum(random_numbers)
        i = 0
        while sum(random_numbers) < N and i < K:
            random_numbers[i] = round(random_numbers[i] * scalar)
            i += 1
        random_numbers[i-1] += N - sum(random_numbers)
        if sum(random_numbers) != N:
            raise Exception('The sum of random numbers is different from N')
        
        rows = []
        j = 0
        for i in quests:
            rows.append({'departure': i[0], 'arrival': i[1], 'starting_time': i[2], 'n_people': random_numbers[j]})
            j += 1

        requests = pd.DataFrame(rows)        
        requests.to_csv(filename, sep=';', index=False)



cities = pd.read_csv(f"city_details.csv", sep=";").iloc[:25]

for city in cities.itertuples(index=False):
    name, year, month, day, gmt_hours = city
    timezone = datetime.timezone(datetime.timedelta(hours=gmt_hours))

    if not os.path.exists(f'requests/{name}'):
        os.makedirs(f'requests/{name}')

    nodes = pd.read_csv(f"new_datasets/{name}/nodes.csv", sep=";")
    nodes_list = list(nodes['stop_I'])
    n_nodes = len(nodes_list)
    edges = pd.read_csv(f"new_datasets/{name}/edges.csv", sep=";")

    # I remove 30 minutes from the first bus that goes
    # time_minimum = int(datetime.datetime.timestamp(datetime.datetime(year, month, day, 0, 0)))
    time_minimum = edges.loc[0]['dep_time_ut'] - 1800
    time_maximum = (time_minimum + max(edges['dep_time_ut'])) // 2

    create_requests(name, n_nodes, n_nodes*10, nodes_list, time_minimum, time_maximum)
    create_requests(name, n_nodes, n_nodes*100, nodes_list, time_minimum, time_maximum)
    create_requests(name, n_nodes*10, n_nodes*10, nodes_list, time_minimum, time_maximum)
    create_requests(name, n_nodes*10, n_nodes*100, nodes_list, time_minimum, time_maximum)
