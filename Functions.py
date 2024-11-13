import math
import pandas as pd
import random

def foremost_path(x, min_interval, max_interval, nodes, edges_stream):
    # given the starting node, departure time, max time of arrival, list of nodes and edges stream (sorted by departure time),
    # returns the foremost path (dictionary with keys the nodes and values a list of tuple with details on trip)
    t = {}
    for node in nodes:
        if node == x:
            t[x] = [(None, None, None, min_interval)]
        else:
            t[node] = [(None, None, None, math.inf)]

    for edge in edges_stream.itertuples(index=False):
        u = edge.from_stop_I
        v = edge.to_stop_I
        dep_time = edge.dep_time_ut
        arr_time = edge.arr_time_ut

        if arr_time <= max_interval and dep_time >= t[u][-1][3]:
            if arr_time < t[v][-1][3]:
                if u != x:
                    t[v] = t[u][:] + [edge]
                else:
                    t[v] = [edge]
    
        elif dep_time >= max_interval:
            return t

    return t

def create_dict_quest(requests):
    # given the requests in a pandas dataframe object and returns a dictionary where the key is the string "'starting_node'-'starting_time'" and the value is a dictionary {node_arrival: # equal requests}
    # this because to calculate foremost path I need only departure node and departure time, all destinations are calculated, so they are grouped as values of that key
    quest_dict = {}
    for row in requests.itertuples(index=False):
        dep = row.departure
        arr = row.arrival
        time = row.starting_time
        n_people = row.n_people
        key = str(dep) + '-' + str(time)
        if key in quest_dict:
            if arr in quest_dict[key]:
                quest_dict[key][arr] += n_people
            else:
                quest_dict[key][arr] = n_people
        else:
            quest_dict[key] = {arr: n_people}
    return quest_dict

def fill_occupancy_matrix(occupancy_matrix, node, from_time, to_time, n_people):
    # function to fill occupancy matrix
    # dictionary where the key is the node and the value is a list of tuples composed of (from time in ut, to time in ut, n people)
    # note: nodes without people waiting (in all the day) are not present
    if node not in occupancy_matrix:
        occupancy_matrix[node] = {(from_time, to_time): n_people}
    elif (from_time, to_time) not in occupancy_matrix[node]:
        occupancy_matrix[node][(from_time, to_time)] = n_people
    else:
        occupancy_matrix[node][(from_time, to_time)] += n_people 
    return occupancy_matrix

def fill_crowding_vector(crowding_vector, from_node, to_node, duration_trip, trip, n_people):
    # function to fill crowding vector
    # dictionary where the key is the trip (specific bus, tram, etc) and the value is a dictionary where: the key is the route of the bus from a node to another (string 'from_node-to_node'), the value is number of people on the bus
    # note: trip or bus without people are not present 
    from_node_to_node = f'{str(from_node)}-{str(to_node)}_{duration_trip}'
    if trip not in crowding_vector:
        crowding_vector[trip] = {from_node_to_node: n_people}
    elif from_node_to_node not in crowding_vector[trip]:
        crowding_vector[trip][from_node_to_node] = n_people
    else:
        crowding_vector[trip][from_node_to_node] += n_people
    return crowding_vector

def calculate_statistics(quest_dict, nodes, edges):

    occupancy_matrix = {}
    crowding_vector = {}
    waiting_time = 0
    travelling_time = 0
    total_people = 0
    number_requests = 0

    for quest in quest_dict:
        node, time = quest.split('-')
        try:
            node = int(node)
        except:
            pass
        time = int(time)
        t = foremost_path(node, time, math.inf, nodes, edges)
        for arrival in quest_dict[quest]:
            n_people = quest_dict[quest][arrival]
            journey = t[arrival]
            if journey[0][0]:
                # if a path does not exist, the item 0,0 is None
                total_people += n_people
                number_requests += 1
                # this first step calculates waiting time from starting time of the request and dep time of the first bus
                waiting_time += (journey[0][2] - time + 1) * n_people
                occupancy_matrix = fill_occupancy_matrix(occupancy_matrix, node, time, journey[0][2], n_people)
                for j in range(len(journey)):
                    # last iteration is not counted and if next iteration there is a change of trip
                    if (j != len(journey)-1) and (journey[j][5] != journey[j+1][5]):
                        # fill occupancy matrix with actual arr node, actual arr time, next dep time
                        waiting_time += (journey[j+1][2] - journey[j][3] + 1) * n_people
                        occupancy_matrix = fill_occupancy_matrix(occupancy_matrix, journey[j][1], journey[j][3], journey[j+1][2], n_people)
                        
                    if (j != len(journey)-1) and (journey[j][5] == journey[j+1][5]):
                        bus_stopped = journey[j+1][2] - journey[j][3] + 1
                    else:
                        bus_stopped = 0
                    duration_trip = journey[j][3] - journey[j][2] - 1
                    travelling_time += (duration_trip + bus_stopped) * n_people
                    # fill crowding vector with dep node, arr node and trip
                    crowding_vector = fill_crowding_vector(crowding_vector, journey[j][0], journey[j][1], duration_trip, journey[j][5], n_people)
    occupancy_matrix = create_intervals(occupancy_matrix)
    return occupancy_matrix, crowding_vector, waiting_time, travelling_time, total_people, number_requests

def create_intervals(occupancy_matrix):
    for stop in occupancy_matrix:
        occupancy_stop = {}
        for interval in occupancy_matrix[stop]:
            for time in range(interval[0], interval[1]+1):
                if time not in occupancy_stop:
                    occupancy_stop[time] = occupancy_matrix[stop][interval]
                else:
                    occupancy_stop[time] += occupancy_matrix[stop][interval]
        occupancy_range_stop = {}
        min_time = min(occupancy_stop.keys())
        max_time = max(occupancy_stop.keys())
        lower_bound = min_time
        in_range = True
        for i in range(min_time+1, max_time+1):
            if i not in occupancy_stop and in_range:
                occupancy_range_stop[(lower_bound, i-1)] = occupancy_stop[lower_bound]
                in_range = False
            elif i in occupancy_stop and i-1 in occupancy_stop and occupancy_stop[i] != occupancy_stop[i-1]:
                occupancy_range_stop[(lower_bound, i-1)] = occupancy_stop[lower_bound]
                lower_bound = i
            elif i in occupancy_stop and not in_range:
                lower_bound = i
                in_range = True
        occupancy_range_stop[(lower_bound, max_time)] = occupancy_stop[lower_bound]
        occupancy_matrix[stop] = occupancy_range_stop
    return occupancy_matrix

def create_partition_quests(requests):
    partition_quests = {}
    total = 0
    for quest in requests.itertuples(index=False):
        total += quest.n_people
    for quest in requests.itertuples(index=False):
        departure, arrival, starting_time, n_people = quest
        key = departure, arrival, starting_time
        if key not in partition_quests:
            partition_quests[key] = n_people / total
        else:
            partition_quests[key] += n_people / total
    old_value = 0
    for quest in partition_quests:
        partition_quests[quest] = old_value, partition_quests[quest] + old_value
        old_value = partition_quests[quest][1]
    return partition_quests

def extract_sample(partition_quests, k):
    rows = []
    for i in range(k):
        prob = random.random()
        for quest in partition_quests:
            if prob >= partition_quests[quest][0] and prob < partition_quests[quest][1]:
                rows.append({'departure': quest[0], 'arrival': quest[1], 'starting_time': quest[2], 'n_people': 1})
    sample = pd.DataFrame(rows)
    dict_sample = create_dict_quest(sample)
    return dict_sample