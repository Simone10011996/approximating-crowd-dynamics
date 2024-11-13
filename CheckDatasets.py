import pandas as pd
import datetime
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('first')
parser.add_argument('last')
args = parser.parse_args()
first = int(args.first)
last = int(args.last)

# this table has details on each city, with year, month, day, and gmt_hours
cities = pd.read_csv(f"city_details.csv", sep=";")

for n_city in range(first, last):
    city = cities.iloc[n_city]
    name = city['city']
    year = city['year']
    month = city['month']
    day = city['day']
    gmt_hours = city['gmt_hours']
    timezone = datetime.timezone(datetime.timedelta(hours=gmt_hours))

    network_nodes = pd.read_csv(f"all/{name}/network_nodes.csv", sep=";")
    tempday = pd.read_csv(f"all/{name}/network_temporal_day.csv", sep=";")

    # Verifico se l'arco avviene tra nodi diversi, altrimenti lo rimuovo
    # e poi verifico se ci sono tempi zero o negativi, e in tal caso la posizione dei nodi

    tempi_zero = {}
    negativi = []
    rows_to_drop = []
    counter = 0
    numero_zeri = 0
    nodi_uguali = 0

    for index, row in tempday.iterrows():
        u = row['from_stop_I']
        v = row['to_stop_I']
        dep_time = row['dep_time_ut']
        arr_time = row['arr_time_ut']
        time_trip = arr_time - dep_time
        trip = row['trip_I']
        
        if u == v:
            rows_to_drop.append(index)
            nodi_uguali += 1
        elif time_trip == 0:
            lat = list(network_nodes.loc[(network_nodes['stop_I'] == u) | (network_nodes['stop_I'] == v), 'lat'])
            lon = list(network_nodes.loc[(network_nodes['stop_I'] == u) | (network_nodes['stop_I'] == v), 'lon'])
            if lat[0] == lat[1] and lon[0] == lon[1]:
                print('nodi nello stesso identico punto')
            if trip in tempi_zero:
                tempi_zero[trip].append(counter)
            else:
                tempi_zero[trip] = [counter]
            numero_zeri += 1
        elif time_trip < 0:
            negativi.append(counter)
        counter += 1

    # Rimuovo le righe raccolte
    tempday.drop(index=rows_to_drop, inplace=True)

    # Resetto gli indici
    tempday.reset_index(drop=True, inplace=True)

    # print('numero di archi tra nodi uguali:   ', nodi_uguali)
    # print('numero tempi zero:   ', numero_zeri)
    # print('numero trip in cui tragitto tempo zero:  ', len(tempi_zero))
    # print('numero tragitti tempo negativo:     ', len(negativi))

    def update_dataset(dataset, sequence, trip):
        consecutive_zero = [sequence[0]]
        for i in range(1, len(sequence)):
            # finiscono i consecutivi e aggiorno il dataset
            if sequence[i] > sequence[i-1] + 1:
                for j in range(len(consecutive_zero)):
                    dataset.at[consecutive_zero[j], 'arr_time_ut'] += j + 1
                    if j >= 1:
                        dataset.at[consecutive_zero[j], 'dep_time_ut'] += j
                if consecutive_zero[-1] + 1 < len(dataset) and dataset.at[consecutive_zero[-1] + 1, 'trip_I'] == trip:
                    dataset.at[consecutive_zero[-1] + 1, 'dep_time_ut'] += j + 1
                consecutive_zero = [sequence[i]]
            elif sequence[i] == consecutive_zero[-1] + 1:
                consecutive_zero.append(sequence[i])

        # Aggiungo l'ultima sequenza
        for j in range(len(consecutive_zero)):
            dataset.at[consecutive_zero[j], 'arr_time_ut'] += j + 1
            if j >= 1:
                dataset.at[consecutive_zero[j], 'dep_time_ut'] += j
        if consecutive_zero[-1] + 1 < len(dataset) and dataset.at[consecutive_zero[-1] + 1, 'trip_I'] == trip:
            dataset.at[consecutive_zero[-1] + 1, 'dep_time_ut'] += j + 1

    # Aggiorno il dataset
    for trip in tempi_zero:
        update_dataset(tempday, tempi_zero[trip], trip)

    # Verifico che non ci siano più stessi nodi, tempi zero o negativi
    lista_zeri = []
    negativi = []
    for index, row in tempday.iterrows():
        u = row['from_stop_I']
        v = row['to_stop_I']
        dep_time = row['dep_time_ut']
        arr_time = row['arr_time_ut']
        time_trip = arr_time - dep_time
        if u == v:
            raise Exception('Same node')
        elif time_trip == 0:
            lista_zeri.append(index)
        elif time_trip < 0:
            negativi.append(index)
            
    if len(lista_zeri) > 0 or len(negativi) > 0:
        raise Exception(f'We still have some zeros or negative\nzeros: {lista_zeri}, negatives: {negativi}')

    edges_stream = tempday.sort_values('dep_time_ut')

    os.makedirs(f'new_datasets/{name}', exist_ok=True)
    network_nodes.to_csv(f'new_datasets/{name}/nodes.csv', sep=';', index=False)
    edges_stream.to_csv(f'new_datasets/{name}/edges.csv', sep=';', index=False)
