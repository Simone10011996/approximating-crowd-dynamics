import pandas as pd
import datetime
import pytz

## Choose the date
date = 20240603
city_details = pd.read_csv('city_details.csv', sep=';')
city_details.loc[25, 'year'] = int(str(date)[:4])
city_details.loc[25, 'month'] = int(str(date)[4:6])
city_details.loc[25, 'day'] = int(str(date)[6:8])
city_details.to_csv('city_details.csv', sep=';', index=False)

def convert_time(time, date):
    year = str(date)[:4]
    month = str(date)[4:6]
    day = int(str(date)[6:8])
    h, m, s = time.split(':')
    if h >= '24':
        h = str(int(h) - 24)
        time = ':'.join([h, m, s])
        time = datetime.datetime.strptime(f'{year}-{month}-{day+1} {time}', '%Y-%m-%d %H:%M:%S')
    else:
        time = ':'.join([h, m, s])
        time = datetime.datetime.strptime(f'{year}-{month}-{day} {time}', '%Y-%m-%d %H:%M:%S')
    time = pytz.timezone('Europe/Rome').localize(time)
    time = int(time.timestamp())
    return time
    
bus = '48-urbanoareametropolitanafiorentina'
tram = 'gest'
rows = []
edges_list = []

for type_of_route in [bus, tram]:

    calendar_dates = pd.read_csv(f'{type_of_route}/calendar_dates.txt')
    trips = pd.read_csv(f'{type_of_route}/trips.txt')
    routes = pd.read_csv(f'{type_of_route}/routes.txt')
    stops = pd.read_csv(f'{type_of_route}/stops.txt')
    stop_times = pd.read_csv(f'{type_of_route}/stop_times.txt')
    
    # Creo dataset per i nodi
    for i in stops.itertuples(index=False):
        stop_id, stop_name, stop_lat, stop_lon, stop_code = i
        rows.append({'stop_I': stop_id, 'lat': stop_lat, 'lon': stop_lon, 'name': stop_name})
    
    
    # Creo dataset per gli archi
    services_to_exclude = calendar_dates.loc[calendar_dates['date'] == date, 'service_id']
    trips_to_exclude = trips.loc[trips['service_id'].isin(services_to_exclude), 'trip_id']
    stop_times_filtered = stop_times[~stop_times['trip_id'].isin(trips_to_exclude)]
    merged_df = pd.merge(trips, routes, on='route_id', how='inner')
    trip_route_mapping = merged_df.set_index('trip_id')[['route_id', 'route_type']]
    
    for row in stop_times_filtered.itertuples(index=False):
        trip_id, arr_time, dep_time, stop_id, seq, dist = row
        route_id = trip_route_mapping.at[trip_id, 'route_id']
        route_type = trip_route_mapping.at[trip_id, 'route_type']
        arr_time = convert_time(arr_time, date)
        dep_time = convert_time(dep_time, date)
        if seq != 1:
            edges_list[-1]['to_stop_I'] = stop_id
            edges_list[-1]['arr_time_ut'] = arr_time
        edges_list.append({
            'from_stop_I': stop_id,
            'dep_time_ut': dep_time,
            'route_type': route_type,
            'trip_I': trip_id,
            'seq': seq,
            'route_I': route_id,
            'to_stop_I': None,
            'arr_time_ut': None
        })

nodes = pd.DataFrame(rows)
nodes.to_csv('all/florence/network_nodes.csv', sep=';', index=False)
    
edges = pd.DataFrame(edges_list)
edges = edges.dropna(subset=['to_stop_I'])
edges['dep_time_ut'] = edges['dep_time_ut'].astype(int)
edges['arr_time_ut'] = edges['arr_time_ut'].astype(int)
new_column_order = ['from_stop_I', 'to_stop_I', 'dep_time_ut', 'arr_time_ut', 'route_type', 'trip_I', 'seq', 'route_I']
edges = edges[new_column_order]
edges.to_csv('all/florence/network_temporal_day.csv', sep=';', index=False)