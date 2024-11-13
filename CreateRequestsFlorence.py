import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import random
import argparse
from CreateDatasetsFlorence import convert_time, date

parser = argparse.ArgumentParser()
parser.add_argument('seed', type=int)
args = parser.parse_args()

random.seed(args.seed)

def convert_hour(orario, date):
    # Nei dati sul pendolarismo, sono disponibili 4 fasce d'orario: ho scelto di convertire la fascia in un orario specifico.
    # Altrimenti si può estrarre uniformemente su tutto l'intervallo, o selezionare 3/4 orari all'interno della fascia.
    orario = int(orario)
    if orario == 1:
        return convert_time('07:00:00', date)
    elif orario == 2:
        return convert_time('07:45:00', date)
    elif orario == 3:
        return convert_time('08:45:00', date)
    elif orario == 4:
        return convert_time('09:30:00', date)
    

nodes = pd.read_csv('new_datasets/florence/nodes.csv', sep=';')

# Apro shapefile delle zone
shapefile_path = '210707_CMFI_PUMS_Modello/Zonizzazione/210205_Zonizzazione_Attuale_zone.SHP'
zones_gdf = gpd.read_file(shapefile_path)

# Creo df_zone_stop che mi associa le fermate alle zone
zone_stop = []

# Controllo se è necessaria la conversione delle coordinate
if not zones_gdf.crs.is_geographic:
    zones_gdf = zones_gdf.to_crs('EPSG:4326')

# Geodataframe delle zone
for zone in zones_gdf.itertuples(index=False):
    zone_id = zone.NO
    cod_com = zone.PROCOM

    # Geodataframe delle fermate
    for stop in nodes.itertuples(index=False):
        stop_id, lat, lon, name = stop
        point = Point(lon, lat)
        
        if zone.geometry.contains(point):
            zone_stop.append({'zone_id': zone_id, 'cod_com': cod_com, 'stop_id': stop_id})

df_zone_stop = pd.DataFrame(zone_stop)

# Apro file TPL e creo df_zone_to_zone che mi indica il numero di persone che si spostano da zona a zona
testo = open('210707_CMFI_PUMS_Modello/Matrici/03_TPL_Attuale.mtx', 'r').read()
data = []
for line in testo.splitlines():
    elements = line.split()
    if len(elements) == 3:
        try:
            a, b, c = int(elements[0]), int(elements[1]), float(elements[2])
            data.append({'from_zone': a, 'to_zone': b, 'mean_of_people': c})
        except:
            pass
df_zone_to_zone = pd.DataFrame(data)

# Apro file del pendolarismo
file = 'MATRICE PENDOLARISMO 2011/matrix_pendo2011_10112014.txt'
data = open(file, 'r').readlines()
pendo = pd.DataFrame([stringa.split() for stringa in data], columns=['tipo_rec', 'tipo_res', 'prov_res', 'com_res', 'sesso', 'motivo_spost', 'luogo_lav', 'prov_lav', 'com_lav', 'stato_lav', 'mezzo', 'orario_usc', 'tempo_imp', 'stima_num', 'num_individui'])
# Prendo solo L (record che hanno informazioni sull'ora di uscita) e chi si sposta con mezzi pubblici
pendo = pendo[(pendo['tipo_rec'] == 'L') & (pendo['mezzo'] <= '06')]

# Creazione di un nuovo dataframe con le sole informazioni necessarie. Infine ottengo new_df con codice del comune di partenza 
# e di arrivo, i 4 orari di uscita e la proporzione di persone raggruppata per la chiave partenza-arrivo
new_df = pd.DataFrame({
    'com_res': pendo['prov_res'].astype(int).astype(str) + pendo['com_res'],
    'com_lav': pendo['prov_lav'].astype(int).astype(str) + pendo['com_lav'],
    'orario_usc': pendo['orario_usc'],
    'stima_num': pendo['stima_num'].astype(float).round().astype(int)
})
new_df.com_res = new_df.com_res.astype(int)
new_df.com_lav = new_df.com_lav.astype(int)
new_df = new_df[(new_df['com_res'].isin(df_zone_stop['cod_com'])) & (new_df['com_lav'].isin(df_zone_stop['cod_com']))]

grouped = new_df.groupby(['com_res', 'com_lav', 'orario_usc'], as_index=False)['stima_num'].sum()
# Sostituisci i valori nella colonna 'stima_num' del DataFrame originale
new_df['stima_num'] = new_df.set_index(['com_res', 'com_lav', 'orario_usc']).index.map(grouped.set_index(['com_res', 'com_lav', 'orario_usc'])['stima_num'])
new_df = new_df.drop_duplicates()
totali_per_gruppo = new_df.groupby(['com_res', 'com_lav'])['stima_num'].transform('sum')
# Aggiunta della colonna che rappresenta la proporzione del totale
new_df['Proporzione'] = new_df['stima_num'] / totali_per_gruppo



# Preparo i dataframe zona-stop e zona-comune
zone_to_stops = df_zone_stop.groupby('zone_id')['stop_id'].apply(list).to_dict()
zone_to_com = df_zone_stop.groupby('zone_id')['cod_com'].apply(lambda x: x.values[0]).to_dict()

requests = []

# Itero il dataframe (da zona - a zona - n persone)
for row in df_zone_to_zone.itertuples(index=False):
    from_zone, to_zone, mean_of_people = row
    n_people = int(round(mean_of_people))
    if n_people > 0:

        # Date le zone di partenza e arrivo, raccolgo le fermate disponibili
        from_stops_av = zone_to_stops.get(from_zone, [])
        to_stops_av = zone_to_stops.get(to_zone, [])
    
        if from_stops_av and to_stops_av:

            # This is for the set of requests K=N
            for i in range(n_people):

                # Scelgo casualemente una sola zona da quelle disponibili, si potrebbe pensare di prenderne di più e ingrandire il set di richieste
                from_stop = random.choice(from_stops_av)
                to_stop = random.choice(to_stops_av)
        
                # Mi assicuro che partenza e arrivo siano diversi
                while len(to_stops_av) > 1 and from_stop == to_stop:
                    to_stop = random.choice(to_stops_av)
                
                if from_stop != to_stop:
                    # Dall'altro dataframe (da comune - a comune - orario) prendo i tempi disponibili
                    from_com = zone_to_com[from_zone]
                    to_com = zone_to_com[to_zone]
                    times_av = new_df[(new_df['com_res'] == from_com) & (new_df['com_lav'] == to_com)]
                    
                    if not times_av.empty:
                        # Estrazione dell'orario in base alle proporzioni nei gruppi
                        times = times_av['orario_usc'].tolist()
                        weights = times_av['Proporzione'].tolist()
                        orario = random.choices(times, weights=weights, k=1)[0]
                        starting_time = convert_hour(orario, date)
                        requests.append({
                            'departure': from_stop, 
                            'arrival': to_stop, 
                            'starting_time': starting_time,
                            # 1 for the set of requests K=N, n_people // 2 if n_people // 2 > 0 else 1 to add the return
                            'n_people': 1 #n_people  #1 #n_people // 2 if n_people // 2 > 0 else 1
                        })
                        # # To add the return, switch the stops and add 10 hours in seconds (36000)
                        # requests.append({
                        #     'departure': to_stop, 
                        #     'arrival': from_stop, 
                        #     'starting_time': starting_time + 36000, 
                        #     'n_people': n_people // 2 if n_people // 2 > 0 else 1
                        # })


requests = pd.DataFrame(requests)
K = len(requests)
N = requests['n_people'].sum()
requests.to_csv(f'requests/florence/requests_K{K}_N{N}.csv', sep=';', index=False)