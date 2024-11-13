import numpy as np
import pandas as pd
import re
import glob
import csv
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('first')
parser.add_argument('last')
args = parser.parse_args()
first = int(args.first)
last = int(args.last)

def error_lists_OM(popul, sample, N, k):
    actual_values = []
    estimate_values = []
    for stop in popul:
        for interval in popul[stop]:
            time_interval = interval[1] - interval[0] + 1
            actual_values += [popul[stop][interval] / N] * time_interval
            if stop in sample and interval in sample[stop]:
                estimate_values += [sample[stop][interval] / k] * time_interval
            elif stop in sample:
                put_value = False
                for int_sample in sample[stop]:
                    if interval[0] >= int_sample[0] and interval[1] <= int_sample[1]:
                        estimate_values += [sample[stop][int_sample] / k] * time_interval
                        put_value = True
                if not put_value:
                    estimate_values += [0] * time_interval 
            else:
                estimate_values += [0] * time_interval 
    return actual_values, estimate_values

def error_lists_CV(popul, sample, N, k):
    actual_values = []
    estimate_values = []
    for bus in popul:
        for trip in popul[bus]:
            actual_values.append(popul[bus][trip] / N)
            if bus in sample and trip in sample[bus]:
                estimate_values.append(sample[bus][trip] / k)
            else:
                estimate_values.append(0)
    return actual_values, estimate_values

def error_statistics(actual_values, estimate_values):
    diff_vector = np.subtract(estimate_values, actual_values)
    mean = np.mean(diff_vector)
    qua_25 = np.quantile(diff_vector, 0.25)
    median = np.quantile(diff_vector, 0.5)
    qua_75 = np.quantile(diff_vector, 0.75)
    mse = sum([x**2 for x in diff_vector]) / len(diff_vector)
    corr = np.corrcoef(actual_values, estimate_values)[0, 1]
    return mean, qua_25, median, qua_75, mse, corr


cities = pd.read_csv(f"city_details.csv", sep=";")

for n_city in range(first, last+1):
    city = cities.iloc[n_city]
    city = city['city']
    nodes = pd.read_csv(f"new_datasets/{city}/nodes.csv", sep=";")
    edges = pd.read_csv(f"new_datasets/{city}/edges.csv", sep=";")
    n_nodes = len(nodes)
    n_edges = len(edges)

    header = ['K', 'N', 'k', 'OM_mean', 'OM_qua_25', 'OM_median', 'OM_qua_75', 'OM_mse', 'OM_corr', 'CV_mean', 'CV_qua_25', 'CV_median', 'CV_qua_75', 'CV_mse', 'CV_corr', 'AW_sample - AW_real', 'AT_sample - AT_real', 'ExTime_sample', 'ExTime_real']
    data = []

    for file in glob.glob(f'results/{city}/result_*.txt'):
        K = int(re.search('\d+', re.search('K\d+', file).group()).group())
        N = int(re.search('\d+', re.search('N\d+', file).group()).group())
        popul = open(file, 'r').read()
        NP_popul = int(re.search('\d+', re.search('number of people: \d+', popul).group()).group())
        NR_popul = int(re.search('\d+', re.search('number of requests: \d+', popul).group()).group())
        AW_popul = int(re.search('\d+', re.search('average waiting time: \d+', popul).group()).group())
        AT_popul = int(re.search('\d+', re.search('average travelling time: \d+', popul).group()).group())
        OM_popul = eval(re.search('{.*', re.search('occupancy matrix: .*\n', popul).group()[:-1]).group())
        CV_popul = eval(re.search('{.*', re.search('crowding vector: .*', popul).group()).group())
        ET_popul = re.search('\d.*', re.search('execution time: .*', popul).group()).group()
        FP_popul = int(re.search('\d+', re.search('number of foremost path calculated: \d+', popul).group()).group())

        for filesample in glob.glob(f'results/{city}/sample_k*_K{K}_N{N}_*.txt'):
            k = int(re.search('\d+', re.search('k\d+', filesample).group()).group())
            num_test = int(filesample.split('_')[-1].split('.')[0])
            sample = open(filesample, 'r').read()
            NP_sample = int(re.search('\d+', re.search('number of people: \d+', sample).group()).group())
            NR_sample = int(re.search('\d+', re.search('number of requests: \d+', sample).group()).group())
            AW_sample = int(re.search('\d+', re.search('average waiting time: \d+', sample).group()).group())
            AT_sample = int(re.search('\d+', re.search('average travelling time: \d+', sample).group()).group())
            OM_sample = eval(re.search('{.*', re.search('occupancy matrix: .*\n', sample).group()[:-1]).group())
            CV_sample = eval(re.search('{.*', re.search('crowding vector: .*', sample).group()).group())
            ET_sample = re.search('\d.*', re.search('execution time: .*', sample).group()).group()
            FP_sample = int(re.search('\d+', re.search('number of foremost path calculated: \d+', sample).group()).group())

            summary = 'Differences between approximation and real:\n\n'
            summary += f'Average waiting time: {AW_sample} - {AW_popul} = {AW_sample-AW_popul}'
            summary += f'\nAverage travelling time: {AT_sample} - {AT_popul} = {AT_sample-AT_popul}'
            summary += f'\nExecution time: {ET_sample} - {ET_popul}'
            summary += f'\nNumber of people: {NP_sample} - {NP_popul}'
            summary += f'\nNumber of requests: {NR_sample} - {NR_popul}'
            summary += f'\nNumber of foremost path calculated: {FP_sample} - {FP_popul}'
            OM_actual_values, OM_estimate_values = error_lists_OM(OM_popul, OM_sample, NP_popul, NP_sample)
            OM_mean, OM_qua_25, OM_median, OM_qua_75, OM_mse, OM_corr = error_statistics(OM_actual_values, OM_estimate_values)
            CV_actual_values, CV_estimate_values = error_lists_CV(CV_popul, CV_sample, NP_popul, NP_sample)
            CV_mean, CV_qua_25, CV_median, CV_qua_75, CV_mse, CV_corr = error_statistics(CV_actual_values, CV_estimate_values)
            summary += f'\n\nStatistics occupancy matrix:\nmean:        {OM_mean}\n25 quantile: {OM_qua_25}\nmedian:      {OM_median}\n75 quantile: {OM_qua_75}\nmse:         {OM_mse}\ncorrelation: {OM_corr}\n'
            summary += f'\nStatistics crowding vector:\nmean:        {CV_mean}\n25 quantile: {CV_qua_25}\nmedian:      {CV_median}\n75 quantile: {CV_qua_75}\nmse:         {CV_mse}\ncorrelation: {CV_corr}'

            filename = f'summary_k{k}_K{K}_N{N}_{num_test}'
            open(f'results/{city}/{filename}.txt', 'w').write(summary)
            data.append([K, N, k, format(OM_mean, '.20f'), format(OM_qua_25, '.20f'), format(OM_median, '.20f'), format(OM_qua_75, '.20f'), format(OM_mse, '.20f'), format(OM_corr, '.20f'), format(CV_mean, '.20f'), format(CV_qua_25, '.20f'), format(CV_median, '.20f'), format(CV_qua_75, '.20f'), format(CV_mse, '.20f'), format(CV_corr, '.20f'), AW_sample-AW_popul, AT_sample-AT_popul, str(ET_sample).split('.')[0], str(ET_popul).split('.')[0]])

    data = sorted(data)
    with open(f'results/{city}/summaryTable_increasing_k.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

    os.makedirs(f'summaries/{city}', exist_ok=True)
    pd.DataFrame([header] + data).to_excel(f'summaries/{city}/summaryTable_increasing_k.xlsx', index=False, header=False)
