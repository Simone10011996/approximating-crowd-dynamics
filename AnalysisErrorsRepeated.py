import numpy as np
import pandas as pd
import re
import glob
import csv
import datetime
import os

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


for folder in glob.glob('results_samplesrep/*'):
    print(folder)
    city = folder.split('/')[-1]
    file = glob.glob(f'{folder}/*.txt')[0]
    K = int(re.search('K\d+', file).group()[1:])
    N = int(re.search('N\d+', file).group()[1:])
    k = int(re.search('k\d+', file).group()[1:])

    if not os.path.exists(f'summaries/{city}/summaryTable_K{K}_N{N}_k{k}.xlsx'):

        list_M = [1, 5, 10, 50, 100]
        header = ['K', 'N', 'k', 'M', 'num_test', 'OM', 'CV', 'AW', 'AT', 'ExTime_sample', 'ExTime_real', 'OM_ErrTot', 'CV_ErrTot', 'AW_ErrTot', 'AT_ErrTot', 'Exec time mean']  
        data = []

        errors_OM = {}
        errors_CV = {}
        errors_AW = {}
        errors_AT = {}
        exec_times = {}
        for m in list_M:
            errors_OM[m] = []
            errors_CV[m] = []
            errors_AW[m] = []
            errors_AT[m] = []
            exec_times[m] = []

        file = f'results/{city}/result_K{K}_N{N}.txt'
        popul = open(file, 'r').read()
        NP_popul = int(re.search('\d+', re.search('number of people: \d+', popul).group()).group())
        OM_popul = eval(re.search('{.*', re.search('occupancy matrix: .*\n', popul).group()[:-1]).group())
        CV_popul = eval(re.search('{.*', re.search('crowding vector: .*', popul).group()).group())
        AW_popul = int(re.search('\d+', re.search('average waiting time: \d+', popul).group()).group())
        AT_popul = int(re.search('\d+', re.search('average travelling time: \d+', popul).group()).group())
        ET_popul = re.search('\S+$', re.search('execution time: \S+', popul).group()).group()
        et_popul = str(ET_popul).split('.')[0]

        for filesample in glob.glob(f'results_samplesrep/{city}/sample_k{k}_K{K}_N{N}_*_M*.txt'):
            M = int(re.search('\d+', re.search('M\d+', filesample).group()).group())
            num_test = int(re.search('\d+', re.search('_\d+_', filesample).group()).group())
            sample = open(filesample, 'r').read()
            NP_sample = int(re.search('\d+', re.search('number of people: \d+', sample).group()).group())
            OM_sample = eval(re.search('{.*', re.search('occupancy matrix: .*\n', sample).group()[:-1]).group())
            CV_sample = eval(re.search('{.*', re.search('crowding vector: .*', sample).group()).group())
            AW_sample = int(re.search('\d+', re.search('average waiting time: \d+', sample).group()).group())
            AT_sample = int(re.search('\d+', re.search('average travelling time: \d+', sample).group()).group())
            ET_sample = re.search('\S+$', re.search('execution time: \S+', sample).group()).group()
            et_sample = str(ET_sample).split('.')[0]

            OM_actual_values, OM_estimate_values = error_lists_OM(OM_popul, OM_sample, NP_popul, NP_sample)
            OM_mean = np.mean(np.subtract(OM_actual_values, OM_estimate_values))
            CV_actual_values, CV_estimate_values = error_lists_CV(CV_popul, CV_sample, NP_popul, NP_sample)
            CV_mean = np.mean(np.subtract(CV_actual_values, CV_estimate_values))

            errors_OM[M].append(OM_mean)
            errors_CV[M].append(CV_mean)
            errors_AW[M].append(AW_sample-AW_popul)
            errors_AT[M].append(AT_sample-AT_popul)
            exec_times[M].append(et_sample)

            data.append([K, N, k, M, num_test, format(OM_mean, '.20f'), format(CV_mean, '.20f'), AW_sample-AW_popul, AT_sample-AT_popul, et_sample, et_popul])


        for row in data:
            row.append(format(np.mean(errors_OM[row[3]]), '.20f'))
            row.append(format(np.mean(errors_CV[row[3]]), '.20f'))
            row.append(np.mean(errors_AW[row[3]]))
            row.append(np.mean(errors_AT[row[3]]))
            row.append(str(datetime.timedelta(seconds=sum(map(lambda f: int(f[0])*3600 + int(f[1])*60 + int(f[2]), map(lambda f: f.split(':'), exec_times[row[3]])))/len(exec_times[row[3]]))))

        data = sorted(data)
        with open(f'results_samplesrep/{city}/summaryTable_K{K}_N{N}_k{k}.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)
            for row in data:
                writer.writerow(row)

        os.makedirs(f'summaries/{city}', exist_ok=True)
        pd.DataFrame([header] + data).to_excel(f'summaries/{city}/summaryTable_K{K}_N{N}_k{k}.xlsx', index=False, header=False)