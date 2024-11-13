import subprocess

# Choose the seed
seed = '20241023'

# Choose which city you want to do
# 0 = adelaide, 1 = belfast, 2 = berlin, 3 = bordeaux, 4 = brisbane
# 5 = canberra, 6 = detroit, 7 = dublin, 8 = grenoble, 9 = helsinki
# 10 = kuopio, 11 = lisbon, 12 = luxembourg, 13 = melbourne, 14 = nantes
# 15 = palermo, 16 = paris, 17 = prague, 18 = rennes, 19 = roma
# 20 = sydney, 21 = toulouse, 22 = turku, 23 = venice, 24 = winnipeg
# 25 = florence
first = '0'
last = '25'

# Pre-processing of datasets
subprocess.run(['python3', 'script/CheckDatasets.py', first, last])

# Create fake requests
subprocess.run(['python3', 'script/FakeRequests.py', seed])

# Create dataset for Florence
subprocess.run(['python3', 'script/CreateDatasetsFlorence.py'])

# Create requests for Florence
subprocess.run(['python3', 'script/CreateRequestsFlorence.py', seed])

# Foremost path for the entire set of requests
subprocess.run(['python3', 'script/ForemostPath.py', first, last])

# Sampling
subprocess.run(['python3', 'script/Sampling.py', seed, first, last])

# Sampling repeated
subprocess.run(['python3', 'script/SamplingRepeated.py', seed, first, last])

# Analysis errors
subprocess.run(['python3', 'script/AnalysisErrors.py', first, last])

# Analysis errors repeated
subprocess.run(['python3', 'script/AnalysisErrorsRepeated.py'])