import pandas as pd
project = pd.read_csv(
    "/Users/matteo/Desktop/lower_bound_v2.csv", sep=';', decimal=',')


# CODIFY ALL THE MODULATIONS OF THE PROJECT FILE
# BPSK = 0
# 4QAM-Strong = 1
# 4QAM = 2
# 8PSK = 3
# 16QAM-Strong = 4
# 16QAM = 5
# 32QAM = 6
# 64QAM = 7
# 128QAM = 8
# 256QAM = 9
# 512QAM = 10
# 1024QAM = 11
# 2048QAM = 12
# 4096QAM = 13

index = project.index[project['LOWERMOD'] == "BPSK-Normal"]
project.loc[index, 'LOWERMOD'] = 0

index = project.index[(project['LOWERMOD'] == "4QAM-Strong")
                      | (project['LOWERMOD'] == "acm-4QAMs")]
project.loc[index, 'LOWERMOD'] = 1

index = project.index[(project['LOWERMOD'] == "4QAM-Normal")
                      | (project['LOWERMOD'] == "acm-4QAM")]
project.loc[index, 'LOWERMOD'] = 2

index = project.index[(project['LOWERMOD'] == "8PSK-Normal")
                      | (project['LOWERMOD'] == "acm-8PSK")]
project.loc[index, 'LOWERMOD'] = 3

index = project.index[(project['LOWERMOD'] == "16QAM-Normal")
                      | (project['LOWERMOD'] == "acm-16QAM")]
project.loc[index, 'LOWERMOD'] = 4

index = project.index[(project['LOWERMOD'] == "16QAM-Strong")
                      | (project['LOWERMOD'] == "acm-16QAMs")]
project.loc[index, 'LOWERMOD'] = 5

index = project.index[(project['LOWERMOD'] == "32QAM-Normal")
                      | (project['LOWERMOD'] == "acm-32QAM")]
project.loc[index, 'LOWERMOD'] = 6
#
index = project.index[(project['LOWERMOD'] == "64QAM-Normal")
                      | (project['LOWERMOD'] == "acm-64QAM")]
project.loc[index, 'LOWERMOD'] = 7
#
index = project.index[(project['LOWERMOD'] == "128QAM-Normal")
                      | (project['LOWERMOD'] == "acm-128QAM")]
project.loc[index, 'LOWERMOD'] = 8
#
index = project.index[(project['LOWERMOD'] == "256QAM-Normal")
                      | (project['LOWERMOD'] == "acm-256QAM")]
project.loc[index, 'LOWERMOD'] = 9
#
index = project.index[(project['LOWERMOD'] == "512QAM-Normal")
                      | (project['LOWERMOD'] == "acm-512QAM")]
project.loc[index, 'LOWERMOD'] = 10
#
index = project.index[(project['LOWERMOD'] == "1024QAM-Normal")
                      | (project['LOWERMOD'] == "acm-1024QAM")]
project.loc[index, 'LOWERMOD'] = 11
#
index = project.index[(project['LOWERMOD'] == "2048QAM-Normal")
                      | (project['LOWERMOD'] == "acm-2048QAM")]
project.loc[index, 'LOWERMOD'] = 12
#
index = project.index[(project['LOWERMOD'] == "4096QAM-Normal")
                      | (project['LOWERMOD'] == "acm-4096QAM")]
project.loc[index, 'LOWERMOD'] = 13
#
# # Create the new CSV file containing the project information with the codified modulation
project.to_csv("Project_good_modulation.csv", index=False)
