import os
import statistics
import json
import math

creating_data_dir = 'creating_data'
data_dir = 'data'
poor_data_dir = 'poor_data'

def print_statics(dir):
    data_num_info = []
    for file in os.listdir(dir):
        with open(dir+'/'+file, 'r', encoding='utf-8') as f:
            data_num_info.append(len(json.load(f)['data']))

    print(dir)
    num_recp = len(data_num_info)
    print('  num of recipients: {}'.format(num_recp))
    num_data = sum(data_num_info)
    print('  sum: {}'.format(num_data))
    print('  max: {}'.format(max(data_num_info)))
    print('  min: {}'.format(min(data_num_info)))
    ave = num_data / num_recp
    print('  ave: {}'.format(ave))
    print('  med: {}'.format(statistics.median(data_num_info)))
    pvar = statistics.pvariance(data_num_info, ave)
    print('  pvar: {}'.format(pvar))
    print('  pstd: {}'.format(math.sqrt(pvar)))

def count_upper(dir, border):
    data_num_info = []
    for file in os.listdir(dir):
        with open(dir+'/'+file, 'r', encoding='utf-8') as f:
            data_num_info.append(len(json.load(f)['data']))
    
    print('more than {} in {}: {}'.format(border, dir, sum([i >= border for i in data_num_info])))

if __name__ == "__main__":
    print_statics(data_dir)
    border = 8
    count_upper(poor_data_dir, border)