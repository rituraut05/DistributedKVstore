import os
from subprocess import run
import time
from statistics import mean
import argparse
import random
from matplotlib import pyplot as plt
from scipy.interpolate import make_interp_spline
import numpy as np
  
# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument
parser.add_argument("-n", "--num_of_requests", help = "Total number of client requests to server")
parser.add_argument("-rw", "--read_write_ratio", help = "(READS or GET)/(WRITES or PUT) Requests (<1)")
 
# Read arguments from command line
args = parser.parse_args()

if args.num_of_requests:
    print("number of client requests to server: % s" % args.num_of_requests)
if args.read_write_ratio:
    print("read write ratio: % s" % args.read_write_ratio)

def main():
    filename='zipfian_keys.csv'
    with open(filename, 'r') as file:
        keys = file.read()
    key_list = [int(x) for x in keys.split(',')]
    # print(key_list)
    # print(len(key_list))
    num_requests = int(args.num_of_requests)
    rw_ratio = float(args.read_write_ratio)
    timestamps_get = []
    timestamps_put = []
    x_get = []
    x_put = []
    FNULL = open(os.devnull, 'w')
    cmd = ["./../src/build/db_client"]
    # cmd_get=["./build/db_client", "name"]
    # cmd_put=["./build/db_client", "ritu", "raut"]
    i = 0
    r_i = 0
    w_i = 0
    t_start = time.perf_counter_ns()
    while(i < num_requests):
        if(random.uniform(0, 1) <= rw_ratio):
            # print('read')
            get_cmd = cmd + [str(key_list[i])]
            start = time.perf_counter_ns()
            run(get_cmd, stdout=FNULL, stderr=FNULL)
            # run(get_cmd)
            t = time.perf_counter_ns() - start
            timestamps_get.append(t)
            r_i = r_i + 1
            x_get.append(r_i)
        else:
            # print('write')
            put_cmd = cmd + [str(key_list[i]), str(time.perf_counter_ns())]
            start = time.perf_counter_ns()
            run(put_cmd, stdout=FNULL, stderr=FNULL)
            t = time.perf_counter_ns() - start
            timestamps_put.append(t)
            w_i = w_i + 1
            x_put.append(w_i)
        i = i+1
    t_end = time.perf_counter_ns()
    throughput = (num_requests*1000000000)/(t_end - t_start)
    print(f'throughput = {throughput} ops/s')
    # mean_latency = mean(timestamps_get + timestamps_put)
    # print(f'mean read latency is {mean(timestamps_get)/1000000} ms')
    # print(f'mean write latency is {mean(timestamps_put)/1000000} ms') 
    # print(timestamps_put)
    # print(f'mean latency is {mean_latency/1000000} ms')

    # timestamps_get = list(map(lambda n: n/1000000, timestamps_get))
    # timestamps_put = list(map(lambda n: n/1000000, timestamps_put))

    # print(timestamps_get)
    # print()
    # print(timestamps_put)

    # plt.figure(1)
    # plt.scatter(x_get, timestamps_get)
    # plt.ylim(ymin=0.0)
    # plt.xlabel('Iteration')
    # plt.ylabel('Read Latency (ms)')
    # plt.savefig('read_latency.png')

    # plt.figure(2)
    # plt.scatter(x_put, timestamps_put)
    # plt.ylim(ymin=0.0)
    # plt.xlabel('Iteration')
    # plt.ylabel('Write Latency (ms)')
    # plt.savefig('write_latency.png')

if __name__ == "__main__":
    main()