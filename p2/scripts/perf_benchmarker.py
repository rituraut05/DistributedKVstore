import os
from subprocess import run
import time
from statistics import mean
import argparse
import random
  
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
    FNULL = open(os.devnull, 'w')
    cmd = ["./../src/build/db_client"]
    # cmd_get=["./build/db_client", "name"]
    # cmd_put=["./build/db_client", "ritu", "raut"]
    i = 0
    t_start = time.perf_counter_ns()
    while(i < num_requests):
        if(random.uniform(0, 1) <= rw_ratio):
            # print('read')
            get_cmd = cmd + [str(key_list[i])]
            start = time.perf_counter_ns()
            # run(get_cmd, stdout=FNULL, stderr=FNULL)
            run(get_cmd)
            t = time.perf_counter_ns() - start
            timestamps_get.append(t)
        else:
            # print('write')
            put_cmd = cmd + [str(key_list[i]), str(time.perf_counter_ns())]
            start = time.perf_counter_ns()
            run(put_cmd, stdout=FNULL, stderr=FNULL)
            t = time.perf_counter_ns() - start
            timestamps_put.append(t)
        i = i+1
    t_end = time.perf_counter_ns()
    m_l = (t_end - t_start)/num_requests
    print(f'm_l = {m_l} ns')
    mean_latency = mean(timestamps_get + timestamps_put)
    print(f'mean read latency is {mean(timestamps_get)/1000000} ms')
    print(f'mean write latency is {mean(timestamps_put)/1000000} ms') 
    # print(timestamps_put)
    print(f'mean latency is {mean_latency/1000000} ms')

if __name__ == "__main__":
    main()