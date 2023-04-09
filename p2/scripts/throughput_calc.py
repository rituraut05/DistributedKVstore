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
    filename='zipfian_keys_100k.csv'
    with open(filename, 'r') as file:
        keys = file.read()
    key_list = [int(x) for x in keys.split(',')]
    print(len(key_list))
    num_requests = int(args.num_of_requests)
    rw_ratio = float(args.read_write_ratio)
    FNULL = open(os.devnull, 'w')
    cmd = ["./../src/build/db_client"]
    throughput = []
    i = 0
    t_start = t = time.perf_counter_ns()
    count = i = 0    
    while(i < num_requests):
        if(random.uniform(0, 1) <= rw_ratio):
            get_cmd = cmd + [str(key_list[i])]
            p = run(get_cmd, stdout=FNULL, stderr=FNULL)
            # print(f'{i} returncode: {p.returncode}')
            if(p.returncode != 0):
                count = count-1
            # timestamps_get.append(t)
        else:
            put_cmd = cmd + [str(key_list[i]), str(time.perf_counter_ns())]
            p = run(put_cmd, stdout=FNULL, stderr=FNULL)
            if(p.returncode != 0):
                count = count-1
            # timestamps_put.append(t)
        count = count+1
        t_s = time.perf_counter_ns()
        if(t_s - t > 500000000 ):
            tput = count*1e9/(t_s-t)
            print(tput)
            throughput.append(tput)
            t=time.perf_counter_ns()
            count=0
        i = i+1
    print(throughput)

if __name__ == "__main__":
    main()
