#!/usr/bin/env python

import sys
import os
import time
import subprocess
from pymongo import MongoClient


# Steering
insert_rate = 100
update_rate = 0
query_rate = 0
num_collections = 1
total_runtime = 3600 * 12
time_to_ramp = total_runtime/2
ramp_interval = 60
worker_threads = 32
gross_throughput = 10000


runtime = 0
output_filename = "results.csv"
last_ops = {"insert": 0, "update": 0, "delete": 0, "query":0, "writes":0, "write_latency":0}
dbname = "POCDB"
java_command = False

def get_last_ops(client):
    res = client.admin.command('serverStatus')
    ops = res['opcounters']
    gross = ops['insert'] + ops['update']
    last_gross = last_ops['insert'] + last_ops['update']
    writes = res['opLatencies']['writes']['ops'] - last_ops['writes']
    write_latency = res['opLatencies']['writes']['latency'] - last_ops['write_latency']
    last_ops['insert'] = ops['insert']
    last_ops['update'] = ops['update']
    last_ops['writes'] = res['opLatencies']['writes']['ops']
    last_ops['write_latency'] = res['opLatencies']['writes']['latency']
    collections = client[dbname].command('dbstats')['collections']
    avg_latency = 0
    if writes > 0:
        avg_latency = write_latency/writes
    return ("%d,%d,%d,%d,%d" % ((gross - last_gross),collections,writes,write_latency,avg_latency))

def launch_poc_driver():
    global java_proc
    command = ("java -jar bin/POCDriver.jar" \
               " -i " + str(insert_rate) + 
               " -u " + str(update_rate) +
               " -q " + str(query_rate) + 
               " -z 10000000" 
               " -d " + str(total_runtime) +
               " -y " + str(num_collections) +
               " -o out.csv -t " + str(worker_threads) +
               " --incrementPeriod " + str(time_to_ramp) +
               " --incrementIntvl " + str(ramp_interval)) 
    print(command)
    java_proc = subprocess.Popen(command, shell=True)

def load_from_config(filename):
    global insert_rate, update_rate, query_rate, num_collections, total_runtime, time_to_ramp, ramp_interval, worker_threads, gross_throughput
    with open(filename, "r") as f:
        for line in f:
            arr = line.split('=')
            if arr[0] == "insert":
                insert_rate = int(arr[1])
            if arr[0] == "update":
                update_rate = int(arr[1])
            if arr[0] == "read":
                query_rate = int(arr[1])
            if arr[0] == "collections":
                num_collections = int(arr[1])
            if arr[0] == "runtime":
                total_runtime = int(arr[1])
            if arr[0] == "ramptime":
                time_to_ramp = int(arr[1])
            if arr[0] == "ramp_interval":
                ramp_interval = int(arr[1])
            if arr[0] == "thread":
                worker_threads = int(arr[1])
            if arr[0] == "throughput":
                gross_throughput = int(arr[1])

# Main
if len(sys.argv) > 1:
    load_from_config(sys.argv[1])

client = MongoClient('mongodb://localhost:27017/')
fhandle = open(output_filename, 'a')
fhandle.write("time,relative_time,inserts,collections,num_writes,write_latency,average_latency\n")
launch_poc_driver()

start=time.time()
while (total_runtime > runtime):
    now = time.time()
    runtime = now - start
    out = get_last_ops(client)
    fhandle.write("%d,%d,%s\n" % (time.time(),runtime,out))
    fhandle.flush()
    time.sleep(1)

# Kill the 
os.kill(java_proc.pid, 5)
fhandle.close()

