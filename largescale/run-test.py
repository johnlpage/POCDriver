#!/usr/bin/env python

import sys
import os
import time
import subprocess
import random
import datetime
import csv
from pymongo import MongoClient
from bson.binary import Binary
from loremipsum import get_sentences



# Steering
insert_rate = 0
update_rate = 100
query_rate = 0
num_collections = 1
total_runtime = 3600 * 12
time_to_ramp = total_runtime/2
ramp_interval = 300
worker_threads = 32
gross_throughput = 10000
collection_ramp_size = 500
collection_ramp_rate = 1.25
working_set_docs = 1000000
collections_contents = {}
output_csv = "out.csv"
fail_at_ms = 10
batch_size = 1


runtime = 0
output_filename = "results.csv"
last_ops = {"insert": 0, "update": 0, "delete": 0, "query":0, "writes":0, "write_latency":0}
dbname = "POCDB"
collname = "POCCOLL"
java_command = False

def get_last_ops(client):
    res = client.admin.command('serverStatus')
    ops = res['opcounters']
    gross = ops['insert'] + ops['update']
    last_gross = last_ops['insert'] + last_ops['update']
    writes = 0
    write_latency = 0
    if 'opLatencies' in res:
        writes = res['opLatencies']['writes']['ops'] - last_ops['writes']
        write_latency = res['opLatencies']['writes']['latency'] - last_ops['write_latency']
        last_ops['writes'] = res['opLatencies']['writes']['ops']
        last_ops['write_latency'] = res['opLatencies']['writes']['latency']
    last_ops['insert'] = ops['insert']
    last_ops['update'] = ops['update']
    collections = client[dbname].command('dbstats')['collections']
    avg_latency = 0
    if writes > 0:
        avg_latency = write_latency/writes
    return ("%d,%d,%d,%d,%d" % ((gross - last_gross),collections,writes,write_latency,avg_latency)), avg_latency

def random_string(length):
    return get_sentences(length)[0]


def populate_collections(client, collections):
    docs_per = working_set_docs / collections
    for x in range(collections):
        if x not in collections_contents:
            ns = collname + str(x)
            collections_contents[x] = docs_per
            numInBulk = 0
            bulkSize = 1000
            bulk = client[dbname][ns].initialize_unordered_bulk_op()
            str1 = random_string(1)
            str2 = random_string(1)
            str3 = random_string(1)
            str4 = random_string(1)
            str5 = random_string(1)
            for y in range(docs_per):
                if y % bulkSize == 0 and y > 0:
                    bulk.execute()
                    bulk = client[dbname][ns].initialize_unordered_bulk_op()

                bulk.insert({ "_id" : y,
                    "fld0" : random.randint(0,10000000),
                    "fld1" : random.randint(0,10000000),
                    "fld2" : str1,
                    "fld3" : str2,
                    "fld4" : str3,
                    "fld5" : datetime.datetime.now(),
                    "fld6" : random.randint(0,10000000),
                    "fld7" : str4,
                    "fld8" : str5,
                    "fld9" : random.randint(0,10000000),
                    "bin" : Binary("0") })

            bulk.execute()
           # print("populated " + dbname + "." + ns + " with " + str(docs_per) + " documents")
                
    # FsyncLock here
    client.admin.command("fsync", lock=False)


def launch_poc_driver(run_collections):
    docs_per = working_set_docs / run_collections
    FNULL = open(os.devnull, 'w')
    global java_proc
    command = ("java -jar bin/POCDriver.jar" \
               " -i " + str(insert_rate) + 
               " -u " + str(update_rate) +
               " -k " + str(query_rate) +
               " -q " + str(gross_throughput) + 
               " -z " + str(docs_per) +
               " -d " + str(ramp_interval) +
               " -y " + str(run_collections) +
               " --collectionKeyMax " + str(docs_per) +
               " -o " + str(output_csv) +
               " -t " + str(worker_threads) +
               " -b " + str(batch_size))
    print(command)
    sys.stdout.flush()
    java_proc = subprocess.Popen(command, shell=True, stdout=FNULL)

def load_from_config(filename):
    global insert_rate, update_rate, query_rate, num_collections, total_runtime, time_to_ramp, ramp_interval, worker_threads, gross_throughput, collection_ramp_size, working_set_docs, collection_ramp_rate, fail_at_ms
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
            if arr[0] == "rampsize":
                collection_ramp_size = int(arr[1])
            if arr[0] == "working_set_docs":
                working_set_docs = int(arr[1])
            if arr[0] == "collection_ramp_rate":
                collection_ramp_rate = float(arr[1])
            if arr[0] == "fail_at_ms":
                fail_at_ms = int(arr[1])

def gather_avg(colls):
    data = csv.reader(open(output_csv, 'r'), delimiter=",")
    latency = {}
    ops = {}
    for row in data:
        if row[0] != "system_clock":
            coll_count = int(row[2])
            if coll_count not in latency:
                latency[coll_count] = 0
                ops[coll_count] = 0
            latency[coll_count] += (int(row[6]))
            ops[coll_count] += int(row[10])
    #print("Gross latency is " + str(latency[colls]) + ". Gross ops is " + str(ops[colls]))
    #print("Avg latency per op " + str(float(latency[colls])/ops[colls]))
    return float(latency[colls])/ops[colls]


# Main
if len(sys.argv) > 1:
    load_from_config(sys.argv[1])

client = MongoClient('mongodb://localhost:27017/')
fhandle = open(output_filename, 'a')
fhandle.write("time,relative_time,inserts,collections,num_writes,write_latency,average_latency\n")
start=time.time()
go=True
fail_run=False
passed=1
collections=collection_ramp_size
while (go):
    populate_collections(client, collections)
    # Grab the recent latiencies and opctrs, as the populate can skew them
    get_last_ops(client)
    launch_poc_driver(collections)
    start_interval = time.time()
    interval_rutime = 0
    avg_latencies = []
    while (interval_rutime < ramp_interval): 
        now = time.time()
        runtime = now - start
        interval_rutime = now - start_interval
        out, avg_l = get_last_ops(client)
        avg_latencies.append(avg_l)
        fhandle.write("%d,%d,%s\n" % (time.time(),runtime,out))
        fhandle.flush()
        time.sleep(1)
    res = gather_avg(collections)
    print("Run completed avg latency with " + str(collections) + " collections is " + str(res) + "ms/op")
    collections = int(collections * collection_ramp_rate)
    if (time.time() - start) > total_runtime:
        go=False
        passed=0
    if res > fail_at_ms:
        if fail_run == False:
            fail_run=True
        else:
            go=False
    # Work out if we should bail.

# Close the results file 
fhandle.close()
exit(passed)
