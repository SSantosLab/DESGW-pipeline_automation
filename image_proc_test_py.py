import os
import sys
import matplotlib.pyplot as plt
import glob
import random
import pandas as pd
import easyaccess as ea
import numpy as np
import re
import psycopg2
import subprocess
from subprocess import Popen, PIPE
import argparse
from multiprocessing import Pool,Process, Queue, current_process, Semaphore
import time
import queue
import csv

def EXPlist(explist):
    
    file_to_read = open(explist,'r')
    exp_file = file_to_read.readlines()
    elist = [exp.strip() for exp in exp_file]
    file_to_read.close()

    return elist
    
def run_dagsh(exps_to_run, finished_exps, exp_set):
    while True:
        try:
            #try to get task from the queue. get_nowait() function will raise queue.Empty exception if the queue is empty. 
            current_exp = exps_to_run.get_nowait()
        except queue.Empty:

            break
        else:
            #if no exception has been raised, create the command and add the task completion message to finished_exps queue
          
            #initialize command
            
            command = ['./DAGMaker.sh ' + exp_set[current_exp]]
            
            #process for each command ask nora about this
            print("Running " + command[0])
            process = subprocess.Popen(command, bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            f = open('dagmaker_'+exp_set[current_exp]+'.out', 'w')
            f.write(stdout)
            if stderr != None:
                f.write(stderr)
            f.close()
            #print so it knows its running mostly for testing
            print("All done with " + command[0])
            
            new_command = ['jobsub_submit_dag -G des --role=DESGW file://desgw_pipeline_' + exp_set[current_exp] + '.dag']
            
            print("Running" + new_command[0])
            os.system(new_command[0])
            print("Finished with" + new_command[0])
            finished_exps.put(exp_set[current_exp] + ' is done by ' + current_process().name)
           
            #pause the system for a bit so that it has time to finish executing 
            time.sleep(0.5)
    return True

#source setup img proc, necessary for dagmaker
cwd = os.getcwd()
command_source = ['source '+cwd+'/setup_img_proc.sh']

#ensure that source setup_diff_img will run by checking if there's a system error raised. 
output = os.system(command_source[0])
if output == 0:
    os.system(command_source[0])
else:
    raise ValueError('Something went wrong with setup_img_proc.sh. Please manually run or try again.')

inputted_exp_list = raw_input("Please input the filepath to your exp.list file")

# filepath = 'exposures_jul27.list'
filepath = inputted_exp_list
sample_exp_set = EXPlist(filepath)
number_exps = len(sample_exp_set)

#initializing information for the processing function arguments
number_of_tasks = number_exps
#number of processes is how many to submit/run at one time 
number_of_processes = 5
exp_set = sample_exp_set

def main():
    
    #initialize queues so the system can recognize what's been done and what needs to be done 
    exps_to_run = Queue()
    finished_exps = Queue()
    processes = []
    sema = Semaphore(number_of_processes)

    for i in range(number_of_tasks):
        exps_to_run.put(int(i))

    # creating processes
    for w in range(number_of_processes):
        sema.acquire()
        p = Process(target=run_dagsh, args=(exps_to_run, finished_exps, exp_set))
        processes.append(p)
        p.start()

    # completing processes
    for p in processes:
        p.join()

    # print what has been done to double check everything went smoothly
    while not finished_exps.empty():
        print(finished_exps.get())

    return True


if __name__ == '__main__':
    main()