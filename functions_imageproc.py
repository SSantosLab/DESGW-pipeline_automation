import os
import sys
import matplotlib.pyplot as plt
import glob
import random
import pandas as pd
import numpy as np
import re
import subprocess
from subprocess import Popen, PIPE
import argparse
from multiprocessing import Lock, Process, Queue, current_process
import time
import queue


# In[1]:


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
            os.system(new_command)
            print("Finished with" + new_command[0])
            finished_exps.put(exp_set[current_exp] + ' is done by ' + current_process().name)
           
            #pause the system for a bit so that it has time to finish executing 
            time.sleep(0.5)
    return True
