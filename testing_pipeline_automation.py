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
from datetime import date
import logging

#introduce argparse variables to be used as parameters throughout the code
parser = argparse.ArgumentParser(description='Set parameters for image processing.')
parser.add_argument('--test', metavar='t', action='store',
                    help='answers whether or not this is a test run of image processing [y/n]')

parser.add_argument('--benchmark_test', metavar='tsn', action='store',
                    help='answers whether this is a benchmark test [y/n]')

parser.add_argument('--exp_list_location', metavar='exp', action='store',
                    help='Necessary parameter. Input location of an exposure.list file, relative to gw_workflow (which should be located at ../gw_workflow)')


parser.add_argument('--test_season_number', metavar='tsn', action='store',
                    help='specifies test season number to be used. possible values are 2206 and 2208 (default: 2206)')

parser.add_argument('--season', metavar='s', action='store',
                    help='specifies desired season number. if "random" is input for this parameter, a suitable season number will be chosen by the system.')


args= parser.parse_args()

#check if output directory exists, if it doesn't create it
output_dir_exists = os.path.exists('./image_proc_outputs/')
if not output_dir_exists:
        os.mkdir('./image_proc_outputs/')

#create log file for debugging
today = str(date.today())
time_now = str(time.time())

logging.basicConfig(filename='./image_proc_outputs/'+today+'_'+time_now+'_pipeline_automation.out', level=logging.DEBUG)

#check if gw_workflow folder exists. if it doesnt, clone it from github
filepath = ['../gw_workflow']
isExist = os.path.exists(filepath[0])
if isExist:
      print('gw_workflow found')
else:
      logging.info('gw_workflow not found. Cloning github repository.')
      git_command = ['git clone https://github.com/SSantosLab/gw_workflow.git ../gw_workflow' ]
      print('Running '+git_command[0]+'in order to clone necessary folder gw_workflow...')
      git_output = os.system(git_command[0])
    
      if git_output != 0:
           logging.warning('There was an issue with finding and/or cloning gw_workflow. Aborting...')
           raise ValueError('Something went wrong with cloning gw_workflow. Please manually run or try again.')
      
#figure out if pipeline testing suite is present. if not, clone it one folder back
filepath = ['../DESGW-Pipeline-Testing-Suite']
isExist = os.path.exists(filepath[0])
if isExist:
      print('Pipeline testing suite found.')
else:
      logging.debug('Pipeline testing suite not found. Code is cloning it at ../DESGW-Pipeline-Testing-Suite')
      git_command = ['git clone https://github.com/SSantosLab/DESGW-Pipeline-Testing-Suite.git ../DESGW-Pipeline-Testing-Suite' ]
      print('Running '+git_command[0]+'in order to clone necessary folder DESGW-Pipeline-Testing-Suite...')
      git_output = os.system(git_command[0])

      if git_output != 0:
            logging.debug('Something went wrong with finding and/or cloning DESGW-Pipeline-Testing-Suite. Aborting...')
            raise ValueError('Something went wrong with cloning DESGW-Pipeline-Testing-Suite. Please clone it manually or try again.')
            
#start by asking if its a test run to see if we will run the testing suite or the regular automation
answer_test = str(args.test)
bench_criteria = 0
test_criteria = 0

#set path variables, essentially replacing the sourcing of the two .sh files since that occurs in a subshell not accessible by the code
USER = os.getenv('USER')
if USER == 'desgw':
    dagmaker_file = './DAGMaker.sh '
    print('You are running as desgw.')
else:
    dagmaker_file = './DAGMaker_proxyuser.sh '
    print('You are not running as desgw.')

# #record variables for debugging purposes. this will be repeated later as well
# pythonpath_var = os.environ['PYTHONPATH']
# path_var = os.environ['PATH']

# logging.debug('Path and Python path variables before moving directories:' + pythonpath_var + path_var)

#determine if its a benchmark test, in which case some specific exposures will need to be pulled later

logging.info('This run is a test. It will use the Pipeline Testing Suite.')
#ask_bench = raw_input('Is this a benchmark test? [y/n]: ')
#bench_answer = ask_bench
bench_answer = str(args.benchmark_test)
if bench_answer == 'y':
        bench_criteria = 1
        logging.info('This is a benchmark test.')
if bench_answer == 'n':
        test_criteria = 1

# raise ValueError('finish here, line 168')
#testing pipeline
if test_criteria == 1:
    os.chdir('../DESGW-Pipeline-Testing-Suite')
    
    command = ['python configure_dag.py']
#             command = ['pwd']
            
    logging.info('Code now running from DESGW-Pipeline-Testing-Suite...')
    print("Running " + command[0])
    error_dag = 0
    process = subprocess.Popen(command[0], bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
#     print(stdout)
    f = open('configure_dagmaker_errors.out', 'w')
    f.write(stdout)
    if stderr != None:
          logging.warning('Potential errors found while running configure_dag.py. For more information, see configure_dagmaker_errors.out file')
          f.write(stderr)
          error_dag = 1
    f.close()
    
    if error_dag == 1:
        continue_question = raw_input('Warning: there was an error with configure_dag.py. Please check the configure_dagmaker_errors.out file for information, then decide if you would like to continue the testing pipeline by answering [y/n]: ')
        continue_answer = continue_question
        if continue_answer == 'n':
            logging.debug('User terminated program due to errors with configure_dag.py.')
            print('Terminating this program.')
            sys.exit()
            
        else:
            print('Program continuing.')
            
    
    print('Retrieving season in order to run gw_workflow.py...')
    filepath = 'dagmaker.rc'


    with open(filepath, 'r') as file:
        data = file.readlines()
#     print(data[3])
    line = data[3]
    if line.endswith('\n'):
          line_new = line[:-len('\n')]
    if line_new.startswith('SEASON='):
          line_fixed = line[len('SEASON='):]
    season = int(line_fixed)
#     print(season)

#get the season from the list as numbers
    
    gw_command = ['python3 run_gw_workflow.py --exp_list '+season+'exposures.list']
    print("Running " + gw_command[0])
    error_gw = 0
    process = subprocess.Popen(gw_command[0], bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    f = open('run_gw_workflow_errors.out', 'w')
    f.write(stdout)
    if stderr != None:
          f.write(stderr)
          logging.warning('Potential errors found while running run_gw_workflow.py. For more information, see run_gw_workflow_errors.out file')
          error_gw = 1
    f.close()
    
    if error_gw == 1:
        continue_question = raw_input('Warning: there was an error with run gw_workflow.py. Please check the run_gw_workflow_errors.out file for information, then decide if you would like to continue the testing pipeline by answering [y/n]: ')
        continue_answer = continue_question
        if continue_answer == 'n':
            print('Terminating this program.')
            logging.debug('User terminated program due to errors with run gw_workflow.py.')
            sys.exit()
            
    js_command = ['python3 fetchJobSubStats.py --season '+season+' --exp_list {season}exposures.list']
    print("Running " + js_command[0])
    error_js = 0
    process = subprocess.Popen(js_command[0], bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    stdout, stderr = process.communicate()
    f = open('fetchJobSubStats_errors.out', 'w')
    f.write(stdout)
    if stderr != None:
          f.write(stderr)
          error_js = 1
    f.close()
    
    if error_js == 1:
        logging.warning('Potential errors found while running fetchJobSubStats.py. For more information, see fetchJobSubStats_errors.out file')
        continue_question2 = raw_input('Warning: there was an error with fetchJobSubStats.py. Please check the fetchJobSubStats_errors.out file for information, then decide if you would like to continue the testing pipeline by answering [y/n]: ')
        continue_answer2 = continue_question2
        if continue_answer2 == 'n':
            print('Terminating this program.')
            logging.debug('User terminated program due to errors with fetchJobSubStats.py.')
            sys.exit()
    