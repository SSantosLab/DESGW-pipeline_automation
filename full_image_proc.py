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



#check if gw_workflow folder exists. if it doesnt, clone it from github
filepath = ['../gw_workflow']
isExist = os.path.exists(filepath[0])
if isExist:
    print('gw_workflow found')
else:
    git_command = ['git clone https://github.com/SSantosLab/gw_workflow.git ../gw_workflow' ]
    print('Running'+git_command[0]+'in order to clone necessary folder gw_workflow...')
    git_output = os.system(git_command[0])
    #check if the system command will run successfully. if it does, output will be 0 with os.system. if it doesn't, raise exception. if you got this error, try manually git cloning https://github.com/SSantosLab/gw_workflow.git one folder back in a folder called gw_workflow
    if git_output != 0:
        raise ValueError('Something went wrong with cloning gw_workflow. Please manually run or try again.')
        
os.chdir('../gw_workflow')

    
test_check = (raw_input("Would you like to update season? [y/n]: "))
test = test_check
if test == ('n'):
#         #des gw testing suite and season number 
    SEASON = 2208
    print('Double checking SEASON is set to 2208. If you need a different season, please restart or manually update dagmaker.rc before you enter your exposures.list file later in the code.')
#     inputted_season = (raw_input("Do you want your season to be 2206 or 2208? Enter [2206] or [2208] only, please: "))
#     if inputted_season.isdigit():
#         SEASON = int(inputted_season)
#     if SEASON != (2206 | 2208):
#         print("Error: you didn't pick 2206 or 2208. System is setting it to 2206. If you need it to be 2208, please manually update dagmaker.rc.")
    
elif test == ('y'):
    #query the system so that we can tell what seasons are used
    query = """select distinct season from MARCELLE.SNAUTOSCAN union select distinct season from MARCELLE.SNAUTOSCAN_SAVE union select distinct season from MARCELLE.SNCAND union select distinct season from MARCELLE.SNFAKEIMG union select distinct season from MARCELLE.SNFAKEMATCH union select distinct season from MARCELLE.SNFORCE union select distinct season from MARCELLE.SNOBS union select distinct season from MARCELLE.SNOBSINFO union select distinct season from MARCELLE.SNOBS_SAVE union select distinct season from MARCELLE.SNSCAN ;""" 
    
    print('Querying the database for used season numbers...')
    DF=ea.connect('dessci').query_to_pandas(query)
    print('Query complete.')
    
    seasons = DF['SEASON']
    used_seasons = []
    a_list = list(range(1, 100))
    used_seasons.append(5000)
    used_seasons.append(6000)

    for number in a_list:
        used_seasons.append(number)

    for i in seasons:
        if i.is_integer():
            integer_value = int(i)
    else:
        integer_value = 0
    used_seasons.append(integer_value)

    #save the event data along with the season number 
    inputted_season = (raw_input("Enter desired season. If you have no input in mind, enter 'random': "))
    if inputted_season.isdigit():
        SEASON = int(inputted_season)
    elif inputted_season == ('random'):
        SEASON = 'random'
    else:
        raise Exception('Please make sure you have inputted a number or the word "random".')
        
    #for new data, start with asking for a user input of desired season. then check if season is redundant
    i = 0
    while i < 1: 

        print('Checking if season number is redundant...')
        
        if SEASON == ('random'):
            SEASON = random.randint(100, 10000)
            i = 0
        
        elif int(SEASON) in used_seasons:                         
            answer = (raw_input("Input matches previously used value. Proceeding with this Season input will overwrite previous files. Would you like to keep this input and overwrite previous files? [y/n]: "))
            answer_input = answer
            
            if answer_input == ('y'):
                answer_input_2 = (raw_input("Are you SURE you want to overwrite previous values and continue with this number? [y/n]: "))
                answer_2 = answer_input_2
                if answer_2 == ('y'):
                    new_season = (raw_input("YOU CAn'T OVERWRITE pRevIouS FILeS!!!! Pick a new number."))
                    new_value = int(new_season)
                    SEASON = new_value
                    i = 0   
                    
                else:
                    new_season = (raw_input("Please enter a new value for SEASON:"))
                    new_value = int(new_season)
                    SEASON = new_value
                    i = 0        
            elif answer_input != ('n'):  
                raise Exception('You gotta enter y or n or the code will break for now.')
            else:
                new_input = raw_input("Please enter a new value for SEASON:")
                SEASON = new_input
                i = 0
            
        elif SEASON not in used_seasons:
            print("Input accepted. SEASON = ", SEASON)
            i = 1
            
        else:
            raise ERROR("Something's gone wrong. Please restart and input a new season number.")
            
else:
    raise Exception('Please restart and enter [y/n]. ')
    



JOBSUBS_OPTS = None
RM_MYTEMP = None
JOBSUBS_OPTS_SE = None
RESOURCES = None
IGNORECALIB = None
DESTCACHE = None
TWINDOW = None
MIN_NITE = None
MAX_NITE = None
SKIP_INCOMPLETE_SE = None
DO_HEADER_CHECK = None

TEFF_CUT_g = None
TEFF_CUT_i = None
TEFF_CUT_r= None
TEFF_CUT_Y= None
TEFF_CUT_z= None
TEFF_CUT_u= None
# WRITE_DB= None
WRITEDB= None
list_parameters = [WRITEDB, TEFF_CUT_g, TEFF_CUT_i, TEFF_CUT_r, TEFF_CUT_Y, TEFF_CUT_z, TEFF_CUT_u, JOBSUBS_OPTS, RM_MYTEMP, JOBSUBS_OPTS_SE, RESOURCES, IGNORECALIB, DESTCACHE, TWINDOW, MIN_NITE, MAX_NITE, SKIP_INCOMPLETE_SE, DO_HEADER_CHECK]

def update_parameter(parameter):
    new_parameter_input = (raw_input("What would you like to update to? ")) 
    new_parameter = str(new_parameter_input)
    return new_parameter


    
def ask_restart():
    j = 0
    while j == 0:
        restart_or_no = (raw_input('Would you like to update parameters? [y/n/help] '))
        answer_restart = restart_or_no
        if restart_or_no == ('y'):
            update_more = (raw_input("If you know what parameter you'd like to update, type it here. For syntax/a list of possible updates, type 'help': "))
            update_value = update_more
            i = 0
            j = 1
        elif restart_or_no == ('n'):
            update_value = None
            i = 1
            j = 1
        elif restart_or_no == ('help'):
            update_value = 'help'
            i = 0
            j = 1
        else:
            print('Error. Please ensure you answer [y/n] when asked if you want to update values.')
            j = 0
            
    return {'i': i, 'update_value': update_value}
  
  
update_other_stuff = (raw_input("Would you like to update any other parameters? If you know something you'd like to update, type it here. Enter 'n' for no. For syntax/a list of possible updates, type 'help'." ))
update = update_other_stuff 

parameters_strings = ["RM_MYTEMP, JOBSUBS_OPTS, JOBSUBS_OPTS_SE, RESOURCES, IGNORECALIB, DESTCACHE, TEFF_CUT_g, TEFF_CUT_i, TEFF_CUT_r, TEFF_CUT_Y, TEFF_CUT_z, TEFF_CUT_u, TWINDOW, MIN_NITE, MAX_NITE, SKIP_INCOMPLETE_SE, DO_HEADER_CHECK, WRITEDB"]

i = 0
while i < 1:
    
    if update == ('help'):
        
        print("RM_MYTEMP, JOBSUBS_OPTS, JOBSUBS_OPTS_SE, RESOURCES, IGNORECALIB, DESTCACHE, TEFF_CUT_g, TEFF_CUT_i, TEFF_CUT_r, TEFF_CUT_Y, TEFF_CUT_z, TEFF_CUT_u, TWINDOW, MIN_NITE, MAX_NITE, SKIP_INCOMPLETE_SE, DO_HEADER_CHECK, WRITEDB")
        i_new = ask_restart()
        update = i_new['update_value']
        i = i_new['i']
        
    elif update == ('RM_MYTEMP'):
        RM_MYTEMP = update_parameter(RM_MYTEMP)
        new_values_rm = ask_restart()
        update = new_values_rm['update_value']
        i = new_values_rm['i']      
        
    elif (update == 'JOBSUBS_OPTS'):
        JOBSUBS_OPTS = update_parameter(JOBSUBS_OPTS)
        new_values_jo = ask_restart()
        update = new_values_jo['update_value']
        i = new_values_jo['i']
        
    elif (update == 'JOBSUBS_OPTS_SE'):
        JOBSUBS_OPTS_SE = update_parameter(JOBSUBS_OPTS_SE)
        new_values_jos = ask_restart()
        update = new_values_jos['update_value']
        i = new_values_jos['i']
        
    elif (update == 'RESOURCES'):
        RESOURCES = update_parameter(RESOURCES)
        new_values_r = ask_restart()
        update = new_values_r['update_value']
        i = new_values_r['i']
        
    elif (update == 'IGNORECALIB'):
        IGNORECALIB = update_parameter(IGNORECALIB)
        new_values_ic = ask_restart()
        update = new_values_ic['update_value']
        i = new_values_ic['i']
        
    elif (update == 'DESTCACHE'):
        DESTCACHE = update_parameter(DESTCACHE)
        new_values_dc = ask_restart()
        update = new_values_dc['update_value']
        i = new_values_dc['i']
        
    elif (update == 'TEFF_CUT_g'):
        TEFF_CUT_g = update_parameter(TEFF_CUT_g)
        new_values_tcg = ask_restart()
        update = new_values_tcg['update_value']
        i = new_values_tcg['i']
        
    elif (update == 'TEFF_CUT_i'):
        TEFF_CUT_i = update_parameter(TEFF_CUT_i)
        new_values_tci = ask_restart()
        update = new_values_tci['update_value']
        i = new_values_tci['i']

    elif (update == 'TEFF_CUT_r'):
        TEFF_CUT_r = update_parameter(TEFF_CUT_r)
        new_values_tcr = ask_restart()
        update = new_values_tcr['update_value']
        i = new_values_tcr['i']

    elif (update == 'TEFF_CUT_Y'):
        TEFF_CUT_Y = update_parameter(TEFF_CUT_Y)
        new_values_tcy = ask_restart()
        update = new_values_tcy['update_value']
        i = new_values_tcy['i']

    elif (update == 'TEFF_CUT_z'):
        TEFF_CUT_z = update_parameter(TEFF_CUT_z)
        new_values_tcz = ask_restart()
        update = new_values_tcz['update_value']
        i = new_values_tcz['i']

    elif (update == 'TEFF_CUT_u'):
        TEFF_CUT_u = update_parameter(TEFF_CUT_u)
        new_values_tcu = ask_restart()
        update = new_values_tcu['update_value']
        i = new_values_tcu['i']

    elif (update == 'TWINDOW'):
        TWINDOW = update_parameter(TWINDOW)
        new_values_tw = ask_restart()
        update = new_values_tw['update_value']
        i = new_values_tw['i']
        
    elif (update == 'MIN_NITE'):
        MIN_NITE = update_parameter(MIN_NITE)
        new_values_min_n = ask_restart()
        update = new_values_min_n['update_value']
        i = new_values_min_n['i']
        
    elif (update == 'MAX_NITE'):
        MAX_NITE = update_parameter(MAX_NITE)
        new_values_max_n = ask_restart()
        update = new_values_max_n['update_value']
        i = new_values_max_n['i']
        
    elif (update == 'SKIP_INCOMPLETE_SE'):
        SKIP_INCOMPLETE_SE = update_parameter(SKIP_INCOMPLETE_SE)
        new_values_skip = ask_restart()
        update = new_values_skip['update_value']
        i = new_values_skip['i']
        
    elif (update == 'DO_HEADER_CHECK'):
        DO_HEADER_CHECK= update_parameter(DO_HEADER_CHECK)
        new_values_header = ask_restart()
        update = new_values_header['update_value']
        i = new_values_header['i']
        
    elif (update == 'WRITEDB'):
        WRITEDB = update_parameter(WRITEDB)
        new_values_db = ask_restart()
        update = new_values_db['update_value']
        i = new_values_db['i']
        
    elif update == ('n'):
        i=1
        
    else:
        print('Error. Please enter a value.')
        new_values_error = ask_restart()
        update = new_values_error['update_value']
        i = new_values_error['i']
        i=1
        
filepath = 'dagmaker.rc'

    
# with open(filepath, 'r') as fp:
#     line = fp.readline()
#     cnt = 1
#     while line:
#          print("Line {}: {}".format(cnt, line.strip()))
#          line = fp.readline()
#          cnt += 1
            
with open(filepath, 'r') as file:
    data = file.readlines()

# for line in data:
#     for 
# data[18]=f'SEASON={SEASON}\n'
season_temp = str(SEASON)
data[18]='SEASON='+season_temp+'\n'
print('Printing your updates:')
print (data[18])

if  RM_MYTEMP != (None):
    data[23]='RM_MYTEMP='+RM_MYTEMP+'\n'
    print(data[23])
if  JOBSUBS_OPTS != (None):
    data[25]='JOBSUBS_OPTS='+JOBSUBS_OPTS+'\n'
    print(data[25])
if  JOBSUBS_OPTS_SE != (None):
    data[26]='JOBSUBS_OPTS_SE='+JOBSUBS_OPTS_SE+'\n'
    print(data[26])
if  WRITEDB != (None):
    data[20]='WRITEDB='+WRITEDB+'\n'
    print(data[20])
if  RESOURCES != (None):
    data[28]='RESOURCES='+RESOURCES+'\n'
    print(data[28])
if  IGNORECALIB != (None):
    data[29]='IGNORECALIB='+IGNORECALIB+'\n'
    print(data[29])
if  DESTCACHE != (None):
    data[30]='DESTCACHE='+DESTCACHE+'\n'
    print(data[30])
if  TWINDOW != (None):
    data[45]='TWINDOW='+TWINDOW+'\n'
    print(data[45])
if  SKIP_INCOMPLETE_SE != (None):
    data[57]='SKIP_INCOMPLETE_SE='+SKIP_INCOMPLETE_SE+'\n'
    print(data[57])
if  DO_HEADER_CHECK != (None):
    data[60]='DO_HEADER_CHECK='+DO_HEADER_CHECK+'\n'
    print(data[60])
    
if  TEFF_CUT_g != (None):
    data[39]='TEFF_CUT_g='+TEFF_CUT_g+'\n'
    print(data[39])
if  TEFF_CUT_i != (None):
    data[40]='TEFF_CUT_i='+TEFF_CUT_i+'\n'
    print(data[40])
if  TEFF_CUT_r != (None):
    data[41]='TEFF_CUT_r='+TEFF_CUT_r+'\n'
    print(data[41])
if  TEFF_CUT_Y != (None):
    data[42]='TEFF_CUT_Y='+TEFF_CUT_Y+'\n'
    print(data[42])
if  TEFF_CUT_z != (None):
    data[43]='TEFF_CUT_z='+TEFF_CUT_z+'\n'
    print(data[43])
if  TEFF_CUT_u != (None):
    data[44]='TEFF_CUT_u='+TEFF_CUT_u+'\n'
    print(data[44])

with open(filepath, 'w') as file:
     file.writelines( data )
      
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
            start_time_make_dag = time.time()
            command = [' ./DAGMaker.sh ' + exp_set[current_exp]]
#             command = ['pwd']
            
            #process for each command 
            print("Running " + command[0])
            process = subprocess.Popen(command[0], bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            f = open('dagmaker_'+exp_set[current_exp]+'.out', 'w')
            f.write(stdout)
            if stderr != None:
                f.write(stderr)
            f.close()
            #print so it knows its running 
            make_time = str((time.time() - start_time_make_dag)/60)
            print("All done with " + command[0] + '. It took ' + make_time + ' minutes.')
            
            cwd = os.getcwd()
            new_command = ['jobsub_submit_dag -G des --role=DESGW file://desgw_pipeline_' + exp_set[current_exp] + '.dag']
#             new_command = ['/cvmfs/fermilab.opensciencegrid.org/products/common/prd/jobsub_client/v1_3/NULL/jobsub_submit_dag -G des --role=DESGW file://desgw_pipeline_' + exp_set[current_exp] + '.dag']

            
            filepath = [cwd + 'desgw_pipeline_' + exp_set[current_exp] + '.dag']
            isExist = os.path.exists(filepath[0])
    
            if not isExist:
                
#         subprocess.check_output(new_command[0], stderr=subprocess.STDOUT)
                print('Something went wrong with finding the desgw_pipeline.dag file for exposure'+exp_set[current_exp]+'.This may be an issue with the python file, but if submitting the dag does not work, this could be why.')
        
            path = '/cvmfs/fermilab.opensciencegrid.org/products/common/prd/jobsub_client/v1_3/NULL/jobsub_submit_dag'
#                 path = 'jobsub_submit_dag'
            new_command = [path + ' -G des --role=DESGW file://desgw_pipeline_' + exp_set[current_exp] + '.dag']
            start_time_submit_dag = time.time()
            print("Running" + new_command[0])
#             os.system(new_command[0])
            process = subprocess.Popen(new_command[0], bufsize=1, shell=True, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            f = open('dag_submission_'+exp_set[current_exp]+'.out', 'w')
            f.write(stdout)
            if stderr != None:
                f.write(stderr)
            f.close()
            submit_time = str((time.time() - start_time_submit_dag)/60)
            print("All done with " + new_command[0] + '. It took ' + submit_time + ' minutes.')

#             print("Running" + new_command[0])
#             os.system(new_command[0])
            
            finished_exps.put(exp_set[current_exp] + ' is done by ' + current_process().name)
           
            #pause the system for a bit so that it has time to finish executing 
            time.sleep(0.5)
    return True

#source setup img proc, necessary for dagmaker
cwd = os.getcwd()
command_source = ['source '+cwd+'/setup_img_proc.sh']
print('Running'+command_source[0])
#ensure that source setup_diff_img will run by checking if there's a system error raised. 
output = os.system(command_source[0])
if output == 0:
    os.system(command_source[0])
else:
    print('Module os.sys returned the error:')
    print(output)
    raise ValueError('Something went wrong with setup_img_proc.sh. Please manually run or try again.')
    
inputted_exp_list = (raw_input("Please input the filepath to your exp.list file, relative to gw_workflow or as a full path: "))

isExist = os.path.exists(inputted_exp_list)
    
#         subprocess.check_output(new_command[0], stderr=subprocess.STDOUT)
        
if not isExist:
        inputted_exp_list = (raw_input("Error, could not find your .list file. Please check location then input the filepath relative to gw_workflow one more time: "))

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

command_2 = ['. /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh']
print("Running" + command_2[0])
output = os.system(command_2[0])
if output == 0:
    os.system(command_2[0])
else:
    print('os.system returned error number:')
    print(output)
    raise ValueError('Something went wrong with the command. Please manually run or try again.')
    
    
path_find_list = sys.path
pycurl_path = '/cvmfs/fermilab.opensciencegrid.org/products/common/prd/pycurl/v7_16_4/Linux64bit-2-6-2-12/pycurl'
if pycurl_path in path_find_list:
    print('Found the path to pycurl.')
if pycurl_path not in path_find_list:
    print('Missing path to pycurl. Appending path now.')
    sys.path.append('/cvmfs/fermilab.opensciencegrid.org/products/common/prd/pycurl/v7_16_4/Linux64bit-2-6-2-12/pycurl')



start_time_multiproc = time.time()
if __name__ == '__main__':

    main()
    
finish_time = str((time.time() - start_time_multiproc)/60)
print('Finished with dag creation and submission. Multiprocessing took '+finish_time+' minutes.')

with open('exposures.list') as f:
    data = []
    for line in f: 
        for exp in exp_set:
            if exp in line:
                data.append(line)

exp_info = []
nite = []

for line in data:
    data_extracted = line.split()
    exp_info.append(data_extracted[0] + ' ' + data_extracted[5])
    nite.append(data_extracted[1])

# Make the output dir if it doesn't exist



list_info = [str(exp_info), str(SEASON)]


output_dir_exists = os.path.exists('./image_proc_outputs/')
if output_dir_exists:
    file_exists = os.path.exists('./image_proc_outputs/output.txt')
    if file_exists:
        with open('./image_proc_outputs/output.txt', 'w') as file:
            for item in list_info:
                file.write("%s\n" % item)
      
                
            file.close
    else:
 
        f = open('./image_proc_outputs/output.txt', 'a')
        f.write(str(exp_info + '\n' + SEASON + '\n'))
        f.close()
        
else:
    os.mkdir('./image_proc_outputs/')
    f = open('./image_proc_outputs/output.txt', 'a')
    for item in list_info:
                f.write("%s\n" % item)
      
                
    f.close
#exp_info is your list with (exp_num band, exp_num band), other_info is the nite and other info that i will get from dagmaker upon combining codes

print('Image processing completed.')
