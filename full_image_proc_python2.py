import os
import sys
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

parser.add_argument('--test_season', metavar='ts', action='store',
                    help='answers whether user would like to use a test season [y/n]')
parser.add_argument('--exp_list_location', metavar='exp', action='store',
                    help='Necessary parameter. Input location of an exposure.list file, relative to gw_workflow (which should be located at ../gw_workflow)')

parser.add_argument('--test_season_number', metavar='tsn', action='store',
                    help='specifies test season number to be used. possible values are 2206 and 2208 (default: 2206)')

parser.add_argument('--season', metavar='s', action='store',
                    help='specifies desired season number. if "random" is input for this parameter, a suitable season number will be chosen by the system.')

parser.add_argument('--RM_MYTEMP', metavar='rm', action='store',
                    help='When true, DAGMaker will delete a pre-existing mytemp dir for the exposure and rerun. If you are running this dagmaker process again, this should be set to true in order to not confuse the pipeline. Syntax for update is [true/false]')

parser.add_argument('--JOBSUB_OPTS', metavar='jo', action='store',
                    help='Updates JOBSUB_OPTS. The default for this value likely does not need to be changed')

parser.add_argument('--RESOURCES', metavar='re', action='store',
                    help='Updates RESOURCES. The default for this value likely does not need to be changed')

parser.add_argument('--IGNORECALIB', metavar='ic', action='store',
                    help='Updates IGNORECALIB. Syntax is [true/false]')

parser.add_argument('--WRITEDB', metavar='wdb', action='store',
                    help='Updates WRITEDB. WRITEDB should be off for initial testing, but on for any production running. Syntax is [on/off].')

parser.add_argument('--JOBSUB_OPTS_SE', metavar='jos', action='store',
                    help='Updates JOBSUB_OPTS_SE. The default for this value likely does not need to be changed.')

parser.add_argument('--DESTCACHE', metavar='desc', action='store',
                    help='Updates DESTCACHE.')

parser.add_argument('--TEFF_CUT_g', metavar='g', action='store',
                    help='Updates TEFF_CUT_g.')

parser.add_argument('--TEFF_CUT_i', metavar='i', action='store',
                    help='Updates TEFF_CUT_i.')

parser.add_argument('--TEFF_CUT_r', metavar='r', action='store',
                    help='Updates TEFF_CUT_r.')

parser.add_argument('--TEFF_CUT_Y', metavar='Y', action='store',
                    help='Updates TEFF_CUT_Y.')

parser.add_argument('--RNUM', metavar='rn', action='store',
                    help='Specifies rnum parameter for dagmaker')

parser.add_argument('--PNUM', metavar='pn', action='store',
                    help='Specifies pnum parameter for dagmaker')

parser.add_argument('--TEFF_CUT_z', metavar='z', action='store',
                    help='Updates TEFF_CUT_z.')

parser.add_argument('--TEFF_CUT_u', metavar='u', action='store',
                    help='Updates TEFF_CUT_u.')

parser.add_argument('--TWINDOW', metavar='twin', action='store',
                    help='Updates TWINDOW.')

parser.add_argument('--MIN_NITE', metavar='min', action='store',
                    help='ONLY use this option if you have nothing but late-time templates. It should be commented out for standard nightly diffim running.')

parser.add_argument('--MAX_NITE', metavar='max', action='store',
                    help='Use only if you want to avoid using images taken after MAX_NITE as templates.')

parser.add_argument('--DO_HEADER_CHECK', metavar='dhc', action='store',
                    help='Turn off header check for FIELD, OBJECT, TILING if you want save time. Can do that if you have already fixed the headers elsewhere (for example when copying from DESDM)')

parser.add_argument('--SKIP_INCOMPLETE_SE', metavar='dhc', action='store',
                    help='updates skip_incomplete_se')

#parse arguments so each can be called as a variable with args.name
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
      
# figure out if pipeline testing suite is present. if not, clone it one folder back
# filepath = ['../DESGW-Pipeline-Testing-Suite']
# isExist = os.path.exists(filepath[0])
# if isExist:
#       print('Pipeline testing suite found.')
# else:
#       logging.debug('Pipeline testing suite not found. Code is cloning it at ../DESGW-Pipeline-Testing-Suite')
#       git_command = ['git clone https://github.com/SSantosLab/DESGW-Pipeline-Testing-Suite.git ../DESGW-Pipeline-Testing-Suite' ]
#       print('Running '+git_command[0]+'in order to clone necessary folder DESGW-Pipeline-Testing-Suite...')
#       git_output = os.system(git_command[0])

#       if git_output != 0:
#             logging.debug('Something went wrong with finding and/or cloning DESGW-Pipeline-Testing-Suite. Aborting...')
#             raise ValueError('Something went wrong with cloning DESGW-Pipeline-Testing-Suite. Please clone it manually or try again.')
            
#check if user is running as desgw or through a proxy
USER = os.getenv('USER')
if USER == 'desgw':
    dagmaker_file = './DAGMaker.sh '
    print('You are running as desgw.')
else:
    dagmaker_file = './DAGMaker_proxyuser.sh '
    print('You are not running as desgw.')

            
#changing directory because this code needs to run in gw_workflow
os.chdir('../gw_workflow')
logging.info('Code now running in gw_workflow.')

#checking to see if exposures.list has been pulled from db
exp_file = ['exposures.list']
if os.path.exists(exp_file[0]):
    print('Found exposures.list. Checking to see if it is empty...')
    if os.path.getsize(exp_file[0]) == 0:
        print("No data found in exposures.list file! Getting the file... (this may take a while)") 
        print('Filesize of exposures.list was empty, pulling it from db')
        os.system('./getExposureInfo.sh')
elif not os.path.exists(exp_file[0]):
        print('Missing exposures.list file. getting the file... (this may take a while)')
        os.system('./getExposureInfo.sh')

# #necessary sourcing
# os.environ["JOBSUB_PYVER"] = "python2.7-ucs4"
# #put in other file stuff
# os.environ["SHELL"]= "/usr/bin/env bash -c 'which bash'"
# os.environ["CC"]="/usr/bin/gcc"
# os.environ["CXX"]="/usr/bin/g++"
# os.environ["GFORTRAN"]="/usr/bin/gfortran"

# os.environ["EUPS_PKGROOT"]="http://desbuild2.cosmology.illinois.edu/eeups/webservice/repository"
# os.environ["SVNROOT"]="https://dessvn.cosmology.illinois.edu/svn/desdm/devel"
# os.environ["EUPS_DIR"]="/cvmfs/des.opensciencegrid.org/2015_Q2/eeups/SL6/eups/1.2.30"

# os.environ["PATH"] = "/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/curl/v7_64_1/Linux64bit-3-10/bin:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/jobsub_client/v1_3_5/NULL:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ups/v6_0_7/Linux64bit-3-10-2-17/bin:/cvmfs/des.opensciencegrid.org/fnal/anaconda2/condabin:/usr/lib64/qt-3.3/bin:/opt/puppetlabs/bin:/home/s1/desgw/perl5/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/opt/puppetlabs/bin"
# os.environ["PYTHONPATH"] = "/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/python_future_six_request/v1_3/Linux64bit-3-10-2-17-python2-7-ucs4:/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/jobsub_client/v1_3_5/NULL"
# os.environ["LIBRARY_PATH"]="/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/curl/v7_64_1/Linux64bit-3-10/lib"
# os.environ["IFDH_NO_PROXY"]="1"
# os.environ["IFDHC_LIB"]="/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_6_8/Linux64bit-3-10-2-17/lib"
# os.environ["IFDHC_LIB"]="/cvmfs/fermilab.opensciencegrid.org/products/common/db/../prd/ifdhc/v2_6_8/Linux64bit-3-10-2-17/lib"
# os.environ["IFDHC_FQ_DIR"]="/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc/v2_6_8/Linux64bit-3-10-2-17"
# os.environ["IFDHC_CONFIG_DIR"]="/cvmfs/fermilab.opensciencegrid.org/products/common/prd/ifdhc_config/v2_6_8/NULL"
# os.environ["CIGETCERTLIBS_DIR"]="/cvmfs/fermilab.opensciencegrid.org/products/common/prd/cigetcertlibs/v1_1/Linux64bit-3-10-2-17"
# os.environ["EUPS_PATH"]="/cvmfs/des.opensciencegrid.org/eeups/fnaleups:$EUPS_PATH"
# #test season numbers are 2206 and 2208
# #test_season_check = (raw_input("Would you like to use a test season number? [y/n]: "))


test_season_check = str(args.test_season)
test_season = test_season_check

#if using a test season number, set it as a default of 2208. it can also be updated to one of the other test season numbers, 2206, 2208, or 2211
if test_season == ('y'):
    SEASON = 2208
#     print('Double checking SEASON is set to 2208. If you need a different season, please restart or manually update dagmaker.rc before you enter your exposures.list file later in the code.')
    #inputted_season = (raw_input("Do you want your season to be 2206, 2211, or 2208? Enter [2206], [2211] or [2208] only, please: "))
    inputted_season = str(args.test_season_number)
    if inputted_season.isdigit():
        SEASON = int(inputted_season)
        print(SEASON)
    if SEASON == (2206):
        print('Success! Season will update to 2206.')
    elif SEASON == (2208):
        print('Success! Season will update to 2208.')
    elif SEASON == (2211):
        print('Success! Season will update to 2211.')
    else:
        logging.warning('User specified that they wanted a test season number, of which there are only three (2206, 2211, and 2208). They did not pick either of those, so the code is automatically setting it to 2208.')
        print("Error: you didn't pick 2206, 2211, or 2208. System is setting it to 2208. If you need it to be something else, please manually update dagmaker.rc.")
        SEASON = 2208

#if not using a test season number, check and make sure the inputted season works
elif test_season == ('n'):
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
#         inputted_season = (raw_input("Enter desired season. If you have no input in mind, enter 'random': "))
    inputted_season = (str(args.season))
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
                new_season = (raw_input("Please enter a new value for SEASON:"))
                new_value = int(new_season)
                SEASON = new_value
                i=0
            else:
                new_input = raw_input("Please enter a new value for SEASON:")
                SEASON = new_input
                i = 0

        elif SEASON not in used_seasons:
            print("Input accepted. SEASON = ", SEASON)
            i = 1

        else:
            raise ERROR("Something's gone wrong. Please restart and input a new season number.")
            logging.debug('Something went wrong with choosing a unique season value.')

# else:
#     raise Exception('Please restart and enter [y/n]. ')



#initialize parameters as being none
# JOBSUB_OPTS = None
# RM_MYTEMP = None
# JOBSUB_OPTS_SE = None
# RESOURCES = None
# IGNORECALIB = None
# DESTCACHE = None
# TWINDOW = None
# MIN_NITE = None
# MAX_NITE = None
# SKIP_INCOMPLETE_SE = None
# DO_HEADER_CHECK = None
# TEFF_CUT_g = None
# TEFF_CUT_i = None
# TEFF_CUT_r= None
# TEFF_CUT_Y= None
# TEFF_CUT_z= None
# TEFF_CUT_u= None
# WRITEDB= None

#list_parameters = [WRITEDB, TEFF_CUT_g, TEFF_CUT_i, TEFF_CUT_r, TEFF_CUT_Y, TEFF_CUT_z, TEFF_CUT_u, JOBSUB_OPTS, RM_MYTEMP, JOBSUB_OPTS_SE, RESOURCES, IGNORECALIB, DESTCACHE, TWINDOW, MIN_NITE, MAX_NITE, SKIP_INCOMPLETE_SE, DO_HEADER_CHECK]

#     def update_parameter(parameter):

#         new_parameter_input = (raw_input("What would you like to update to? ")) 
#         new_parameter = str(new_parameter_input)
#         return new_parameter



#     def ask_restart():
#         j = 0
#         while j == 0:
#             restart_or_no = (raw_input('Would you like to continue to update parameters? [y/n/help] '))
#             answer_restart = restart_or_no
#             if restart_or_no == ('y'):
#                 update_more = (raw_input("If you know what parameter you'd like to update, type it here. For syntax/a list of possible updates, type 'help': "))
#                 update_value = update_more
#                 i = 0
#                 j = 1
#             elif restart_or_no == ('n'):
#                 update_value = None
#                 i = 1
#                 j = 1
#             elif restart_or_no == ('help'):
#                 update_value = 'help'
#                 i = 0
#                 j = 1
#             else:
#                 print('Error. Please ensure you answer [y/n] when asked if you want to update values.')
#                 j = 0

#         return {'i': i, 'update_value': update_value}


#     update_other_stuff = (raw_input("If you know a dagmaker.rc parameter you'd like to update besides season, type it here. Enter 'n' for no if you are done updating dagmaker. For syntax/a list of possible updates, type 'help'." ))
#     update = update_other_stuff 

#     parameters_strings = ["RM_MYTEMP, JOBSUB_OPTS, JOBSUB_OPTS_SE, RESOURCES, IGNORECALIB, DESTCACHE, TEFF_CUT_g, TEFF_CUT_i, TEFF_CUT_r, TEFF_CUT_Y, TEFF_CUT_z, TEFF_CUT_u, TWINDOW, MIN_NITE, MAX_NITE, SKIP_INCOMPLETE_SE, DO_HEADER_CHECK, WRITEDB"]

#     i = 0
#     while i < 1:

#         if update == ('help'):

#             print(parameters_strings)
#             i_new = ask_restart()
#             update = i_new['update_value']
#             i = i_new['i']

#         elif update == ('RM_MYTEMP'):
#             print('RM_MYTEMP update notes: When true, DAGMaker will delete a pre-existing mytemp dir for the exposure and rerun. If you are running this dagmaker process again, this should be set to true in order to not confuse the pipeline. Syntax for update is [true/false].')
#             RM_MYTEMP = update_parameter(RM_MYTEMP)
#             new_values_rm = ask_restart()
#             update = new_values_rm['update_value']
#             i = new_values_rm['i']      

#         elif (update == 'JOBSUB_OPTS'):
#             ask_sure = raw_input('JOBSUB_OPTS update notes: The default for this value likely does not need to be changed. Are you sure you want to proceed? [y/n]: ')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 JOBSUB_OPTS = update_parameter(JOBSUB_OPTS)
#                 new_values_jo = ask_restart()
#                 update = new_values_jo['update_value']
#                 i = new_values_jo['i']
#             else:
#                 print('No longer updating JOBSUB_OPTS.')
#                 new_values_jo = ask_restart()
#                 update = new_values_jo['update_value']
#                 i = new_values_jo['i']

#         elif (update == 'JOBSUB_OPTS_SE'):
#             ask_sure = raw_input('JOBSUB_OPTS_SE update notes: The default for this value likely does not need to be changed. Are you sure you want to proceed? [y/n]: ')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 JOBSUB_OPTS_SE = update_parameter(JOBSUB_OPTS_SE)
#                 new_values_jos = ask_restart()
#                 update = new_values_jos['update_value']
#                 i = new_values_jos['i']
#             else:
#                 print('No longer updating JOBSUB_OPTS_SE.')
#                 new_values_jos = ask_restart()
#                 update = new_values_jos['update_value']
#                 i = new_values_jos['i']

#         elif (update == 'RESOURCES'):
#             ask_sure = raw_input('RESOURCES update notes: The default for this value likely does not need to be changed. Are you sure you want to proceed? [y/n]: ')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 RESOURCES = update_parameter(RESOURCES)
#                 new_values_r = ask_restart()
#                 update = new_values_r['update_value']
#                 i = new_values_r['i']
#             else:
#                 print('No longer updating RESOURCES.')
#                 new_values_r = ask_restart()
#                 update = new_values_r['update_value']
#                 i = new_values_r['i']

#         elif (update == 'IGNORECALIB'):
#             print('IGNORECALIB update note: Syntax is [true/false].')
#             IGNORECALIB = update_parameter(IGNORECALIB)
#             new_values_ic = ask_restart()
#             update = new_values_ic['update_value']
#             i = new_values_ic['i']

#         elif (update == 'DESTCACHE'):
#             DESTCACHE = update_parameter(DESTCACHE)
#             new_values_dc = ask_restart()
#             update = new_values_dc['update_value']
#             i = new_values_dc['i']

#         elif (update == 'TEFF_CUT_g'):
#             TEFF_CUT_g = update_parameter(TEFF_CUT_g)
#             new_values_tcg = ask_restart()
#             update = new_values_tcg['update_value']
#             i = new_values_tcg['i']

#         elif (update == 'TEFF_CUT_i'):
#             TEFF_CUT_i = update_parameter(TEFF_CUT_i)
#             new_values_tci = ask_restart()
#             update = new_values_tci['update_value']
#             i = new_values_tci['i']

#         elif (update == 'TEFF_CUT_r'):
#             TEFF_CUT_r = update_parameter(TEFF_CUT_r)
#             new_values_tcr = ask_restart()
#             update = new_values_tcr['update_value']
#             i = new_values_tcr['i']

#         elif (update == 'TEFF_CUT_Y'):
#             TEFF_CUT_Y = update_parameter(TEFF_CUT_Y)
#             new_values_tcy = ask_restart()
#             update = new_values_tcy['update_value']
#             i = new_values_tcy['i']

#         elif (update == 'TEFF_CUT_z'):
#             TEFF_CUT_z = update_parameter(TEFF_CUT_z)
#             new_values_tcz = ask_restart()
#             update = new_values_tcz['update_value']
#             i = new_values_tcz['i']

#         elif (update == 'TEFF_CUT_u'):
#             TEFF_CUT_u = update_parameter(TEFF_CUT_u)
#             new_values_tcu = ask_restart()
#             update = new_values_tcu['update_value']
#             i = new_values_tcu['i']

#         elif (update == 'TWINDOW'):
#             TWINDOW = update_parameter(TWINDOW)
#             new_values_tw = ask_restart()
#             update = new_values_tw['update_value']
#             i = new_values_tw['i']

#         elif (update == 'MIN_NITE'):
#             print('ONLY use this option if you have nothing but late-time templates. It should be commented out for standard nightly diffim running.')
#             ask_sure = raw_input('Are you sure you want to proceed with updating MIN_NITE? [y/n]: ')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 MIN_NITE = update_parameter(MIN_NITE)
#                 new_values_min_n = ask_restart()
#                 update = new_values_min_n['update_value']
#                 i = new_values_min_n['i']
#             else:
#                 print('No longer updating MIN_NITE.')
#                 new_values_min_n = ask_restart()
#                 update = new_values_min_n['update_value']
#                 i = new_values_min_n['i']

#         elif (update == 'MAX_NITE'):
#             print('Use only if you want to avoid using images taken after MAX_NITE as templates.')
#             ask_sure = raw_input('Are you sure you want to proceed with updating MIN_NITE? [y/n]: ')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 MAX_NITE = update_parameter(MAX_NITE)
#                 new_values_max_n = ask_restart()
#                 update = new_values_max_n['update_value']
#                 i = new_values_max_n['i']
#             else:
#                 print('No longer updating MAX_NITE.')
#                 new_values_max_n = ask_restart()
#                 update = new_values_max_n['update_value']
#                 i = new_values_max_n['i']

#         elif (update == 'SKIP_INCOMPLETE_SE'):
#             print('SKIP_INCOMPLETE_SE parameter info: Syntax is [true/false].')
#             SKIP_INCOMPLETE_SE = update_parameter(SKIP_INCOMPLETE_SE)
#             new_values_skip = ask_restart()
#             update = new_values_skip['update_value']
#             i = new_values_skip['i']

#         elif (update == 'DO_HEADER_CHECK'):
#             print('DO_HEADER_CHECK info: Turn off header check for FIELD, OBJECT, TILING if you want save time. Can do that if you have already fixed the headers elsewhere (for example when copying from DESDM).')
#             ask_sure = raw_input('Are you sure you want to proceed with updating DO_HEADER_CHECK? [y/n]')
#             answer_sure = ask_sure
#             if answer_sure == 'y':
#                 print('Syntax is [0/1].')
#                 DO_HEADER_CHECK= update_parameter(DO_HEADER_CHECK)
#                 new_values_header = ask_restart()
#                 update = new_values_header['update_value']
#                 i = new_values_header['i']
#             else:
#                 print('No longer updating DO_HEADER_CHECK.')
#                 new_values_header = ask_restart()
#                 update = new_values_header['update_value']
#                 i = new_values_header['i']

#         elif (update == 'WRITEDB'):
#             print('WRITEDB info: WRITEDB should be off for initial testing, but on for any production running. Syntax is [on/off].')
#             WRITEDB = update_parameter(WRITEDB)
#             new_values_db = ask_restart()
#             update = new_values_db['update_value']
#             i = new_values_db['i']

#         elif update == ('n'):
#             i=1

#         else:
#             print('Error. Value/parameter not recognized. Please enter a value, and type help when prompted if you need syntax help.')
#             new_values_error = ask_restart()
#             update = new_values_error['update_value']
#             i = new_values_error['i']



filepath = 'dagmaker.rc'

#read in dagmaker file so it knows where to update lines
with open(filepath, 'r') as file:
    data = file.readlines()

season_temp = str(SEASON)
data[18]='SEASON='+season_temp+'\n'
print('Season update:')
print (data[18])

#update all parameters if a change was inputted as an argument
if  args.RM_MYTEMP != (None):
    rm_line = ()
    for index, line in enumerate(data):
        if line.startswith('RM_MYTEMP='):
            rm_line = int(index)
            print('Line '+line+'changed to:')
    data[rm_line] = 'RM_MYTEMP='+str(args.RM_MYTEMP)+'\n'
    print(data[rm_line])

if  (args.MAX_NITE) != (None):
    mn_line = ()
    for index, line in enumerate(data):
        if ('MAX_NITE=') in line:
            mn_line = int(index)
            print('Line '+line+'changed to:')
    data[mn_line] = 'MAX_NITE='+str(args.MAX_NITE)+'\n'
    print(data[mn_line])

if  (args.MIN_NITE) != (None):
    mi_line = ()
    for index, line in enumerate(data):
        if ('MAX_NITE=') in line:
            mi_line = int(index)
            print('Line '+line+'changed to:')
    data[mi_line] = 'MIN_NITE='+str(args.MIN_NITE)+'\n'
    print(data[mi_line])

if  (args.PNUM) != (None):
    job_line = ()
    for index, line in enumerate(data):
        if line.startswith('PNUM='):
            job_line = int(index)
            print('Line '+line+'updated to:')
    data[job_line] = 'PNUM='+str(args.PNUM)+'\n'
    print(data[job_line])
if  (args.RNUM) != (None):
    job_line = ()
    for index, line in enumerate(data):
        if line.startswith('RNUM='):
            job_line = int(index)
            print('Line '+line+'updated to:')
    data[job_line] = 'RNUM='+str(args.RNUM)+'\n'
    print(data[job_line])

if  (args.JOBSUB_OPTS) != (None):
    job_line = ()
    for index, line in enumerate(data):
        if line.startswith('JOBSUB_OPTS='):
            job_line = int(index)
            print('Line '+line+'updated to:')
    data[job_line] = 'JOBSUB_OPTS='+str(args.JOBSUB_OPTS)+'\n'
    print(data[job_line])

if  (args.JOBSUB_OPTS_SE) != (None):
    jobse_line = ()
    for index, line in enumerate(data):
        if line.startswith('JOBSUB_OPTS_SE='):
            jobse_line = int(index)
            print('Line '+line+'updated to:')
    data[jobse_line] = 'JOBSUB_OPTS_SE='+str(args.JOBSUB_OPTS_SE)+'\n'
    print(data[jobse_line])

if  (args.WRITEDB) != (None):
    db_line = ()
    for index, line in enumerate(data):
        if line.startswith('WRITEDB='):
            db_line = int(index)
            print('Line '+line+'updated to:')
    data[db_line] = 'WRITEDB='+str(args.WRITEDB)+'\n'
    print(data[db_line])

if  (args.RESOURCES) != (None):
    r_line = ()
    for index, line in enumerate(data):
        if line.startswith('RESOURCES='):
            r_line = int(index)
            print('Line '+line+'updated to:')
    data[r_line] = 'RESOURCES='+RESOURCES+'\n'
    print(data[r_line])

if  (args.IGNORECALIB) != (None):
    i_index = ()
    for index, line in enumerate(data):
        if line.startswith('IGNORECALIB='):
            i_index = int(index)
            print('Line '+line+'updated to:')
    data[i_index] = 'IGNORECALIB='+str(args.IGNORECALIB)+'\n'
    print(data[i_index])

if  (args.DESTCACHE) != (None):
    d_index = ()
    for index, line in enumerate(data):
        if line.startswith('DESTCACHE='):
            d_index = int(index)
            print('Line '+line+'updated to:')
    data[d_index] = 'DESTCACHE='+str(args.DESTCACHE)+'\n'
    print(data[d_index])

if (args.TWINDOW) != (None):
    t_index = ()
    for index, line in enumerate(data):
        if line.startswith('TWINDOW='):
            t_index = int(index)
            print('Line '+line+'updated to:')
    data[t_index] = 'TWINDOW='+str(args.TWINDOW)+'\n'
    print(data[t_index])


if  (args.SKIP_INCOMPLETE_SE) != (None):
    se_index = ()
    for index, line in enumerate(data):
        if line.startswith('SKIP_INCOMPLETE_SE='):
            se_index = int(index)
            print('Line '+line+'updated to:')
    data[se_index] = 'SKIP_INCOMPLETE_SE='+str(args.SKIP_INCOMPLETE_SE)+'\n'
    print(data[se_index])    

if  (args.DO_HEADER_CHECK) != (None):
    head_index = ()
    for index, line in enumerate(data):
        if line.startswith('DO_HEADER_CHECK='):
            head_index = int(index)
            print('Line '+line+'updated to:')
    data[head_index] = 'DO_HEADER_CHECK='+str(args.DO_HEADER_CHECK)+'\n'
    print(data[head_index])     

if  (args.TEFF_CUT_g) != (None):
    tg_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_g='):
            tg_index = int(index)
            print('Line '+line+'updated to:')
    data[tg_index] = 'TEFF_CUT_g='+str(args.TEFF_CUT_g)+'\n'
    print(data[tg_index])   

if  (args.TEFF_CUT_i) != (None):
    ti_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_i='):
            ti_index = int(index)
            print('Line '+line+'updated to:')
    data[ti_index] = 'TEFF_CUT_i='+str(args.TEFF_CUT_i)+'\n'
    print(data[ti_index])

if  (args.TEFF_CUT_r) != (None):
    tr_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_r='):
            tr_index = int(index)
            print('Line '+line+'updated to:')
    data[tr_index] = 'TEFF_CUT_r='+str(args.TEFF_CUT_r)+'\n'
    print(data[tr_index])

if  (args.TEFF_CUT_Y) != (None):
    ty_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_Y='):
            ty_index = int(index)
            print('Line '+line+'updated to:')
    data[ty_index] = 'TEFF_CUT_Y='+str(args.TEFF_CUT_Y)+'\n'
    print(data[ty_index])

if  (args.TEFF_CUT_z) != (None):
    tz_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_z='):
            tz_index = int(index)
            print('Line '+line+'updated to:')
    data[tz_index] = 'TEFF_CUT_z='+str(args.TEFF_CUT_z) +'\n'
    print(data[tz_index])

if  (args.TEFF_CUT_u) != (None):
    tu_index = ()
    for index, line in enumerate(data):
        if line.startswith('TEFF_CUT_u='):
            tu_index = int(index)
            print('Line '+line+'updated to:')
    data[tu_index] = 'TEFF_CUT_u='+str(args.TEFF_CUT_u)+'\n'
    print(data[tu_index])


with open(filepath, 'w') as file:
     file.writelines( data )
     file.close()

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
            path_find_list = sys.path

            #initialize command
            start_time_make_dag = time.time()
            command = [dagmaker_file + exp_set[current_exp]]
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
#                print so it knows its running 
            make_time = str((time.time() - start_time_make_dag)/60)
            print("All done with " + command[0] + '. It took ' + make_time + ' minutes.')

            cwd = os.getcwd()
            string_command = 'file://desgw_pipeline_'+ exp_set[current_exp] + '.dag'

            if USER == 'desgw':
              new_command = ['jobsub_submit_dag', '-G des', '--role=desgw',  string_command]
            else:
              new_command = ['jobsub_submit_dag', '-G des', '--role=DESGW',  string_command]
            start_time_submit_dag = time.time()
            print("Running" + new_command[0])
            #os.system(new_command)
            process = subprocess.Popen(new_command, bufsize=1, shell=False, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            stdout, stderr = process.communicate()
            f = open('dag_submission_'+exp_set[current_exp]+'.out', 'w')
            f.write(stdout)
            if stderr != None:
                f.write(stderr)
                print('Errors found in dag submission; dags may have to be submitted manually. Please check the dag_submission.out files for more information.')
            f.close()
            submit_time = str((time.time() - start_time_submit_dag)/60)
            print("All done with " + new_command[0] + '. It took ' + submit_time + ' minutes.')


            finished_exps.put(exp_set[current_exp] + ' is done by ' + current_process().name)

            #pause the system for a bit so that it has time to finish executing 
            time.sleep(0.5)
    return True


# if bench_criteria:
#     print("We haven't made the benchmark test exp list yet. This is a placeholder.")

#     inputted_exp_list = (raw_input("Please input the filepath to your exp.list file, relative to gw_workflow or as a full path: "))

inputted_exp_list = str(args.exp_list_location)

i=0    
while i < 1:
    isExist = os.path.exists(inputted_exp_list)

    if not isExist:
            inputted_exp_list = (raw_input("Error, could not find your .list file. Please check location then input the filepath relative to gw_workflow one more time: "))
            logging.warning("User's inputted exposure list wasn't found. Inputted list was" + inputted_exp_list)


    else:
            i=1

logging.info("Inputted exposure list is " + inputted_exp_list)
# filepath = 'exposures_jul27.list'
filepath = inputted_exp_list
sample_exp_set = EXPlist(filepath)
number_exps = len(sample_exp_set)

#initializing information for the processing function arguments
number_of_tasks = number_exps
#number of processes is how many to submit/run at one time 
number_of_processes = 5
exp_set = sample_exp_set

#create and submit dags, multiprocessing
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


start_time_multiproc = time.time()
if __name__ == '__main__':

    main()

finish_time = str((time.time() - start_time_multiproc)/60)
print('Finished with dag creation and submission. Multiprocessing took '+finish_time+' minutes.')


print('Image processing completed.')
