#!/usr/bin/env python
# coding: utf-8

# In[133]:


import os
import sys
import pandas
import glob
import multiprocessing as mlp
import easyaccess
import configparser
import argparse
import shutil
import datetime


# In[134]:


# Get a list of exposures
# Get season
# Check if they've finished in image processing pipeline
# Document and report what hasn't
# Figure out why it didn't finish it
# Build appropriate postproc.ini file
# Check if user wants to run from scratch or start wherever the pipeline failed last
# If the latter, deduce where the pipeline finished off last, set as SKIPTO flag
# Run post-processing pipeline


# In[135]:


#Read file outputted from image processing to get season, nite, exposure, band
#If no file, input manually

try:
    print('Extracting season, nite, exposure, and band from sampeoutput.txt file\n')
    img_proc_file = open("sampleoutput.txt")
    lines = img_proc_file.readlines()

    exposures = lines[0].strip()
    exposures = exposures[1:-1]
    exposures = list(exposures.split(","))

    nite = lines[1].strip()
    season = lines[2].strip()

    print('Season: ' + season)
    print('Nite: ' + nite)
    print('Exposures: ' + str(exposures))
    
except:
    print('no .txt file, must input\n')
    exposures = [str(item) for item in input("Enter each exposure followed by band, separate with commas (ex. '938524 i, 938511 i, 938522 i'): ").split(',')]
    nite = input('Nite: ')
    dpSeason = input('Season: ')




# In[136]:


# Get exposure, format: /pnfs/des/persistent/gw/exp/NITE/EXPOSURE_NUMBER/dpSEASON/BAND_CCD
# Check CCDs from image processing, check forcephoto files
# Output list of exposures that can move on to post processing

dir_prefix = '/pnfs/des/persistent/gw/exp/'
dpSeason = ('dp' + str(season) + '/')    
    
exposures_to_cont = []
bandslist = []

for exposure in exposures:
    exposure = exposure.split()
    band = exposure[1]
    exposure = exposure[0]
    
    if band not in bandslist:
        bandslist.append(band)
    
    term_size = os.get_terminal_size()
    print('=' * term_size.columns)
    print("\nFOR EXPOSURE " + str(exposure) + ":\n")
    exposure_dir = dir_prefix + nite + '/' + exposure +'/' + dpSeason + band
    band_dirs = glob.glob(exposure_dir + '_*' + '/') #what we're counting to make sure they're all there
    print('There are '+ str(len(band_dirs)) + ' ' + str(band) + ' ccds\n')

    
    complete_ccds=0
    incomplete_ccds=0
    failed_ccds=0
    complete_ccds_list = []

    for dir in band_dirs:
        if glob.glob(dir+'*.FAIL'):
            print('ccd ' + dir[-5:-1] + ' failed')
            failed_ccds += 1
        elif glob.glob(dir + 'outputs_*'):
            complete_ccds += 1
            complete_ccds_list.append(dir[-5:-1])
        else:
            print('ccd ' + dir[-5:-1] + ' incomplete')
            incomplete_ccds += 1

    print('\nThere are ' + str(failed_ccds) + ' failed ' + str(band) + ' ccds')        
    print('There are ' + str(incomplete_ccds) + ' incomplete ' + str(band) + ' ccds')
    print('There are ' + str(complete_ccds) + ' complete '+ str(band) + ' ccds')

    if complete_ccds >= 50:
        print('\nover 50 ' + str(band) + ' ccds completed: acceptable')
        num_ccds = complete_ccds
    else:
        print('\nnot enough ccds in exposure ' + str(exposure) + ' for post processing\n')
        continue

# Get forcephoto exposure, format: /pnfs/des/persistent/gw/forcephoto/images/dpSEASON/NITE/EXPOSURE/

    expected_forcephoto_files = num_ccds * 2
    print('\nexpected forcephoto files for band ' + str(band) + ': ' + str(expected_forcephoto_files))

    forcephoto_dir_prefix = '/pnfs/des/persistent/gw/forcephoto/images/'

    forcephoto_dir = forcephoto_dir_prefix + dpSeason + nite + '/' + exposure
    forcephoto_files = glob.glob(forcephoto_dir + '/' + '*' + '_' + str(band) + '_' + '*')
    print('found forcephoto files for exposure '+  str(exposure) + ': ' + str(len(forcephoto_files)) + '\n')
    if len(forcephoto_files) == expected_forcephoto_files: 
        print('all forcephoto files completed in exposure ' + str(exposure) + ' -> transferring to post processing\n')
        exposures_to_cont.append(exposure)
    elif len(forcephoto_files) < expected_forcephoto_files and len(forcephoto_files) > expected_forcephoto_files / 2 :
        print('some forcephoto files not yet completed in exposure ' + str(exposure) + '\n')
        exposures_to_cont.append(exposure)
        for num in complete_ccds_list:
            if not glob.glob(forcephoto_dir + '*' + str(num) + '*.fits') or not (forcephoto_dir + '*' + str(num) + '*.psf'):
                print('forcephoto files for ' + num + ' not completed / missing')
            #elif glob.glob(forcephoto_dir + '*' + num + '*.fits') and glob.glob(forcephoto_dir + '*' + num + '*.psf'):
                #print('exposure ' + num + ' completed')
        print('\nover 50% forcephoto files in exposure ' + exposure + ' completed -> transferring to post processing\n')
    elif len(forcephoto_files) < expected_forcephoto_files and not len(forcephoto_files) > expected_forcephoto_files / 2 :
        print('fewer than 50% forcephoto files completed, will not add to exposures.list')
        continue
    elif len(forcephoto_files) > expected_forcephoto_files:
        print('check: More forcephoto files than expected for this exposure -> will not add to exposures.list')
        continue
    
print('exposures moving to post processing:\n' + str(exposures_to_cont))


# In[137]:


#create custom exposure.list file

print('creating .list file for completed exposures\n')

current_exposures = 'complete_exposures' + '_S' + str(season) + '_' + str(datetime.datetime.now().strftime("%Y%m%d_%H-%M")) + '.list'
with open(current_exposures, 'w') as f:
    for exposure in exposures_to_cont:
        f.write("%s\n" % exposure)


# In[143]:


#create .ini file

#move .ini file into Post-Processing

season = str(season)

#ask user for
ligoid = input('ligoid (ex. GW170814): ')
triggerid = input('triggerid (ex. G298048): ')
propid = input('propid (ex. 2017B-0110): ')
triggermjd = input('triggermjd (ex. 57979.437): ')

print('creating .ini file with completed exposures list\n')

exposures_listfile = str(current_exposures)

shutil.copyfile('template_postproc.ini', 'postproc_' + str(season) + '.ini')
postproc_season_file = 'postproc_'+ str(season) + '.ini'

edit = configparser.ConfigParser()
edit.read(postproc_season_file)

general = edit["general"]
general["season"] = season
general["ligoid"] = ligoid
general["triggerid"] = triggerid
general["propid"] = propid
general["triggermjd"] = triggermjd
general["exposures_listfile"] = exposures_listfile

bandslist = str(bandslist)
bandslist = bandslist.strip("[]'")
bandslist = bandslist.replace("'","")

general["bands"] = str(bandslist)

with open(postproc_season_file, 'w') as configfile:
    edit.write(configfile)


# In[144]:


#fetching directories from .ini file for SKIPTO

outdir = general["outdir"] 

truthtable = edit["truthtable"]
truthplusfile = truthtable['plusname']


# In[145]:


#Check if we want to SKIPTO

if glob.glob('./Post-Processing/'+ outdir[2:] + '/makedatafiles/LightCurvesReal/*.dat'):
    skip = input('It seems step 5 run_postproc has already been completed, would you like to skip to step 6? (y/n): ')
    if skip == ('y'):
        SKIPTO_flag = 6
        print('\nWill run post processing from step 6')
    else:
        print('\nWill run post processing from scratch')
elif os.path.exists('./Post-Processing/' + outdir[2:] + '/truthtable'+str(season)+'/'+truthplusfile): #output from step 4
    skip = input('\nIt seems step 4 run_postproc has already been completed, would you like to skip to step 5? (y/n): ')
    if skip == ('y'):
        SKIPTO_flag = 5
    else:
        print('Will run post processing from step 5')
else:
    print('No evidence of steps already completed in post processing, will not skip')

print('\nContinuing to post processing')


# In[146]:


#move .ini file and exposures list into Post-Processing

os.system('mv ' + str(postproc_season_file) + ' ./Post-Processing')
os.system('mv ' + str(current_exposures) + ' ./Post-Processing')

#setup for Post Processing
os.system('source ./Post-Processing/diffimg_setup.sh')
print('running diffimg_setup.sh\n')

update_forcephoto_links = input('Are you running post processing for new exposures? (aka: run ./update_forcephoto_links.sh?) y/n: ')
if update_forcephoto_links == ('y'):
    os.system('./Post-Processing/update_forcephoto_links.sh')
    
#run_postproc.py

try:
    SKIPTO_flag
except NameError:
    print("\nRunning run_postproc.py\n")
    os.system('nohup python ./Post-Processing/run_postproc.py --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out &')
else:
    print("\nRunning run_postproc.py\n")
    os.system('nohup python ./Post-Processing/run_postproc.py --SKIPTO ' + str(SKIPTO_flag) + ' --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out &')


# In[142]:


#make cuts


# In[ ]:




