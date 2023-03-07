#danas_automations_:)
#!/usr/bin/env python
# coding: utf-8

# In[1]:

import re
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
from collections import Counter


try:
    img_proc_file = open("./image_proc_outputs/output.txt")
    lines = img_proc_file.readlines()
    
    season = lines[1].strip()
except:
    
    season = raw_input('Season: ')

#move into post-proc directory

os.chdir("../Post-Processing")

os.system('. update_forcephoto_links.sh')
    
#run_postproc.py

try:
    SKIPTO_flag
except NameError:
    print("\nRunning run_postproc.py\n")
    os.system('nohup python run_postproc.py --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out &')
#else:
#    print("\nRunning run_postproc.py with skip\n")
#    os.system('nohup python run_postproc.py --SKIPTO ' + str(SKIPTO_flag) + ' --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out &')


# In[ ]:


#make cuts


# In[ ]:




