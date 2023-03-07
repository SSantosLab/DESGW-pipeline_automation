#!/bin/bash

#export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
#source $CONDA_DIR/etc/profile.d/conda.sh

#conda activate des18a

#. /data/des80.a/data/desgw/BBH_Global/200224/gw_workflow_0310/debugging/startupcachejob31i_nfs_2.sh
#export JOBSUB_PYVER=python2.7-ucs4
#JOBSUB_PYVER=python2.7-ucs4

#echo $CIGETCERTLIBS_DIR

conda activate des18a

python full_image_proc_python2.py --test_season=y --exp_list_location=../DESGW-pipeline_automation/exposure_228.list --test_season_number=2208 --WRITEDB=on --RNUM=4 --PNUM=7 --DO_HEADER_CHECK=1  

#conda deactivate

#python ppp1

#cd ../Post-Processing/

#source diffimg_setup.sh 

#echo $AUTOSCAN_PYTHON

#cd -

#python ppp2
