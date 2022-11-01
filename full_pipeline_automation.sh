#conda activate des18a
#source ../gw_workflow/setup_img_proc.sh

export CONDA_DIR=/cvmfs/des.opensciencegrid.org/fnal/anaconda2
source $CONDA_DIR/etc/profile.d/conda.sh

#. /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh

. /data/des80.a/data/desgw/BBH_Global/200224/gw_workflow_0310/debugging/startupcachejob31i_nfs_2.sh
export JOBSUB_PYVER=python2.7-ucs4
JOBSUB_PYVER=python2.7-ucs4

#conda deactivate
conda activate des20a

python2 full_image_proc_python2.py

python3 postprocessing_automations.py
