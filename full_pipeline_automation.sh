source stup_img_proc.sh
. /cvmfs/des.opensciencegrid.org/eeups/startupcachejob31i.sh
conda activate des20a

python2 full_image_proc_python2.py

python3 postprocessing_automations.py
