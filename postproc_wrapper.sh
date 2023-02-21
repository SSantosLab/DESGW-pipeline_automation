#!/bin/bash

conda activate des18a

python2 postproc_auto_1.py

cd ../Post-Processing

source diffimg_setup.sh

cd -

python2 postproc_auto_2.py
