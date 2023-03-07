#!/bin/bash

conda activate des18a

echo "before postproc_auto_1"

python2 postproc_auto_1.py

echo "after postproc_auto_1"

cd ../Post-Processing

echo $PWD

source diffimg_setup.sh

echo $PWD

cd -

echo "before postproc_auto_2"

python2 postproc_auto_2.py
