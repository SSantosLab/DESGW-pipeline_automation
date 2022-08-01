# DESGW-image-processing
---
## Introduction to this automation:
Our codes work with user input to, theoretically, automate the DESGW pipeline (the image processing and post processing steps). The goal of this automation is to reduce user input. However, there is still some user input required while running, which will be specified along with each step.
## Image Processing Code Explanation:
### Updating dagmaker.rc
Dagmaker.rc is a config file that sets the parameters necessary for later creating a dag with dagmaker.sh. This section explains how the image processing code searches for these input parameters from the user in order to update the config file. 

To start off with, the image processing code asks for user input on a desired season number. Season is essentially the identification for a dataset, and should therefore be a unique number. Once the user inputs a desired season (or asks the code to generate one), the code will query the DES database in order to make sure it hasn't been used before. 

Next, the code asks if you'd like to update any other parameters. For a list of parameters, look within the overall dagmaker.rc explanation within gw_workflow. These parameters should be updated only if the user knows what they need to update them to.

### Parallel-Processing Dagmaker and Submitting Dags
Before running the processes outlined in this section, it's necessary to have a DESGW proxy. It's also necessary to run:

    source setup_img_proc.sh
  
The automation will run this for you. However, if, in doing so, it raises an exception, it is important to manually run it. Without sourcing setup_img_proc, dag creation and submission will fail.

Next, the user needs to input a .list file containing a list of the exposures they need to run image processing on. The code will then automatically run dagmaker.sh, producing a .out file named with the exposure in mind, and then submit the created dag. This is done through parallel processing (the multiproc package). The code will run up to 5 processes at once, in order to not overwhelm the DES machine, but if desired, one can go into the code and edit the "number_of_processes" variable to change how many will parallel process. 

The code works by creating a queue object of all of the exposures within the .list file, then taking five initial exposures to run dagmaker.sh on, piping the output to a dagmaker_*exp_number*.out file. As soon as the .out file has been successfully created, the dag is submitted. Once an exposure finishes, the code takes the next exposure from the queue and runs it on the process that is no longer running that exposure, and removes the exposure that finished from the queue. The code will continue to run until the queue object is empty, that is, all the exposures have gone through. 

### Output
After the previous steps, the code looks in the exposures.list file contained within gw_workflow for information necessary for post-processing. It outputs a list, containing each exposure with its respective band in the format [expnum band, expnum band, expnum band]. It also passes on the NITE and SEASON to post-processing. 

## Post Processing Code Explanation:
## How to Use the Repo:

