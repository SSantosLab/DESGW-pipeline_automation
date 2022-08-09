# DESGW-image-processing
---
## Introduction to this automation:
Our codes work with user input to, theoretically, automate the DESGW pipeline (the image processing and post processing steps). The goal of this automation is to reduce user input and streamline observation response. Some user input is required for the automation, which will be specified along with each step.
## Image Processing Code Explanation:
### IMPORTANT NOTES
As of now, the image processing code should be run in gw_workflow due to ongoing path issues. You should follow these steps to make sure it runs properly:

1. Copy full_image_proc_gwworkflow.py to your gw_workflow folder
2. Run your proxy
2. Run

    ```
    conda activate des18a
    ```
3. Run

    ```
    python full_image_proc_gwworkflow.py 
    ```

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

These will be placed in a file located in image_proc_outputs called output.txt. The first line of this .txt is the exp_list, the second line is NITE, and the third line is SEASON. 

## Post Processing Code Explanation:
Once Image Processing has completed running, the post processing automation retreives the exposures and their bands in the format above, along with the nite and season from the output file. 

If there is no output file in the expected format, the code will prompt the user to provide this information. 

### Checking CCDs:
The automation then looks through the files in the directories with format '/pnfs/des/persistent/gw/exp/NITE/EXPOSURE_NUMBER/dpSEASON/BAND_CCD' to print which of the 62 total CCDs were successfully completed, failed, or incomplete. If the code says there are fewer than 62 CCDs, some of them were likely broken. 

### Checking Forcefiles
If enough CCDs successfully completed for an exposure, the code looks for the .psf and .fits "forcefiles" in the directories with format '/pnfs/des/persistent/gw/forcephoto/images/dpSEASON/NITE/EXPOSURE/'. If enough of them have been completed, then those exposures will move on to post processing. The code prints if the exposures will move to post-processing, and if not, why, which may be because there are not enough complete forcephoto files or that there are more than expected, so this must be investigated by the user. It also prints which forcephoto files have not yet been completed if there are enough to move to post-processing but some are still incomplete. This is to give the user a sense for the "completeness" of an exposure's forcefiles. 

### File Creation
The code then goes on to create a custom exposure.list file that contains all of the exposures that are ready to be post processed. This, along with user inputted information and the bands, is put into a postproc_"season#".ini file based on the template_postproc.ini file whcih must also be in this directory.

### SKIPTO
The code moves on to check if Post Processing has been run on these exposures before by checking for certain files in the out directory indicated by the .ini file. Those files would have only been created during certain steps of post processing, so it is safe to skip those steps in future runs of the pipeline. The code then includes the SKIPTO value as an flag when it runs run_postproc.py. If the code determines that the exposures must run from scratch, run_postproc.py runs without a SKIPTO flag and starts at the beginning.

### Setup before run_postproc.py
The code moves .ini file and .list file into the Post-Processing directory. It runs:

    diffimg_setup.sh
    
and then if requested, runs

    update_forcephoto_links.sh

It then runs run_postproc.py in the form of

    nohup python ./Post-Processing/run_postproc.py --outputdir outdir --season '+ str(season)+ ' &> postproc_run.out &'

## How to Use the Repo:

