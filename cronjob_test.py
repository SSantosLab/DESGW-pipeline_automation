import os
import sys
import subprocess

#run ManualCheckDB.py to see if it found any new exposures. this should save any outputted new exposures from manualcheckdb as a string

# check_exps = subprocess.check_output(['python', 'ManualCheckDB.py', '--season', '--exp_list', '--outfile', '--propid'], stdin=None, stderr=None, shell=False, universal_newlines=False)
check_exps = subprocess.check_output(['python', 'test_manualcheck.py'], stdin=None, stderr=None, shell=False, universal_newlines=False)
print(check_exps)
i=0
machine_list = [60, 61, 70, 71, 80, 81, 90, 91]
USER = os.getenv('USER')

if check_exps != 'There are no new exposures': 
    for machine in machine_list:
        #os.sys version, which seems less robust but works better for now
        command = f'ssh {USER}@des{str(machine)}.fnal.gov pgrep top'
        output = os.system(command)
        if output == 0:
            print(f'Function is running on des{machine}')
        else:
            print(f'Function is not running on des{machine}. Running image processing theoretically...')
            print('Ending cron job')
            sys.exit()
                         
    #subprocess.Popen option. to be put before the if/else if want to use; still figuring out and long so moved it to the end here. the -tt is required to avoid the pseudoterminal not allowed error, but it also is what makes the terminal login and show each server it goes through
#     sshProcess = subprocess.Popen(['ssh',
#                                '-tt',
#                                f'{USER}@des{str(machine)}.fnal.gov'],
#                                stdin=subprocess.PIPE, 
#                                stdout = subprocess.PIPE,
#                                universal_newlines=True,
#                                bufsize=0)
#     sshProcess.stdin.write("pgrep top\n")
#     sshProcess.stdin.write("echo END\n")
#     sshProcess.stdin.write("uptime\n")
#     sshProcess.stdin.write("logout\n")
#     sshProcess.stdin.close()

#     #I don't exactly understand this END, but I know it's in there to prevent some vulnurabilities in the system when using an SSH command (https://stackoverflow.com/questions/19900754/python-subprocess-run-multiple-shell-commands-over-ssh)
#     for line in sshProcess.stdout:
#         if line == "END\n":
#             break
#         print(line,end="")

#     #to catch the lines up to logout
#     for line in  sshProcess.stdout: 
#         print(line,end="")