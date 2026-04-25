import threading,time,shlex
from subprocess import DEVNULL, STDOUT, check_call
import subprocess as sp
import shlex
import os, glob
import sys
import shutil
max_elements=10000
exe_list = [0 for i in range(max_elements)]
read_list= [0 for i in range(max_elements)]
write_list = [0 for i in range(max_elements)]
def run(command,count):
    start_time = time.time()
    cmd = shlex.split(command)
    sp.call(cmd, stdout=DEVNULL)
    exe_list[count] = time.time() - start_time
    
    start_time=time.time()
    f= open("/tmp/read_file_1000genome_individual_merge.txt", 'r+')
    lines=f.read()
    read_list[count] = time.time() - start_time
   
    start_time=time.time()
    e=open("/tmp/write_file_1000genome_individual_merge"+str(count)+".txt", 'w+')
    e.write(lines)
    write_list[count] = time.time() - start_time

    e.close()
    f.close()

num_run=int(sys.argv[1])
commands = ["./individual_merge.exe --niter 1000" for i in range(num_run)]
threads=[]
count=0
for command in commands:
    t = threading.Thread(target=run,args=(command,count))
    count+=1
    t.start()
    threads.append(t)
for t in threads:
    t.join()

for filename in glob.glob("/tmp/write_file_1000genome_individual_merge*"):
    os.remove(filename) 

exe_list = [i for i in exe_list if i != 0]
read_list = [i for i in read_list if i != 0]
write_list = [i for i in write_list if i != 0]
total_list=[exe_list, read_list, write_list]


with open('individual_merge_output.txt', 'w') as filehandle:
    for listitem in total_list:
        filehandle.write('%s\n' % listitem)
