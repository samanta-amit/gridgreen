import threading
import subprocess as sp
import shlex
import os
import signal
import psutil
import time

pro_list=[]
child_list=[]

def kill_processes(parent_pid, sig=signal.SIGTERM):
    try:
      parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
      return
    children = parent.children(recursive=True)
    for process in children:
      process.send_signal(sig)

    try:
       os.kill(parent_pid, signal.SIGKILL)
    except:
       pass

def run_app(x,y):
   sp.check_output(shlex.split("python3 run_pileup.py 1"))
   for c in child_list:
      try:
         cmd="sudo kill "+str(c)
         sp.check_output(shlex.split(cmd))
      except:
         pass
   try:
        os.kill(pro_list[0].pid, signal.SIGKILL)
   except:
        pass
   return 0

def run_profiler(x,y):
   p=sp.Popen(shlex.split("bash profiler.sh"), shell=False)
   pro_list.append(p)
   time.sleep(2)
   children = psutil.Process(pro_list[0].pid).children(recursive=True)
   for c in children:
       child_list.append(c.pid)
   return 0

threads=[]
t1=threading.Thread(target=run_app, args=(1,1))
t1.start()
threads.append(t1)

t1=threading.Thread(target=run_profiler, args=(1,1))
t1.start()
threads.append(t1)

for t in threads:
        t.join()

