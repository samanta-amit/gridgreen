import subprocess as sp
import shlex
import os.path
import os
import time
import signal

def func():
  pro_list=[]
  #p=sp.Popen(shlex.split("""aws --cli-binary-format raw-in-base64-out lambda invoke --function-name run_individual_merge --payload '{"key1":"0"}' response.txt --cli-read-timeout 0"""))
  p=sp.Popen(shlex.split("""aws lambda invoke --function-name run_individual_merge --payload '{"key1":"0"}' response.txt --cli-read-timeout 0"""))
  pro_list.append(p)
  print("started")
  filelist=["response.txt"]
  while True:
      list1 = []
      for file in filelist:
          list1.append(os.path.isfile(file))
      if all(list1):
          print("present")
          return 0
          break
      else:
          print("not here")
          time.sleep(2)

i=func()

