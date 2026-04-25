import subprocess as sp 
import shlex 
import boto3 
import time 


def main(event, context):
   start_time=time.time()
   try:
      sp.check_output(shlex.split("./merge2.exe --niter 200"), timeout=850) #
   except:
      pass
   BUCKET_NAME = 'utahlambdabucket'
   BUCKET_FILE_NAME = 'read_file_SRAsearch_merge2.txt' #
   LOCAL_FILE_NAME = '/tmp/read_file_SRAsearch_merge2.txt' #
   s3 = boto3.client('s3')
   exe_time=time.time()-start_time
   
   start_time=time.time()
   s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)
   read_time=time.time()-start_time
   start_time=time.time()
   WRITE_FILE_NAME= "write_file_SRAsearch_merge2"+str(event['key1'])+".txt" #
   s3.upload_file(LOCAL_FILE_NAME, 'utahlambdabucket', WRITE_FILE_NAME)
   write_time=time.time()-start_time
   return exe_time, read_time, write_time
