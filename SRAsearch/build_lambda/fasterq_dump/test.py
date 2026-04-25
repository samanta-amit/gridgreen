import subprocess as sp 
import shlex
import boto3 
import time 

def main(event, context):
   start_time=time.time()
   sp.check_output(shlex.split("./fasterq_dump.exe -s 30")) #
   BUCKET_NAME = 'utahlambdabucket'
   BUCKET_FILE_NAME = 'read_file_SRAsearch_fasterq_dump.txt' #
   LOCAL_FILE_NAME = '/tmp/read_file_SRAsearch_fasterq_dump.txt' #
   s3 = boto3.client('s3')
   exe_time=time.time()-start_time
   
   start_time=time.time()
   s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)
   read_time=time.time()-start_time
   start_time=time.time()
   WRITE_FILE_NAME= "write_file_SRAsearch_fasterq_dump"+str(event['key1'])+".txt" #
   s3.upload_file(LOCAL_FILE_NAME, 'utahlambdabucket', WRITE_FILE_NAME)
   write_time=time.time()-start_time
   return exe_time, read_time, write_time
