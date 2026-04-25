import subprocess as sp 
import shlex
import boto3 
import time 

def main(event, context):
   start_time=time.time()
   sp.check_output(shlex.split("./filtercontams.exe -s 24")) #
   BUCKET_NAME = 'utahlambdabucket'
   BUCKET_FILE_NAME = 'read_file_epigenomics_filtercontams.txt' #
   LOCAL_FILE_NAME = '/tmp/read_file_epigenomics_filtercontams.txt' #
   s3 = boto3.client('s3')
   exe_time=time.time()-start_time
   
   start_time=time.time()
   s3.download_file(BUCKET_NAME, BUCKET_FILE_NAME, LOCAL_FILE_NAME)
   read_time=time.time()-start_time
   start_time=time.time()
   WRITE_FILE_NAME= "write_file_epigenomics_filtercontams"+str(event['key1'])+".txt" #
   s3.upload_file(LOCAL_FILE_NAME, 'utahlambdabucket', WRITE_FILE_NAME)
   write_time=time.time()-start_time
   return exe_time, read_time, write_time
