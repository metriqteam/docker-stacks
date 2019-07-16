# -*- coding: utf-8 -*-

from os import listdir, urandom
from os.path import isfile, join, splitext
from base64 import urlsafe_b64encode
from hashlib import md5
from uuid import uuid4
import logging
from json import dumps
from requests import post

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
                         BucketAlreadyExists)


AIRFLOW_URL = 'http://webserver:8081/api/experimental/dags/execute_code/dag_runs'

USE_MINIO = True
MINIO_ADDRESS = 'minio:9000'
MINIO_ACCESS_KEY = 'access_access' # TODO: varenv
MINIO_SECRET_KEY = 'secret_secret' # TODO: varenv

"""
Minio Python lib:

SSL certificate:
https://docs.min.io/docs/how-to-secure-access-to-minio-server-with-tls.html

minioClient = Minio('minio:9000', 
                    access_key='access_access', 
                    secret_key='secret_secret', 
                    secure=False)
minioClient.make_bucket("mybucket")

"""


class ExecuThor:
    python_files = []
    requirements_file = None

    def __init__(self, folder_path):
        self.folder_path = folder_path

        files = [f for f in listdir(self.folder_path) 
                if isfile(join(self.folder_path, f))]
        for f in files:
            split = splitext(f)
            filename = split[0]
            extension = split[1]

            if extension in ['.py', '.ipynb']:
                self.python_files.append(join(self.folder_path, f))
            if 'requirements' in filename:
                self.requirements_file = join(self.folder_path, f)

        print(
            "Initialized ExecuThor with:\n"
            "Files: {0}\n"
            "Requirements file: {1}".format(
            self.python_files, self.requirements_file))
    
        if USE_MINIO == True:
            self.__init_minio_client()
        

    def __init_minio_client(self, secure:bool=False):
        print("Initializing Minio client...")
        self.minioClient = Minio(MINIO_ADDRESS, 
                                 access_key=MINIO_ACCESS_KEY, 
                                 secret_key=MINIO_SECRET_KEY,
                                 secure=secure)
        
        
    def __create_project_associated_bucket(self):
        self.project_bucket_name = str(uuid4())
        print("Initializing bucket with random name: {}".format(
            self.project_bucket_name))
        self.minioClient.make_bucket(bucket_name=self.project_bucket_name)
        
        return self.project_bucket_name
        
    def __get_bucket_name(self):
        try:
            return self.project_bucket_name
        except:
            raise Exception("Error. Project bucket name is not define.")
        
    def __copy_file_to_minio_bucket(self):
        # Create empty output folder
        

        files = [f for f in listdir(self.folder_path) 
                if isfile(join(self.folder_path, f))]
        for f in files:
            split = splitext(f)
            filename = split[0]
            extension = split[1]
            file_path = join(self.folder_path, f)
            
            try:
                self.minioClient.fput_object(
                    bucket_name=self.__get_bucket_name(), 
                    object_name=f,
                    file_path=file_path, 
                    # content_type='application/csv' # optional
                )
            except ResponseError as err:
                print(err)
        
        
    def launch(self):
        print("Copying files...")
        self.__create_project_associated_bucket()
        self.__copy_file_to_minio_bucket()
        print("Launching tasks...")

        bucket_name = self.__get_bucket_name()
        airflow_data = {
            'conf': {
                'bucket_name': bucket_name
                # checksum file?
            }
        }
        r = post(AIRFLOW_URL, data=dumps(airflow_data))
        if not r.ok:
            print(r.status_code, r.reason)
            print(r.text[:300] + '...')
        else:
            print("Airflow task launched for bucket {0}".format(bucket_name))


if __name__ == "__main__":
    executhor_instance = ExecuThor(
        folder_path='/home/jovyan/work'
    )
    executhor_instance.launch()
