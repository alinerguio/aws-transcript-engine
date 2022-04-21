import os
import sys
import time
import json
import boto3
import logging
import requests
import datetime                      
import pandas as pd
from botocore.exceptions import ClientError


def log_time_specifics(start_time, end_time, dataset, quantity_files):
    process_time = end_time - start_time

    if os.path.isfile('execution_time_specifics.txt'):
        f = open('execution_time_specifics.txt', 'a')
    else:
        f = open('execution_time_specifics.txt', 'w')
        
    f.write('\n' + dataset + ';' +  str(quantity_files) + ';' + str(process_time))
    f.close()


def iterate_folder(bucket_name, ACCESS_KEY, SECRET_KEY, main_dir, region=None):
    all_folders = [folder for folder in os.listdir(main_dir) if '.' not in folder]
    
    for folder in all_folders:
        start_time = datetime.datetime.now()
        quantity_files = 0 

        curr_dir = main_dir + folder
        transc_folder = []

        all_files = [file for file in os.listdir(curr_dir) if '.wav' in file]

        if not(os.path.isdir('./transcriptions/')):
            os.mkdir('./transcriptions/')

        output_path = './transcriptions/' + folder + '.csv'

        if os.path.isfile(output_path):
            files_transcripted = pd.read_csv(output_path)
            files_transcripted_list = files_transcripted.file.tolist()
            all_files = [file for file in all_files if file not in files_transcripted_list]

        for file in all_files:
            try:
                instance = transcribe(bucket_name, ACCESS_KEY, SECRET_KEY, file, curr_dir, region) 
                final_result = pd.DataFrame(instance)
                final_result.to_csv(output_path, mode='a', header=not os.path.exists(output_path))
                quantity_files += 1

            except Exception as e:
                print('Not possible to proceed to transcript file: ' + file)
                print(e)

            except KeyboardInterrupt:
                print('\nKeyboardInterrupt: stopping manually')
                end_time = datetime.datetime.now()
                log_time_specifics(start_time, end_time, folder, quantity_files)

                sys.exit()

        end_time = datetime.datetime.now()
        log_time_specifics(start_time, end_time, folder, quantity_files)


def create_bucket(bucket_name, ACCESS_KEY, SECRET_KEY, region=None):
    """Create an S3 bucket in a specified region

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """

    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client(
                's3',
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client(
                's3',
                region_name=region,
                aws_access_key_id=ACCESS_KEY,
                aws_secret_access_key=SECRET_KEY
            )
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, folder, region=None):
    file_path = folder + '/' + file

    if region is None:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
    else:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=region
        )

    s3 = session.resource('s3')
    obj = s3.Object(bucket_name, file)
    result = obj.put(Body=open(file_path, 'rb'))


def transcribe_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, region=None):
    if region is None:
        transcribe = boto3.client(
            'transcribe',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
    else:
        transcribe = boto3.client(
            'transcribe',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=region
        )

    

    job_name = str(datetime.datetime.now()).replace(' ', '-').replace(':', '.') + "-" + file
    job_uri = "s3://" + bucket_name + "/" + file

    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={
            'MediaFileUri': job_uri
            },
        MediaFormat='wav',
        LanguageCode='pt-BR'
    )

    while True:
        status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            break
        time.sleep(5)

    return status['TranscriptionJob']['Transcript']['TranscriptFileUri']


def delete_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, region=None):
    if region is None:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
    else:
        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=region
        )

    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')
    obj = s3.Object(bucket_name, file)
    obj.delete()


def request_transcript(url, file, folder):
    folder = folder.replace('../data/', '')
    resp = requests.get(url)
    result = json.loads(resp.text)

    return [{'transcriptions': result['results']['transcripts'], 'items': result['results']['items'], 'status': result['status'], 'file': file, 'database': folder}]


def transcribe(bucket_name, ACCESS_KEY, SECRET_KEY, file, folder, region=None):
    upload_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, folder, region)
    url = transcribe_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, region)
    delete_file(bucket_name, ACCESS_KEY, SECRET_KEY, file, region)

    return request_transcript(url, file, folder)


def log_time(start_time, end_time):
    process_time = end_time - start_time

    if os.path.isfile('execution_time.txt'):
        f = open('execution_time.txt', 'a')
        f.write('\n' + str(process_time))
        f.close()
    else:
        f = open('execution_time.txt', 'w')
        f.write(str(process_time))
        f.close()


if __name__ == '__main__':
    # NEED TO CHANGE THIS INFORMATION
    region = None
    ACCESS_KEY = None
    SECRET_KEY = None
    bucket_name = 'test-transcribe-CHANGE'
    main_dir = '../data/'
    #################################
    
    # create_bucket(bucket_name, ACCESS_KEY, SECRET_KEY, region)
    start_time = datetime.datetime.now()

    try:
        iterate_folder(bucket_name, ACCESS_KEY, SECRET_KEY, main_dir, region)
        end_time = datetime.datetime.now()
        log_time(start_time, end_time)
    except Exception as e:
        end_time = datetime.datetime.now()
        log_time(start_time, end_time)
        print(e)
    
