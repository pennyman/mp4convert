#!/usr/bin/env python

import glob
import json
import os
import uuid
import botocore
import boto3
import datetime
import random
import urlparse
# import io
# from PIL import Image

from botocore.client import ClientError
from botocore.config import Config

def handler(event, context):
    assetID = str(uuid.uuid4())
    sourceS3Bucket = event['Records'][0]['s3']['bucket']['name']
    sourceS3Key = event['Records'][0]['s3']['object']['key']
    sourceS3 = 's3://'+ sourceS3Bucket + '/' + sourceS3Key
    sourceS3Basename = os.path.splitext(os.path.basename(sourceS3))[0]
    # destinationS3 = 's3://' + os.environ['DestinationBucket']
    destinationS3 = 's3://xxxx'
    destinationS3Thumb = 's3://xxxxx'
    destinationS3basename = os.path.splitext(os.path.basename(destinationS3))[0]
    mediaConvertRole = os.environ['MediaConvertRole']
    region = os.environ['AWS_DEFAULT_REGION']
    statusCode = 200
    rotate = 0
    width = 0
    ismuted = 'false'
    isorigin = 'false'
    height = 0
    body = {}
    transcoderKey = event['Records'][0]['s3']['object']['key']

    config = Config(connect_timeout=5, retries={'max_attempts': 10})

    # Use MediaConvert SDK UserMetadata to tag jobs with the assetID
    # Events from MediaConvert will have the assetID in UserMedata
    jobMetadata = {'assetID': assetID}
    # print("boto3 version:"+boto3.__version__)
    # print("botocore version:"+botocore.__version__)

    print (json.dumps(event))

    try:
        # Job settings are in the lambda zip file in the current working directory
        with open('job.json') as json_data:
            jobSettings = json.load(json_data)
            print(jobSettings)

        # get the account-specific mediaconvert endpoint for this region
        mc_client = boto3.client('mediaconvert', region_name=region, config= config)
        endpoints = mc_client.describe_endpoints()

        # add the account-specific endpoint to the client session
        client = boto3.client('mediaconvert', region_name=region, endpoint_url=endpoints['Endpoints'][0]['Url'], verify=False)

        search = 'null'
        # Connect to S3
        s3 = boto3.client('s3')
        try:
            print('Key:')

            response = s3.get_object(Bucket=sourceS3Bucket, Key=sourceS3Key)
            print(response['Metadata'])

            # Call some function here?
            if(len(response['Metadata']) > 0):
                if(len(response['Metadata']['rotate']) > 0 and search not in response['Metadata']['rotate']):
                    rotate = int(response['Metadata']['rotate'])

                if('width' in response['Metadata']):
                    width = int(response['Metadata']['width'])
                    print(response['Metadata']['width'])

                if('height' in response['Metadata']):
                    height = int(response['Metadata']['height'])
                    print(response['Metadata']['height'])

                if('ismuted' in response['Metadata']):
                    ismuted = response['Metadata']['ismuted']
                    print(response['Metadata']['ismuted'])
                if('isorigin' in response['Metadata']):
                    isorigin = response['Metadata']['isorigin']
                    print(response['Metadata']['isorigin'])
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

        if(rotate == 0):
            print('rotate is null')
        elif(rotate > 0  and 'false' in isorigin):
            try:
                transcoder = boto3.client('elastictranscoder', 'ap-northeast-1', config=config)
                # pipeline_id = read_pipeline(transcoder, 'MP4 Transcode')
                # input_file = os.path.basename(sourceS3Key)
                if ('true' in ismuted):
                    job = transcoder.create_job(
                        PipelineId= '1524038804778-d81ha3',
                        Input={
                                      'Key': sourceS3Key,
                                      'FrameRate': 'auto',
                                      'Resolution': 'auto',
                                      'AspectRatio': 'auto',
                                      'Interlaced': 'auto',
                                      'Container' : 'auto'
                        },
                        Outputs=[{
                                      'Key': sourceS3Key.replace("org.mp4", "")  + 'default.mp4',
                                      'ThumbnailPattern': sourceS3Key.replace("org.mp4", "")  + 'tran' +'-{count}',
                                      'PresetId': '1536550767311-y7fi7a',
                                      'Rotate': str(rotate)
                        }
                        ]
                    )
                    lowJob = transcoder.create_job(
                        PipelineId='1524038804778-d81ha3',
                        Input={
                            'Key': sourceS3Key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto'
                        },
                        Outputs=[{
                            'Key': sourceS3Key.replace("org.mp4", "") + '360p.mp4',
                            'ThumbnailPattern': sourceS3Key.replace("org.mp4", "") + '360' + '-{count}',
                            'PresetId': '1547621518410-xqdb9j',
                            'Rotate': str(rotate)
                        }
                        ]
                    )
                else:
                    job = transcoder.create_job(
                        PipelineId= '1524038804778-d81ha3',
                        Input={
                                      'Key': sourceS3Key,
                                      'FrameRate': 'auto',
                                      'Resolution': 'auto',
                                      'AspectRatio': 'auto',
                                      'Interlaced': 'auto',
                                      'Container' : 'auto'
                        },
                        Outputs=[{
                                      'Key': sourceS3Key.replace("org.mp4", "")  + 'default.mp4',
                                      'ThumbnailPattern': sourceS3Key.replace("org.mp4", "")  + 'tran' +'-{count}',
                                      'PresetId': '1535355015547-50lfw2',
                                      'Rotate': str(rotate)
                        }
                        ]
                    )
                    lowJob = transcoder.create_job(
                        PipelineId='1524038804778-d81ha3',
                        Input={
                            'Key': sourceS3Key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto'
                        },
                        Outputs=[{
                            'Key': sourceS3Key.replace("org.mp4", "") + '360p.mp4',
                            'ThumbnailPattern': sourceS3Key.replace("org.mp4", "") + '360' + '-{count}',
                            'PresetId': '1547620703360-s952sk',
                            'Rotate': str(rotate)
                        }
                        ]
                    )
                print('job:')
                print(job)
                job_id = job['Job']['Id']
                print(job_id)
            except Exception as e:
                # Create SQS client
                sqs = boto3.client('sqs')

                queue_url = 'xxxx'
                show_srl = sourceS3Key
                print('show_srl:')
                print(show_srl)
                # Send message to SQS queue
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    DelaySeconds=10,
                    MessageAttributes={
                    },
                    MessageBody=(
                        show_srl
                    )
                )
                print(response['MessageId'])

                print 'Exception: %s' % e
                statusCode = 500
                raise
        elif(rotate > 0  and 'true' in isorigin):
            try:
                transcoder = boto3.client('elastictranscoder', 'ap-northeast-1', config=config)
                # pipeline_id = read_pipeline(transcoder, 'MP4 Transcode')
                # input_file = os.path.basename(sourceS3Key)
                if ('true' in ismuted):
                    lowJob = transcoder.create_job(
                        PipelineId='1524038804778-d81ha3',
                        Input={
                            'Key': sourceS3Key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto'
                        },
                        Outputs=[{
                            'Key': sourceS3Key.replace("org.mp4", "") + '360p.mp4',
                            'ThumbnailPattern': sourceS3Key.replace("org.mp4", "") + '360' + '-{count}',
                            'PresetId': '1547621518410-xqdb9j',
                            'Rotate': str(rotate)
                        }
                        ]
                    )
                else:
                    lowJob = transcoder.create_job(
                        PipelineId='1524038804778-d81ha3',
                        Input={
                            'Key': sourceS3Key,
                            'FrameRate': 'auto',
                            'Resolution': 'auto',
                            'AspectRatio': 'auto',
                            'Interlaced': 'auto',
                            'Container': 'auto'
                        },
                        Outputs=[{
                            'Key': sourceS3Key.replace("org.mp4", "") + '360p.mp4',
                            'ThumbnailPattern': sourceS3Key.replace("org.mp4", "") + '360' + '-{count}',
                            'PresetId': '1547620703360-s952sk',
                            'Rotate': str(rotate)
                        }
                        ]
                    )
                print('job:')
                print(job)
                job_id = job['Job']['Id']
                print(job_id)
            except Exception as e:
                # Create SQS client
                sqs = boto3.client('sqs')

                queue_url = 'xx'
                show_srl = sourceS3Key
                print('show_srl:')
                print(show_srl)
                # Send message to SQS queue
                response = sqs.send_message(
                    QueueUrl=queue_url,
                    DelaySeconds=10,
                    MessageAttributes={
                    },
                    MessageBody=(
                        show_srl
                    )
                )
                print(response['MessageId'])

                print 'Exception: %s' % e
                statusCode = 500
                raise

        if(rotate == 0  and 'false' in isorigin):
            # Update the job settings with the source video from the S3 event and destination
            # paths for converted videos
            jobSettings['Inputs'][0]['FileInput'] = sourceS3
            print('width:')
            print(width)

            if width > 0 and width < 721 and height > 0 and height < 721:
                jobSettings['OutputGroups'][0]['Outputs'][0]['VideoDescription']['Width'] = width
                jobSettings['OutputGroups'][0]['Outputs'][0]['VideoDescription']['Height'] = height
                print('width1 :')
                print(width + 12)
            elif width > height:
                jobSettings['OutputGroups'][0]['Outputs'][0]['VideoDescription']['Width'] = 1024
                jobSettings['OutputGroups'][0]['Outputs'][0]['VideoDescription']['Height'] = 576
                jobSettings['OutputGroups'][1]['Outputs'][0]['VideoDescription']['Width'] = 640
                jobSettings['OutputGroups'][1]['Outputs'][0]['VideoDescription']['Height'] = 360

            S3KeyWatermark1 = sourceS3Key.replace("org.mp4", "")  + 'default'
            jobSettings['OutputGroups'][0]['OutputGroupSettings']['FileGroupSettings']['Destination'] \
                = destinationS3 + '/' + S3KeyWatermark1
            lowName = sourceS3Key.replace("org.mp4", "") + '360p'
            jobSettings['OutputGroups'][1]['OutputGroupSettings']['FileGroupSettings']['Destination'] \
                = destinationS3 + '/' + lowName

            # ismuted
            if ('true' in ismuted):
                jobSettings['OutputGroups'][0]['Outputs'][0]['AudioDescriptions'].pop()
                jobSettings['OutputGroups'][1]['Outputs'][0]['AudioDescriptions'].pop()

            print('jobSettings:')
            print(json.dumps(jobSettings))

            # Convert the video using AWS Elemental MediaConvert
            job = client.create_job(Role=mediaConvertRole, UserMetadata=jobMetadata, Settings=jobSettings)
            print (json.dumps(job, default=str))
        elif (rotate == 0  and 'true' in isorigin):
            jobSettings['Inputs'][0]['FileInput'] = sourceS3
            print('width:')
            print(width)

            if width > height:
                jobSettings['OutputGroups'][1]['Outputs'][0]['VideoDescription']['Width'] = 640
                jobSettings['OutputGroups'][1]['Outputs'][0]['VideoDescription']['Height'] = 360

            lowName = sourceS3Key.replace("org.mp4", "") + '360p'
            jobSettings['OutputGroups'][1]['OutputGroupSettings']['FileGroupSettings']['Destination'] \
                = destinationS3 + '/' + lowName

            # ismuted
            if ('true' in ismuted):
                jobSettings['OutputGroups'][1]['Outputs'][0]['AudioDescriptions'].pop()

            print('jobSettings:')
            print(json.dumps(jobSettings))

            # Convert the video using AWS Elemental MediaConvert
            job = client.create_job(Role=mediaConvertRole, UserMetadata=jobMetadata, Settings=jobSettings)
            print (json.dumps(job, default=str))

    except Exception as e:
        # Create SQS client
        sqs = boto3.client('sqs')

        queue_url = 'xxx'
        show_srl = sourceS3Key
        print('show_srl:')
        print(show_srl)
        # Send message to SQS queue
        response = sqs.send_message(
            QueueUrl=queue_url,
            DelaySeconds=10,
            MessageAttributes={
            },
            MessageBody=(
                show_srl
            )
        )
        print(response['MessageId'])

        print 'Exception: %s' % e
        statusCode = 500
        raise

    finally:
        print('finally:')

        return {
            'statusCode': statusCode,
            'body': json.dumps(body),
            'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
        }
