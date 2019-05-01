from __future__ import print_function
import json
import urllib.parse
import boto3
import os
import io
import csv
import time
print('Loading function')

# Create an S3 client
s3 = boto3.client('s3')

# Set end point name from environmental variable
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']

# Set sagemaker runtime endpoint
runtime= boto3.client('runtime.sagemaker')

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    
    # Get the object from the event and show its content type
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding = ("utf-8-sig"))
    
    destination_bucket = 'testing-lambda99'

    print("NEW FILE " + key +" UPLOADED INTO " + source_bucket +" S3 BUCKET")

    try:
        # Get the file object
        file = s3.get_object(Bucket=source_bucket, Key=key)
        file_content_type = file['ContentType']
        #print("CONTENT TYPE:  " + fileContent)
        
        fileContent = file['Body'].read().decode('utf-8-sig').rstrip()
        #print("NEW DATA OBTAINED" + fileContent)

        # Check for missing values in real time data
        
        # Split into list to check for missing data
        fileContent_list = list(map(lambda x: x.split(','), fileContent.splitlines()))
        
        # Delete row that has any missing value
        fileContent_clean =[]
        [fileContent_clean.append('\n'+','.join(item)) for i, item in enumerate(fileContent_list)
                        if all(x for x in fileContent_list[i]) == True]
        fileContent_clean = ''.join(fileContent_clean)
        #print("Data after removing rows that have missng data", fileContent_clean)


        # Invoke Sagemaker endpoint
        result_response = runtime.invoke_endpoint(EndpointName=ENDPOINT_NAME,
                                           ContentType=file_content_type,       #ContentType='text/csv'
                                           Body=fileContent_clean)
        #print(result_response)
        
        
        # Get Predictions
        result = json.loads(result_response['Body'].read().decode())
        #print("RESULT :",result)

        
        preds = [(num['score']) for num in result['predictions']]
        return("PREDICTED VALUES: ", preds)


        ## Save result to csv file in destination bucket
        string_result = ("\n".join(map(str, preds)))
        #csv_buffer = io.StringIO(str(preds))
        csv_buffer = io.StringIO(string_result)
        
        # Upload to destination S3 bucket
        s3.put_object(Bucket=destination_bucket,
                        Key='lambda-output/result-'+time.strftime("%Y-%m-%d-%H-%M-%S", time.gmtime())+'.csv',
                        Body=csv_buffer.read())
        print("PREDICTION RESULT SAVED TO " + destination_bucket )
        
        
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
