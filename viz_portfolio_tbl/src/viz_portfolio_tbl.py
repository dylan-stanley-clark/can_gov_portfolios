import pandas as pd
import boto3
import argparse
import os
import io
from datetime import timedelta, date,datetime
import streamlit as st


def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser



if __name__ == "__main__":
    #load command line args
    parser  = create_argument_parser()
    args = parser.parse_args()

    #proccessing to produce portfolio tbl (rows of each term a federal government portfolio was held by a cabinet member)
    if args.run_type == 'vizualize':
        #connect to amazon web services S3 bucket
        s3 = boto3.resource(service_name='s3',region_name='ca-central-1',aws_access_key_id=str(os.getenv("aws_access")[1:-1]),aws_secret_access_key=str(os.getenv("aws_key")[1:-1]))

        file_obj = s3.Bucket('polemics').Object("processed/portfolio_tbl.csv").get()
        ministry_info = pd.read_csv(io.BytesIO(file_obj['Body'].read()))

        file_obj = s3.Bucket('polemics').Object('processed/cabinet_tbl.csv').get()
        cabinet_tbl = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        cabinet_tbl.drop(columns=['Unnamed: 0'],inplace=True)
