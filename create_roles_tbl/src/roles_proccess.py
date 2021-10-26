import pandas as pd
import numpy as np
import openpyxl
import os
import io
import boto3
import argparse
AWS_ACCESS_KEY_ID = os.getenv("aws_access")[1:-1]
AWS_SECRET_ACCESS_KEY = os.getenv("aws_key")[1:-1]
#setup connection to s3
s3 = boto3.resource(
    service_name='s3',
    region_name='ca-central-1',
    aws_access_key_id=str(AWS_ACCESS_KEY_ID),#AWS_ACCESS_KEY_ID,
    aws_secret_access_key=str(AWS_SECRET_ACCESS_KEY)
    )

def load_aws(file,bucket='polemics',sheet="-"):
    """
    Function to read an excel or csv file from a s3 storage Bucket into a pandas dataframe
    file: the file name (aws key) to load
    """

    # Load s3 file into pandas data frame
    file_obj = s3.Bucket('polemics').Object(file).get()

    if sheet == "-":
        df = pd.read_excel(io.BytesIO(file_obj['Body'].read()))
    if sheet !="-":
        df = pd.read_excel(io.BytesIO(file_obj['Body'].read()),sheet)
    # else:
    #     df = pd.read_csv(io.BytesIO(file_obj['Body'].read()))

    return df

def write_csv_aws(df,file_name,bucket='polemics'):
    """
    Function to write a pandas dataframe to a csv file in a s3 storage Bucket
    df: pandas dataframe to write to s3
    file: the file name (aws key) to save
    """
    #write data to specified s3 bucket
    s3.Object(bucket, file_name).put(Body=df.to_csv())
    print("successfully wrote data to s3 csv")

def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser

def assocaite_parls(df, file="elections.xlsx"):
    """
    Associate each role in the LOP roles table to a parliamentary session

    df: pandas dataframe of the ParlinfoFederalAreaOfResponsibilitiy.xlsx from the L.O.P
    Returns data frame with an added coloumn called "Parliament",
    which is the Parliament session # assocaited with each role/row
    """

    # get rows assocaited with each parliament
    #ie. second parliament roles start at row 12387 and end at 12487
    locations = {}
    for i,parl in enumerate(election_dates["Ids"].to_list()):

        try:
            #get the row number associated with the start a parliaments roles
            sessions = list(getIndexes(df, parl))
            row = sessions[0][0]
            # add dictionary item of row start for each session
            locations[election_dates.loc[i,"Parliament"]] = row
        except:
            print("manually set start to row 1 for:",parl)
            locations[election_dates.loc[i,"Parliament"]] = 1

    sessions = [1]*len(df)
    sessions[:342] = [43]*len(sessions[:342]) # 43 here is a dummy parliament why :342?
    # replace dummy list with parliament that the role is associated
    for parl in list(locations.keys()):
        try:
            start,end = locations[parl],locations[parl-1]
            sessions[start:end] = [parl]*len(sessions[start:end])
        except:
            start = locations[parl]
            sessions[start:] = [parl]*len(sessions[start:])

    return sessions

def get_active_role(df):
    return np.where(df.isnull(), "active", "inactive")

def get_session_start(dates,min_date):
    # dates = the role start date
    start_dates = [datetime.strptime(date,"%Y/%m/%d") for date in dates.to_list()]
    return [  min_date < start_dates[i] for i in range(len(start_dates) )]

def getIndexes(dfObj, value):
    listOfPos = []
    result = dfObj.isin([value])
    seriesObj = result.any()
    columnNames = list(seriesObj[seriesObj == True].index)
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
        for row in rows:
            listOfPos.append((row, col))

    return listOfPos

if __name__ == "__main__":
    parser  = create_argument_parser()
    #load command line args
    args = parser.parse_args()
    #run a test if specified
    if args.run_type == 'write_data_test':
        write_csv_aws(pd.DataFrame({'A' : []}),'test.csv')
    if args.run_type == 'load_data_test':
        raw_roles = load_aws("raw/ParlinfoFederalAreaOfResponsibilitiy.xlsx")
    #run the preproccessing code
    if args.run_type == 'proccess':
        #load role table from
        raw_roles = load_aws("raw/ParlinfoFederalAreaOfResponsibilitiy.xlsx")
        #get reference dates for adding session links to roles ie. what parliament it occured
        election_dates = load_aws("references/elections.xlsx",sheet='elections')
    
        # get rows assocaited with each parliament
        #proccess raw table from LOP
        raw_roles['parliament'] = assocaite_parls(raw_roles) # add coloumn 'parliament'
        raw_roles['status'] = get_active_role(raw_roles['End Date']) # add coloumn 'status'
        # remove duplicates and none role rows
        raw_roles.dropna(subset=['Name'], inplace=True)
        clean_roles = raw_roles.drop_duplicates()
        write_csv_aws(clean_roles,'processed/clean_roles_tbl.csv')
