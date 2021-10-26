import pandas as pd
import boto3
import argparse
import os
import io
from datetime import timedelta, date,datetime
import json
from ast import literal_eval

def create_df(sample,cols):
    #return list of lists for each row for each portfolio held by a position

    #load string python list
    try:
        portfolios = literal_eval(sample['Portfolios'])
    except:
        #two rows have [nan] lists as their portolios for an unknown reason
        portfolios = ['Unknown']

    #create a unique row for each portfolio
    data =[]
    for portfolio in portfolios:
        d = list(sample.values)
        d[9] = portfolio
        data.append(d)

    #turn list of lists into datframe
    f = pd.DataFrame(data,columns=cols)

    return f
def create_portfolio_rows(sample):
    #accepts a dataframe filtered to all roles within a ministry and
    #returns a dataframe of rows of each portofolio held by a cabinet members
    d = pd.DataFrame([], columns=list(sample))
    for row in range(len(sample)):
        d = d.append(create_df(sample.iloc[row,:],cols=list(sample)))

    return d
def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser
def merge_portfolios(d,df2,name):
    # create list of lists of a collapsed set of portfolio terms held by ministers
    #i.e combine if same ministers holds the same portfolio over 6 years but is split two subsequent terms
    for portfolio in list(d['Portfolios'].unique()):
            sample = d[d['Portfolios'] == portfolio]

            if len(sample)<2:
                #if only one instance of the portfolio holding create list of Name, Portfolio, Start Date, End Date
                data2 =[name,portfolio,sample['Start Date'].tolist()[0], sample['End Date'].tolist()[0]]
                df2.append(data2)

            else:
                #combine if unique holding is split by one day differnce
                for i,start in enumerate(sample['Start Date'].to_list()):
                    if start-timedelta(days=1) in sample['End Date'].to_list():
                        #find the frame row number of the matching end-date to get other info
                        index = sample['End Date'].to_list().index(start-timedelta(days=1))
                        #check if the end date is also precceeded by another holding
                        if sample.iloc[index,5]-timedelta(days=1) in sample['End Date'].to_list():
                            #if the sitting is split into three
                            #find the frame row number of the matching end-date to get other info
                            index2 = sample['End Date'].to_list().index(sample.iloc[index,5]-timedelta(days=1))
                            data2 = [name,portfolio,sample.iloc[index2,5], sample.iloc[i,6]]
                        else:
                            #if the sitting is only split into two
                            data2 = [name,portfolio,sample.iloc[index,5], sample.iloc[i,6]]
                        df2.append(data2)

                    else:
                        #if they are different sittings?
                        data2 = [name,portfolio,sample['Start Date'].tolist()[i], sample['End Date'].tolist()[i]]
                        df2.append(data2)

    return df2

def create_portfolio_df(df,portfolio_tbl):
    #accepts ministry dataframes with row for each portfolio to create a table of rows of merged portfolio holdings
    df.drop(columns=['Title','Parliament'],inplace=True)
    df.drop_duplicates(inplace=True)
    #load date-time strings as datetime objects
    df['Start Date'] = pd.to_datetime(df['Start Date'],format='%d-%m-%Y')
    df['End Date'] = pd.to_datetime(df['End Date'],format='%d-%m-%Y',errors='coerce')

    for name in list(df['Name'].unique()):
        #Send df for each unique name in a cabinet
        portfolio_tbl = merge_portfolios(df[df['Name'] == name],portfolio_tbl,name)

    return portfolio_tbl

if __name__ == "__main__":
    #load command line args
    parser  = create_argument_parser()
    args = parser.parse_args()

    #proccessing to produce portfolio tbl (rows of each term a federal government portfolio was held by a cabinet member)
    if args.run_type == 'proccess':
        #connect to amazon web services S3 bucket
        s3 = boto3.resource(service_name='s3',region_name='ca-central-1',aws_access_key_id=str(os.getenv("aws_access")[1:-1]),aws_secret_access_key=str(os.getenv("aws_key")[1:-1]))

        file_obj = s3.Bucket('polemics').Object("references/PrimeMinisters.csv").get()
        ministry_info = pd.read_csv(io.BytesIO(file_obj['Body'].read()))

        file_obj = s3.Bucket('polemics').Object('processed/cabinet_tbl.csv').get()
        cabinet_tbl = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        cabinet_tbl.drop(columns=['Unnamed: 0'],inplace=True)

        #create list of dataframes for each ministry and the terms portfolios were held by cabinet members
        data = [create_portfolio_rows(cabinet_tbl[cabinet_tbl['Ministry'] == ministry]) for ministry in list(cabinet_tbl['Ministry'].unique())]
        portfolio_tbl = []
        for df in data:
            portfolio_tbl = create_portfolio_df(df,portfolio_tbl)

        portfolio_tbl = pd.DataFrame(portfolio_tbl,columns=['Name',"Portfolio","Start","End"])
        portfolio_tbl.drop_duplicates(inplace=True)

        s3.Object('polemics', 'processed/portfolio_tbl.csv').put(Body=portfolio_tbl.to_csv())
        print("successfully stored proccessed file in s3 bucket!")
