import pandas as pd
import boto3
import argparse
import os
import io
from datetime import timedelta, date,datetime
import json

AWS_ACCESS_KEY_ID = os.getenv("aws_access")[1:-1]
AWS_SECRET_ACCESS_KEY = os.getenv("aws_key")[1:-1]
s3 = boto3.resource(
    service_name='s3',
    region_name='ca-central-1',
    aws_access_key_id=str(AWS_ACCESS_KEY_ID),
    aws_secret_access_key=str(AWS_SECRET_ACCESS_KEY)
)

file_obj = s3.Bucket('polemics').Object("references/elections.xlsx").get()
ministry_dates = pd.read_excel(io.BytesIO(file_obj['Body'].read()),'ministries')

def convert_date(date):
    # convert dates from different formats
    try:
        d = datetime.strptime(date,"%Y/%m/%d")
        date2 = d.strftime("%d-%m-%Y")
    except:
        try:
            d = datetime.strptime(date,"%Y-%m-%d")
            date2 = d.strftime("%d-%m-%Y")
        except:
            try:
                date = str(date) +"-01"
                d = datetime.strptime(date,"%Y-%m-%d")
                date2 = d.strftime("%d-%m-%Y")
            except:
                try:
                    date = str(date) +"-01"
                    d = datetime.strptime(date,"%Y-%m-%d")
                    date2 = d.strftime("%d-%m-%Y")
                except:
                    date2 = ""
    return date2

def get_ministry(dates,kind="start"):
    # Find the associated ministry with a role
    role_start_dates = []
    for i,date in enumerate(dates.to_list()):

        try:
            d = datetime.strptime(date,"%d-%m-%Y")
            role_start_dates.append(d)
        except:
            # if the role is still active set it as today for date comparison
            today = datetime.today().strftime("%d-%m-%Y")
            role_start_dates.append(datetime.strptime(today,"%d-%m-%Y"))
            continue

    ministry = ['Unknown']*len(role_start_dates)

    print('Getting ' + kind + ' Ministry...')
    for t,date in enumerate(role_start_dates):
        #compare start or end date to each ministry period to find associated one
        for i in range(len(ministry_dates)):
            min_start = datetime.strptime(str(ministry_dates.iloc[i,1])[:10],"%Y-%m-%d")
            min_end = datetime.strptime(str(ministry_dates.iloc[i,2])[:10],"%Y-%m-%d")

            if df.iloc[t,11] == "active":
                ministry[t] = 29
                break
            if kind =="end":
                #don't consider roles that end within 3 days of a cabinet starting as associated
                min_start = min_start + timedelta(days=3)
                if (date >= min_start) and (date <= min_end):
                    ministry[t] = ministry_dates.iloc[i,0]
                    break

                if i == len(ministry_dates)-1:
                    min_starts = [datetime.strptime(str(ministry_dates.iloc[x,1])[:10],"%Y-%m-%d") + timedelta(days=3) for x in range(len(ministry_dates))]
                    min_ends = [datetime.strptime(str(ministry_dates.iloc[x,2])[:10],"%Y-%m-%d") + timedelta(days=3) for x in range(len(ministry_dates))]

            else:
                if (date >= min_start) and (date < min_end):
                    ministry[t] = ministry_dates.iloc[i,0]
                    break
    return ministry

def create_argument_parser():
    """
    Function to add command line arguments at run time
    """
    parser  = argparse.ArgumentParser(description = 'Script to test out pipeline')
    parser.add_argument('--run-type', nargs = '?', required = True, help = 'Command to run task or test')
    return parser

if __name__ == "__main__":
    parser  = create_argument_parser()
    #load command line args
    args = parser.parse_args()
    if args.run_type == 'proccess':
        # Load proccessed roles table into pandas data frame
        file_obj = s3.Bucket('polemics').Object('processed/clean_roles_tbl.csv').get()
        roles_tbl = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        roles_tbl.drop(columns=['Unnamed: 0'],inplace=True)

        #Load reference table for cabinent membership status
        file_obj = s3.Bucket('polemics').Object('references/exclusion_role_tbl.csv').get()
        false_cab = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        false_cab.drop(columns=['notes'],inplace=True)
        false_cab['Start Date'] = pd.to_datetime(false_cab['Start Date'], format="%Y-%m-%d", errors='coerce')
        false_cab['Start Date'] = false_cab['Start Date'].apply(lambda x: x.strftime('%Y/%m/%d'))
        # print(list(roles_tbl),list(false_cab))
        #exclude select roles that would otherwise be erroneously included
        uid1 = false_cab['Name']+false_cab['Title']+false_cab['Start Date']
        uid2 = roles_tbl['Name']+roles_tbl['Title']+roles_tbl['Start Date']


        roles_tbl.insert(len(list(roles_tbl)), "uid", uid2, True)
        false_cab.insert(len(list(false_cab)), "uid", uid1, True)

        common = roles_tbl.merge(false_cab, on=['uid'])
        df = roles_tbl[~roles_tbl['uid'].isin(common['uid'])]
        print(len(df),len(roles_tbl))

        df.drop(columns=['uid'])
        #exclude non-cabinet roles from data frame
        df2 = df[df['Role'].isin(['Minister','Minister (Acting)',
                                  'Minister (Acting Minister)','Secretary of State'])]


        #include select roles that were erroneously discluded
        file_obj = s3.Bucket('polemics').Object('references/roles_inclusion_tbl.csv').get()
        true_cab = pd.read_csv(io.BytesIO(file_obj['Body'].read()))
        true_cab.drop(columns=['notes'],inplace=True)

        df2 = df2.append(true_cab, ignore_index=True)

        #convert all date to same format (day-month-year)
        df2.iloc[:,8] = [ convert_date(date) for date in list(df2['Start Date'])]
        df2.iloc[:,9] = [ convert_date(date) for date in list(df2['End Date'])]
        #add new columns at end to determine which ministry a role started and ended
        df2.insert(len(list(df2)), "Start Ministry", get_ministry(df2['Start Date']), True)
        df2.insert(len(list(df2)), "End Ministry", get_ministry(df2['End Date'],kind="end"), True)


        sittings = pd.DataFrame(columns=['Title','Name','Status','Gender','Constituency','Province or Territory',
                                'Start Date','End Date','Parliament','Portfolios','Political Affiliation'])
        #get sittings data for each role/minister
        for parl in list(df2['parliament'].unique()):

            df3 = df2[df2["parliament"] == parl]

            for role in list(df3['Title'].unique()):
                #look at each minister role indepenendently
                df4 = df3[df3["Title"] == role]
                #remove extra spaces at the end of the title
                if role[-1]==" ":
                    role = role[:-1]

                #add unique id by name + role start date
                uid = df4["Name"] + df4["Start Date"]
                df4 = df4.assign(uid=pd.Series(uid).values)

                for name in list(df4['uid'].unique()):
                    #just look at one person at a time
                    df5 = df4[df4['uid'] == name]

                    #cleaning up ministry data
                    end_ministry = df5['End Ministry'].to_list()[0]
                    start_ministry = df5['Start Ministry'].to_list()[0]
                    try:
                        num_ministries = (int(end_ministry) - int(start_ministry)) + 1
                    except:
                        #to catch un converted datetimes
                        num_ministries = "Unknown"
                        ministries = [start_ministry,end_ministry]

                    #add itermediary ministries if role spans more than 2 (ie. start 1, end 3)
                    if num_ministries == 2:
                        ministries = [start_ministry,end_ministry]
                    if num_ministries == 3:
                        ministries = [start_ministry,start_ministry+1,end_ministry]
                    if num_ministries == 1:
                        ministries = [start_ministry]
                    try:
                        ministries.remove("Unknown")
                    except:
                        pass
                    #get details for this person
                    status = df5['status'].to_list()[0]
                    gender = df5['Gender'].to_list()[0]
                    party = df5['Political Affiliation'].to_list()[0]
                    district = df5['Constituency'].to_list()[0]
                    province = df5['Province or Territory'].to_list()[0]

                    end = df5['End Date'].to_list()[0]

                    try:
                        end = pd.to_datetime(end, format="%d-%m-%Y", errors='coerce')
                    except:
                        end = pd.to_datetime(end, format="%d/%m/%Y", errors='coerce')

                    try:
                        end = end.strftime('%Y-%m-%d')
                    except:
                        pass


                    start = df5['Start Date'].to_list()[0]

                    try:
                        start = pd.to_datetime(start, format="%d-%m-%Y", errors='coerce')
                    except:
                        start = pd.to_datetime(start, format="%d/%m/%Y", errors='coerce')

                    start = start.strftime('%Y-%m-%d')

                    parl = df5['parliament'].to_list()[0]
                    portfolio = list(df5['Portfolio'].unique())


                    #create row for each miniostry the role spans ie. 3 rows if role goes from ministry 1,2,3
                    for ministry in ministries:
                        sittings = sittings.append({'Title':role,'Name':name[:-10],'Status':status,'Gender':gender,'Political Affiliation':party,
                                                    'Constituency':district,'Province or Territory':province,
                                                    'Start Date':start,'End Date':end,'Parliament':parl,
                                                    'Portfolios':portfolio,"Ministry":ministry}, ignore_index=True)


        s3.Object('polemics', 'processed/cabinet_tbl.csv').put(Body=sittings.to_csv())
        print("successfully stored proccessed file in s3 bucket!")
