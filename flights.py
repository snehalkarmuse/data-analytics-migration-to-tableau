import pandas as pd
import psycopg2
import glob
import os
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text
from yaml import safe_load
import validate as vld
# changed the security rules and allowed inbound traffic rule. in RDS instance aws.

class Understand_Data:
    def get_data(self):
        # the path to your csv file directory
        mycsvdir = 'data-analytics-files'
        # get all the csv files in that directory (assuming they have the extension .csv)
        csvfiles = glob.glob(os.path.join(mycsvdir, '*.csv'))
        # loop through the files and read them in with pandas
        # a list to hold all the individual pandas DataFrames
        list_of_dataframe = [ ]
        for csvfile in csvfiles:
            df = pd.read_csv(csvfile)
            list_of_dataframe.append(df)
        return list_of_dataframe
    
    def clean_each_column(self,df_temp):
       
        validator.validate_number(df_temp,'Year')
        validator.validate_number(df_temp,'Month')
        validator.validate_number(df_temp,'DayofMonth')
        validator.validate_number(df_temp,'DayOfWeek')
        validator.validate_number(df_temp,'CRSDepTime')
        validator.validate_number(df_temp,'CRSArrTime')
        validator.validate_number(df_temp,'FlightNum')
        validator.validate_number(df_temp,'CRSElapsedTime')

        df_temp['Cancelled'] = df_temp['Cancelled'].astype('category')
        df_temp['Cancelled'] = df_temp['Cancelled'].replace({0:False,1:True})
        df_temp['Diverted'] = df_temp['Diverted'].astype('category')
        df_temp['Diverted'] = df_temp['Diverted'].replace({0:False,1:True})

        #df_temp['DepTime'] = df_temp['DepTime'].astype(str)
        validator.validate_float(df_temp,'DepTime')
        #df_temp['ArrTime'] = df_temp['ArrTime'].astype(str)
        validator.validate_float(df_temp,'ArrTime')
        
        #df_temp['ActualElapsedTime'] = df_temp['ActualElapsedTime'].astype(str)
        validator.validate_float(df_temp,'ActualElapsedTime')
        
        #df_temp['ArrDelay'] = df_temp['ArrDelay'].astype(str)
        validator.validate_float(df_temp,'ArrDelay')
        #df_temp['DepDelay'] = df_temp['DepDelay'].astype(str)
        validator.validate_float(df_temp,'DepDelay')
        #df_temp['Distance'] = df_temp['Distance'].astype(str)
        validator.validate_float(df_temp,'Distance')
       

        validator.validate_string(df_temp,'Origin')
        validator.validate_string(df_temp,'Dest')
        return df_temp

        
    def clean_data(self,list_of_dataframe):
        list_of_clean_df= []
        new_df = pd.DataFrame

        for df in list_of_dataframe:
            df = self.clean_each_column(df)
            df_temp = df.dropna(how='all').dropna(how='all', axis=1)
            df_temp = df_temp.fillna(0)
            #df_temp = self.clean_each_column(df_temp)
            list_of_clean_df.append(df_temp)

        #for i in list_of_clean_df:
        new_df = pd.concat(list_of_clean_df)
        print(new_df.shape[0])
        print(new_df.info())
        new_df.to_csv('combined_data.csv',header = True, index = False)

    def read_db_creds(self):
            with open('db_creds.yaml', 'r') as f:
                #df = pd.json_normalize(safe_load(f))
                cd = safe_load(f)
            return cd
    
    '''his method calls yaml file with read database method. creates engine.'''
    def init_db_engine(self):
        cd = self.read_db_creds()
        cd['RDS_PORT'] = str(cd['RDS_PORT'])
        self.db_engine = create_engine(cd['RDS_DATABASE_TYPE']+"+"+cd['RDS_DBAPI']+"://"+cd['RDS_USER']+":"
                                       +cd['RDS_PASSWORD']+"@"+cd['RDS_HOST']+":"+cd['RDS_PORT']+"/"+cd['RDS_DATABASE']) 
        return self.db_engine
    
understand_data = Understand_Data()
understand_data.read_db_creds()
understand_data.init_db_engine()
validator = vld.Validate()
list_of_dataframe =  understand_data.get_data()
understand_data.clean_data(list_of_dataframe)




