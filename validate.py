import pandas as pd
import re

class Validate:

    def check_null(self,df):
        df.dropna(inplace =True, axis = 0)
        df.drop_duplicates(inplace = True)
        return df
    
    def validate_string(self, df, column_name):
        #self.drop_invld_rows(df, df[column_name].str.match('NULL'))
        self.drop_invld_rows(df, df[column_name].str.contains(r'\W'))
        self.drop_invld_rows(df, df[column_name].str.contains(r'[0-9]'))
        return df
    
    def validate_number(self,df,column_name):
        df[column_name] = df[column_name].astype(str)
        #self.drop_invld_rows(df, df[column_name].str.match('NULL'))
        self.drop_invld_rows(df, df[column_name].str.contains(r'\W'))
        self.drop_invld_rows(df, df[column_name].str.contains(r'[a-z]|[A-Z]'))
        #df[column_name] = pd.to_numeric(df[column_name],errors = 'coerce')
        df[column_name] = df[column_name].astype(int)
        return df
    
    def validate_varchar(self,df,column_name):
        self.drop_invld_rows(df, df[column_name].str.contains(r'\W'))
    
    def validate_float(self,df,column_name):
       # self.drop_invld_rows(df, df[column_name].str.contains(r'[a-z]|[A-Z]'))
        df[column_name] = pd.to_numeric(df[column_name],errors = 'coerce',downcast = 'float')

    def drop_invld_rows(self, df, invalid_rows):
         df.drop(df.loc[invalid_rows].index, axis = 0, inplace = True)
        

    
    def validate_date(self,df,column_name):
        #self.drop_invld_rows(df, df[column_name].str.match('NULL'))
        df[column_name] = pd.to_datetime(df[column_name],errors = 'coerce').dt.date
        self.drop_invld_rows(df,df[column_name].isnull())
        return df
    
    def validate_email(self,df,column_name):
        df.drop(df.loc[df[column_name].str.contains('@') == False].index,axis = 0,inplace = True)
        #self.drop_invld_rows(df, df[column_name].str.match('NULL'))
        return df
    
   