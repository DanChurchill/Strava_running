import numpy as np
import pandas as pd
import datetime
import os
import warnings
warnings.filterwarnings("ignore")

def get_strava_data():
    '''
    Function looks for saved csv file.  If present it returns that file, otherwise it returns a new one
    '''
    filename = "strava.csv"

    # if file is available locally, read it
    if os.path.isfile(filename):
        return pd.read_csv(filename)

    else:
        # read the specified columns from the raw CSV file

        cols = ['Activity ID',
                'Activity Date',
                'Activity Name',
                'Activity Type',
                'Distance',
                'Moving Time',
                'Average Heart Rate',
                'Calories',
                'Average Temperature']
        df = pd.read_csv('data/activities.csv', usecols=cols)
        
        # Write that dataframe to disk for later. Called "caching" the data for later.
        df.to_csv(filename, index=False)
        return df


    #  CONVERT DISTANCE FROM KM TO MILES
def convert_distance(df):
    '''
    takes in dataframe and converts distance column from km to miles
    Also removes runs less than .25 miles
    '''
    df['distance'] = round(df['distance'] * 0.621371,2)
    df = df[df['distance'] >= 0.25]

    return df

def convert_time(df):
    '''
    takes in dataframe and converts Elapsed time from seconds to HH:MM:SS
    '''
    secs = 600
    def secs_to_time(secs):
        return str(datetime.timedelta(seconds = secs))
    
    df['moving_time'] = df['moving_time'].apply(secs_to_time)

    return df

def col_names(df):
    '''
    Takes in dataframe and converts column names to lower case 
    and replaces spaces with underscores
    returns dataframe with converted column names
    '''
    df.columns = df.columns.str.lower()
    df.columns = df.columns.str.replace(" ", "_")

    return df



def convert_temp(df):
    '''
    accepts a dataframe of strava activity data and converts the 
    average temperature column from Celcius to Farenheit'''

    df['average_temperature'] = (df['average_temperature'] * (9/5)) + 32
    return df




def runs_only(df):
    '''
    filters out any activity that is not a run (hike, walk, bike, etc)
    then remove the activity type column
    '''
    df = df[df['activity_type'] == 'Run']
    df = df.drop(columns=['activity_type'])
    return df

def typ_clean(df):
    '''
    accepts a dataframe of strava activities and performs typical cleaning functions
    calls 'runs_only' to remove non-running activities
    calls 'col_names' to lowercase the columns and remove spaces
    calls 'convert_time' to convert seconds to HH:MM:SS
    calls 'convert_distance' to convert kms to miles
    calls 'convert_temp' to convert C to F
    '''
    df = col_names(df)
    df = convert_distance(df)
    df = convert_time(df)
    df = runs_only(df)
    df = convert_temp(df)

    return df